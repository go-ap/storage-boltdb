package boltdb

import (
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/crypto/bcrypt"
)

type Metadata struct {
	Pw         []byte `jsonld:"pw,omitempty"`
	PrivateKey []byte `jsonld:"key,omitempty"`
}

// PasswordSet
func (r *repo) PasswordSet(it vocab.Item, pw []byte) error {
	path := itemBucketPath(it.GetLink())

	err := r.d.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(r.root)
		if err != nil {
			return errors.Errorf("Not able to write to root bucket %s", r.root)
		}
		if root == nil {
			return ErrorInvalidRoot(r.root)
		}
		if !root.Writable() {
			return errors.Errorf("Non writeable bucket %s", r.root)
		}
		var b *bolt.Bucket
		b, _, err = descendInBucket(root, path, true)
		if err != nil {
			return errors.Newf("Unable to find %s in root bucket", path)
		}
		if !b.Writable() {
			return errors.Errorf("Non writeable bucket %s", path)
		}

		pw, err = bcrypt.GenerateFromPassword(pw, -1)
		if err != nil {
			return errors.Annotatef(err, "Could not encrypt the pw")
		}
		m := Metadata{
			Pw: pw,
		}
		entryBytes, err := encodeFn(m)
		if err != nil {
			return errors.Annotatef(err, "Could not marshal metadata")
		}
		err = b.Put([]byte(metaDataKey), entryBytes)
		if err != nil {
			return errors.Errorf("Could not insert entry: %s", err)
		}
		return nil
	})

	return err
}

// PasswordCheck
func (r *repo) PasswordCheck(it vocab.Item, pw []byte) error {
	path := itemBucketPath(it.GetLink())
	m := Metadata{}
	err := r.d.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(r.root)
		if root == nil {
			return ErrorInvalidRoot(r.root)
		}
		var b *bolt.Bucket
		var err error
		b, path, err = descendInBucket(root, path, false)
		if err != nil {
			return errors.Newf("Unable to find %s in root bucket", path)
		}
		entryBytes := b.Get([]byte(metaDataKey))
		err = decodeFn(entryBytes, &m)
		if err != nil {
			return errors.Annotatef(err, "Could not unmarshal metadata")
		}
		if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
			return errors.NewUnauthorized(err, "Invalid pw")
		}
		return nil
	})
	return err
}

// LoadMetadata
func (r *repo) LoadMetadata(iri vocab.IRI, m any) error {
	path := itemBucketPath(iri)

	return r.d.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(r.root)
		if root == nil {
			return ErrorInvalidRoot(r.root)
		}
		var b *bolt.Bucket
		var err error
		b, path, err = descendInBucket(root, path, false)
		if err != nil {
			return errors.Newf("Unable to find %s in root bucket", path)
		}
		entryBytes := b.Get([]byte(metaDataKey))
		return decodeFn(entryBytes, m)
	})
}

// SaveMetadata
func (r *repo) SaveMetadata(iri vocab.IRI, m any) error {
	path := itemBucketPath(iri)
	err := r.d.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(r.root)
		if err != nil {
			return errors.Errorf("Not able to write to root bucket %s", r.root)
		}
		if root == nil {
			return ErrorInvalidRoot(r.root)
		}
		if !root.Writable() {
			return errors.Errorf("Non writeable bucket %s", r.root)
		}
		var b *bolt.Bucket
		b, _, err = descendInBucket(root, path, true)
		if err != nil {
			return errors.Newf("Unable to find %s in root bucket", path)
		}
		if !b.Writable() {
			return errors.Errorf("Non writeable bucket %s", path)
		}

		entryBytes, err := encodeFn(m)
		if err != nil {
			return errors.Annotatef(err, "Could not marshal metadata")
		}
		err = b.Put([]byte(metaDataKey), entryBytes)
		if err != nil {
			return errors.Errorf("Could not insert entry: %s", err)
		}
		return nil
	})

	return err
}

// LoadKey loads a private key for an actor found by its IRI
func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil {
		return nil, err
	}
	b, _ := pem.Decode(m.PrivateKey)
	if b == nil {
		return nil, errors.Errorf("failed decoding pem")
	}
	prvKey, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	return prvKey, nil
}

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (vocab.Item, error) {
	ob, err := r.loadOneFromBucket(iri)
	if err != nil {
		return nil, err
	}

	typ := ob.GetType()
	if !vocab.ActorTypes.Contains(typ) {
		return ob, errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", typ)
	}
	actor, err := vocab.ToActor(ob)
	if err != nil {
		return ob, errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", typ)
	}

	m := new(Metadata)
	if err = r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return ob, err
	}

	if m.PrivateKey != nil {
		r.logFn("actor %s already has a private key", iri)
	}
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		r.errFn("unable to x509.MarshalPKCS8PrivateKey() the private key %T for %s", key, iri)
		return ob, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(iri, m); err != nil {
		r.errFn("unable to save the private key %T for %s", key, iri)
		return ob, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case *ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.errFn("received key %T does not match any of the known private key types", key)
		return ob, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.errFn("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return ob, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	actor.PublicKey = vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}
	return r.Save(actor)
}
