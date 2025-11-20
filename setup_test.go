package boltdb

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/crypto/bcrypt"
)

func areErrors(a, b any) bool {
	_, ok1 := a.(error)
	_, ok2 := b.(error)
	return ok1 && ok2
}

func compareErrors(x, y interface{}) bool {
	xe := x.(error)
	ye := y.(error)
	if errors.Is(xe, ye) || errors.Is(ye, xe) {
		return true
	}
	return xe.Error() == ye.Error()
}

var EquateWeakErrors = cmp.FilterValues(areErrors, cmp.Comparer(compareErrors))

type fields struct {
	path string
	d    *bolt.DB
	root []byte
}

func mockRepo(t *testing.T, f fields, initFns ...initFn) *repo {
	pp, err := fullPath(&f.path)
	if err != nil {
		t.Errorf("unable to get full path for db: %s", err)
		return nil
	}
	r := &repo{
		path:  pp,
		root:  f.root,
		logFn: t.Logf,
		errFn: t.Errorf,
	}

	for _, fn := range initFns {
		_ = fn(r)
	}
	return r
}

type initFn func(*repo) *repo

func withBootstrap(r *repo) *repo {
	if err := bootstrap(r.d, []byte(rootBucket)); err != nil {
		r.errFn("unable to bootstrap BoltDB: %s", err)
	}
	return r
}

func withOpenRoot(r *repo) *repo {
	var err error
	r.d, err = bolt.Open(r.path, 0600, nil)
	r.root = []byte(rootBucket)
	if err != nil {
		r.errFn("Could not open db %s: %s", r.path, err)
	}
	return r
}

var mockItems = vocab.ItemCollection{
	vocab.IRI("https://example.com/plain-iri"),
	&vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType},
	&vocab.Link{ID: "https://example.com/1", Href: "https://example.com/1", Type: vocab.LinkType},
	&vocab.Actor{ID: "https://example.com/~jdoe", Type: vocab.PersonType},
	&vocab.Activity{ID: "https://example.com/~jdoe/1", Type: vocab.UpdateType},
	&vocab.Object{ID: "https://example.com/~jdoe/tag-none", Type: vocab.UpdateType},
	&vocab.Question{ID: "https://example.com/~jdoe/2", Type: vocab.QuestionType},
	&vocab.IntransitiveActivity{ID: "https://example.com/~jdoe/3", Type: vocab.ArriveType},
}
var (
	pk, _      = rsa.GenerateKey(rand.Reader, 4096)
	pkcs8Pk, _ = x509.MarshalPKCS8PrivateKey(pk)
	key        = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Pk,
	})

	pubEnc, _  = x509.MarshalPKIXPublicKey(pk.Public())
	pubEncoded = pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	apPublic = &vocab.PublicKey{
		ID:           "https://example.com/~jdoe#main",
		Owner:        "https://example.com/~jdoe",
		PublicKeyPem: string(pubEncoded),
	}

	defaultPw = []byte("dsa")

	encPw, _ = bcrypt.GenerateFromPassword(defaultPw, bcrypt.DefaultCost)
)

func withMockItems(r *repo) *repo {
	for _, it := range mockItems {
		if _, err := save(r, it); err != nil {
			r.errFn("unable to save item: %s: %s", it.GetLink(), err)
		}
	}
	return r
}

func withMetadataJDoe(r *repo) *repo {
	m := Metadata{
		Pw:         encPw,
		PrivateKey: key,
	}

	if err := r.SaveMetadata("https://example.com/~jdoe", m); err != nil {
		r.errFn("unable to save metadata for jdoe: %s", err)
	}
	return r
}

var (
	defaultClient = &osin.DefaultClient{
		Id:          "test-client",
		Secret:      "asd",
		RedirectUri: "https://example.com",
		UserData:    nil,
	}
)

func mockAuth(code string, cl osin.Client) *osin.AuthorizeData {
	return &osin.AuthorizeData{
		Client:    cl,
		Code:      code,
		ExpiresIn: 10,
		CreatedAt: time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:  vocab.IRI("https://example.com/jdoe"),
	}
}

func mockAccess(code string, cl osin.Client) *osin.AccessData {
	return &osin.AccessData{
		Client:        cl,
		AuthorizeData: mockAuth("test-code", cl),
		AccessToken:   code,
		RefreshToken:  "refresh",
		ExpiresIn:     10,
		Scope:         "none",
		RedirectUri:   "http://localhost",
		CreatedAt:     time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:      vocab.IRI("https://example.com/jdoe"),
	}
}

func withClient(r *repo) *repo {
	if err := r.CreateClient(defaultClient); err != nil {
		r.errFn("failed to create client: %s", err)
	}
	return r
}

func withAuthorization(r *repo) *repo {
	if err := r.SaveAuthorize(mockAuth("test-code", defaultClient)); err != nil {
		r.errFn("failed to create authorization data: %s", err)
	}
	return r
}

func withAccess(r *repo) *repo {
	if err := r.SaveAccess(mockAccess("access-666", defaultClient)); err != nil {
		r.errFn("failed to create authorization data: %s", err)
	}
	return r
}
