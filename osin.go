package boltdb

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/go-ap/errors"
	"github.com/openshift/osin"
	bolt "go.etcd.io/bbolt"
)

const (
	clientsBucket   = "clients"
	authorizeBucket = "authorize"
	accessBucket    = "access"
	refreshBucket   = "refresh"
)

type cl struct {
	Id          string
	Secret      string
	RedirectUri string
	Extra       interface{}
}

type auth struct {
	Client      string
	Code        string
	ExpiresIn   time.Duration
	Scope       string
	RedirectURI string
	State       string
	CreatedAt   time.Time
	Extra       interface{}
}

type acc struct {
	Client       string
	Authorize    string
	Previous     string
	AccessToken  string
	RefreshToken string
	ExpiresIn    time.Duration
	Scope        string
	RedirectURI  string
	CreatedAt    time.Time
	Extra        interface{}
}

type ref struct {
	Access string
}

var encodeFn = func(v any) ([]byte, error) {
	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

var decodeFn = func(data []byte, m any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(m)
}

// Clone the storage if needed. For example, using mgo, you can clone the session with session.Clone
// to avoid concurrent access problems.
// This is to avoid cloning the connection at each method access.
// Can return itself if not a problem.
func (r *repo) Clone() osin.Storage {
	r.Close()
	return r
}

// Close closes the boltdb database if possible.
func (r *repo) Close() {
	err := r.close()
	if err != nil {
		r.errFn("error closing: %+s", err)
	}
}

func (r *repo) ListClients() ([]osin.Client, error) {
	clients := make([]osin.Client, 0)
	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cl := cl{}
		cb := rb.Bucket([]byte(clientsBucket))
		if cb == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, clientsBucket)
		}
		c := cb.Cursor()
		for k, raw := c.First(); k != nil; k, raw = c.Next() {
			if err := decodeFn(raw, &cl); err != nil {
				r.errFn("Unable to unmarshal client object %s", err)
				continue
			}
			d := osin.DefaultClient{
				Id:          cl.Id,
				Secret:      cl.Secret,
				RedirectUri: cl.RedirectUri,
				UserData:    cl.Extra,
			}
			clients = append(clients, &d)
		}
		return nil
	})
	return clients, err
}

// GetClient loads the client by id
func (r *repo) GetClient(id string) (osin.Client, error) {
	if id == "" {
		return nil, errors.NotFoundf("Empty client id")
	}
	c := osin.DefaultClient{}

	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cl := cl{}
		cb := rb.Bucket([]byte(clientsBucket))
		if cb == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, clientsBucket)
		}
		raw := cb.Get([]byte(id))
		if err := decodeFn(raw, &cl); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal client object")
		}
		c.Id = cl.Id
		c.Secret = cl.Secret
		c.RedirectUri = cl.RedirectUri
		c.UserData = cl.Extra
		return nil
	})

	return &c, err
}

// UpdateClient updates the client (identified by it's id) and replaces the values with the values of client.
func (r *repo) UpdateClient(c osin.Client) error {
	cl := cl{
		Id:          c.GetId(),
		Secret:      c.GetSecret(),
		RedirectUri: c.GetRedirectUri(),
		Extra:       c.GetUserData(),
	}
	raw, err := encodeFn(cl)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal client object")
	}
	return r.d.Update(func(tx *bolt.Tx) error {
		rb, err := tx.CreateBucketIfNotExists(r.root)
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s", r.root)
		}
		cb, err := rb.CreateBucketIfNotExists([]byte(clientsBucket))
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s/%s", r.root, clientsBucket)
		}
		return cb.Put([]byte(cl.Id), raw)
	})
}

// CreateClient stores the client in the database and returns an error, if something went wrong.
func (r *repo) CreateClient(c osin.Client) error {
	return r.UpdateClient(c)
}

// RemoveClient removes a client (identified by id) from the database. Returns an error if something went wrong.
func (r *repo) RemoveClient(id string) error {
	return r.d.Update(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cb := rb.Bucket([]byte(clientsBucket))
		if cb == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, clientsBucket)
		}
		return cb.Delete([]byte(id))
	})
}

// SaveAuthorize saves authorize data.
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	auth := auth{
		Client:      data.Client.GetId(),
		Code:        data.Code,
		ExpiresIn:   time.Duration(data.ExpiresIn),
		Scope:       data.Scope,
		RedirectURI: data.RedirectUri,
		State:       data.State,
		CreatedAt:   data.CreatedAt.UTC(),
		Extra:       data.UserData,
	}
	raw, err := encodeFn(auth)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal authorization object")
	}
	return r.d.Update(func(tx *bolt.Tx) error {
		rb, err := tx.CreateBucketIfNotExists(r.root)
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s", r.root)
		}
		cb, err := rb.CreateBucketIfNotExists([]byte(authorizeBucket))
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s/%s", r.root, authorizeBucket)
		}
		return cb.Put([]byte(data.Code), raw)
	})
}

// LoadAuthorize looks up AuthorizeData by a code.
// Client information MUST be loaded together.
// Optionally can return error if expired.
func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty authorize code")
	}
	var data osin.AuthorizeData

	auth := auth{}
	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		ab := rb.Bucket([]byte(authorizeBucket))
		if ab == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, authorizeBucket)
		}
		raw := ab.Get([]byte(code))

		if err := decodeFn(raw, &auth); err != nil {
			err = errors.Annotatef(err, "Unable to unmarshal authorization object")
			r.errFn("Authorization code %s: %+s", code, err)
			return err
		}
		data.Code = auth.Code
		data.ExpiresIn = int32(auth.ExpiresIn)
		data.Scope = auth.Scope
		data.RedirectUri = auth.RedirectURI
		data.State = auth.State
		data.CreatedAt = auth.CreatedAt
		data.UserData = auth.Extra

		if data.ExpireAt().Before(time.Now().UTC()) {
			err := errors.Errorf("Token expired at %s.", data.ExpireAt().String())
			r.errFn("Authorize code %s: %+s", code, err)
			return err
		}

		c := osin.DefaultClient{}
		cl := cl{}
		cb := rb.Bucket([]byte(clientsBucket))
		if cb != nil {
			rawC := cb.Get([]byte(auth.Client))
			if err := decodeFn(rawC, &cl); err != nil {
				err = errors.Annotatef(err, "Unable to unmarshal client object")
				r.errFn("Authorize code %s: %+s", code, err)
				return nil
			}
			c.Id = cl.Id
			c.Secret = cl.Secret
			c.RedirectUri = cl.RedirectUri
			c.UserData = cl.Extra

			data.Client = &c
		} else {
			err := errors.Newf("Invalid bucket %s/%s", r.root, clientsBucket)
			r.errFn("Authorize code %s: %+s", code, err)
			return nil
		}
		return nil
	})

	return &data, err
}

// RemoveAuthorize revokes or deletes the authorization code.
func (r *repo) RemoveAuthorize(code string) error {
	return r.d.Update(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cb := rb.Bucket([]byte(authorizeBucket))
		if cb == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, authorizeBucket)
		}
		return cb.Delete([]byte(code))
	})
}

// SaveAccess writes AccessData.
// If RefreshToken is not blank, it must save in a way that can be loaded using LoadRefresh.
func (r *repo) SaveAccess(data *osin.AccessData) error {
	prev := ""
	authorizeData := &osin.AuthorizeData{}

	if data.AccessData != nil {
		prev = data.AccessData.AccessToken
	}

	if data.AuthorizeData != nil {
		authorizeData = data.AuthorizeData
	}

	if data.RefreshToken != "" {
		if err := r.saveRefresh(data.RefreshToken, data.AccessToken); err != nil {
			r.errFn("Refresh id %s: %+s", data.Client.GetId(), err)
			return err
		}
	}

	if data.Client == nil {
		return errors.Newf("data.Client must not be nil")
	}

	acc := acc{
		Client:       data.Client.GetId(),
		Authorize:    authorizeData.Code,
		Previous:     prev,
		AccessToken:  data.AccessToken,
		RefreshToken: data.RefreshToken,
		ExpiresIn:    time.Duration(data.ExpiresIn),
		Scope:        data.Scope,
		RedirectURI:  data.RedirectUri,
		CreatedAt:    data.CreatedAt.UTC(),
		Extra:        data.UserData,
	}
	raw, err := encodeFn(acc)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal access object")
	}
	return r.d.Update(func(tx *bolt.Tx) error {
		rb, err := tx.CreateBucketIfNotExists(r.root)
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s", r.root)
		}
		cb, err := rb.CreateBucketIfNotExists([]byte(accessBucket))
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s/%s", r.root, accessBucket)
		}
		return cb.Put([]byte(acc.AccessToken), raw)
	})
}

// LoadAccess retrieves access data by token. Client information MUST be loaded together.
// AuthorizeData and AccessData DON'T NEED to be loaded if not easily available.
// Optionally can return error if expired.
func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty access code")
	}
	var result osin.AccessData

	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		var access acc
		ab := rb.Bucket([]byte(accessBucket))
		if ab == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, accessBucket)
		}
		raw := ab.Get([]byte(code))
		if raw == nil {
			return errors.Newf("Unable to load access information for %s/%s/%s", r.root, accessBucket, code)
		}
		if err := decodeFn(raw, &access); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal access object")
		}
		result.AccessToken = access.AccessToken
		result.RefreshToken = access.RefreshToken
		result.ExpiresIn = int32(access.ExpiresIn)
		result.Scope = access.Scope
		result.RedirectUri = access.RedirectURI
		result.CreatedAt = access.CreatedAt.UTC()
		result.UserData = access.Extra

		c := osin.DefaultClient{}
		cl := cl{}
		cb := rb.Bucket([]byte(clientsBucket))
		if cb == nil {
			r.errFn("Access code %s: %+s", code, errors.Newf("Invalid bucket %s/%s", r.root, clientsBucket))
			return nil
		}
		rawC := cb.Get([]byte(access.Client))
		if err := decodeFn(rawC, &cl); err != nil {
			r.errFn("Access code %s: %+s", code, errors.Annotatef(err, "Unable to unmarshal client object"))
			return nil
		}
		c.Id = cl.Id
		c.Secret = cl.Secret
		c.RedirectUri = cl.RedirectUri
		c.UserData = cl.Extra

		result.Client = &c

		authB := rb.Bucket([]byte(authorizeBucket))
		if authB == nil {
			r.errFn("Access code %s: %+s", code, errors.Newf("Invalid bucket %s/%s", r.root, authorizeBucket))
			return nil
		}
		if access.Authorize != "" {
			auth := auth{}

			rawAuth := authB.Get([]byte(access.Authorize))
			if rawAuth == nil {
				r.errFn("Access code %s: %+s", code, errors.Newf("Invalid authorize data"))
				return nil
			}
			if err := decodeFn(rawAuth, &auth); err != nil {
				r.errFn("Client code %s: %+s", code, errors.Annotatef(err, "Unable to unmarshal authorization object"))
				return nil
			}

			data := osin.AuthorizeData{
				Code:        auth.Code,
				ExpiresIn:   int32(auth.ExpiresIn),
				Scope:       auth.Scope,
				RedirectUri: auth.RedirectURI,
				State:       auth.State,
				CreatedAt:   auth.CreatedAt,
				UserData:    auth.Extra,
			}

			if data.ExpireAt().Before(time.Now().UTC()) {
				r.errFn("Access code %s: %+s", code, errors.Errorf("Token expired at %s.", data.ExpireAt().String()))
				return nil
			}
			result.AuthorizeData = &data
		}
		if access.Previous != "" {
			var prevAccess acc
			rawPrev := ab.Get([]byte(access.Previous))
			if err := decodeFn(rawPrev, &prevAccess); err != nil {
				r.errFn("Access code %s: %+s", code, errors.Annotatef(err, "Unable to unmarshal previous access object"))
				return nil
			}
			prev := osin.AccessData{}
			prev.AccessToken = prevAccess.AccessToken
			prev.RefreshToken = prevAccess.RefreshToken
			prev.ExpiresIn = int32(prevAccess.ExpiresIn)
			prev.Scope = prevAccess.Scope
			prev.RedirectUri = prevAccess.RedirectURI
			prev.CreatedAt = prevAccess.CreatedAt.UTC()
			prev.UserData = prevAccess.Extra

			result.AccessData = &prev
		}
		return nil
	})

	return &result, err
}

// RemoveAccess revokes or deletes an AccessData.
func (r *repo) RemoveAccess(code string) (err error) {
	return r.d.Update(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cb := rb.Bucket([]byte(accessBucket))
		if cb == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, accessBucket)
		}
		return cb.Delete([]byte(code))
	})
}

// LoadRefresh retrieves refresh AccessData. Client information MUST be loaded together.
// AuthorizeData and AccessData DON'T NEED to be loaded if not easily available.
// Optionally can return error if expired.
func (r *repo) LoadRefresh(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty refresh code")
	}

	var ref ref
	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cb := rb.Bucket([]byte(refreshBucket))
		prefix := []byte(code)
		u := cb.Cursor()
		if u == nil {
			return errors.Errorf("Invalid bucket cursor %s/%s", r.root, refreshBucket)
		}
		for k, v := u.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = u.Next() {
			if err := decodeFn(v, &ref); err != nil {
				return errors.Annotatef(err, "Unable to unmarshal refresh token object")
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.LoadAccess(ref.Access)
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(code string) error {
	return r.d.Update(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return errors.Errorf("Invalid bucket %s", r.root)
		}
		cb := rb.Bucket([]byte(refreshBucket))
		if cb == nil {
			return errors.Newf("Invalid bucket %s/%s", r.root, refreshBucket)
		}
		return cb.Delete([]byte(code))
	})
}

func (r *repo) saveRefresh(refresh, access string) error {
	ref := ref{
		Access: access,
	}
	raw, err := encodeFn(ref)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal refresh token object")
	}
	return r.d.Update(func(tx *bolt.Tx) error {
		rb, err := tx.CreateBucketIfNotExists(r.root)
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s", r.root)
		}
		cb, err := rb.CreateBucketIfNotExists([]byte(refreshBucket))
		if err != nil {
			return errors.Annotatef(err, "Invalid bucket %s/%s", r.root, refreshBucket)
		}
		return cb.Put([]byte(refresh), raw)
	})
}
