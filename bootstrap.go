package boltdb

import (
	"os"

	"github.com/go-ap/errors"
	bolt "go.etcd.io/bbolt"
)

func Bootstrap(conf Config) error {
	if conf.Path == "" {
		return os.ErrNotExist
	}
	r, err := New(conf)
	if err != nil {
		return err
	}
	db, err := bolt.Open(r.path, 0600, bolt.DefaultOptions)
	if err != nil {
		return err
	}
	defer db.Close()
	return bootstrap(db, r.root)
}

func bootstrap(db *bolt.DB, root []byte) error {
	if db == nil {
		return errNotOpen
	}
	return db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(root)
		if err != nil {
			return errors.Annotatef(err, "could not create root bucket")
		}
		// NOTE(marius): we assume no changes happen during the execution and if
		// root bucket was created successfully, the rest will succeed too.
		_, _ = root.CreateBucketIfNotExists([]byte(accessBucket))
		_, _ = root.CreateBucketIfNotExists([]byte(refreshBucket))
		_, _ = root.CreateBucketIfNotExists([]byte(authorizeBucket))
		_, _ = root.CreateBucketIfNotExists([]byte(clientsBucket))
		return nil
	})
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}
