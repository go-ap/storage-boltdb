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
	db, err := bolt.Open(r.path, 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return bootstrap(db, r.root)
}

func bootstrap(db *bolt.DB, root []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(root)
		if err != nil {
			return errors.Annotatef(err, "could not create root bucket")
		}
		_, err = root.CreateBucketIfNotExists([]byte(accessBucket))
		if err != nil {
			return errors.Annotatef(err, "could not create %s bucket", accessBucket)
		}
		_, err = root.CreateBucketIfNotExists([]byte(refreshBucket))
		if err != nil {
			return errors.Annotatef(err, "could not create %s bucket", refreshBucket)
		}
		_, err = root.CreateBucketIfNotExists([]byte(authorizeBucket))
		if err != nil {
			return errors.Annotatef(err, "could not create %s bucket", authorizeBucket)
		}
		_, err = root.CreateBucketIfNotExists([]byte(clientsBucket))
		if err != nil {
			return errors.Annotatef(err, "could not create %s bucket", clientsBucket)
		}
		return nil
	})
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, bolt.DefaultOptions)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket([]byte(rootBucket))
		return nil
	})
}
