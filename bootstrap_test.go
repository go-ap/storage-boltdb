package boltdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	bolt "go.etcd.io/bbolt"
)

func TestClean1(t *testing.T) {
	dir := os.TempDir()

	conf := Config{Path: dir}
	path, _ := Path(conf)
	{
		Clean(conf)
	}
	{
		db, err := bolt.Open(path, 0600, nil)
		if err != nil {
			t.Errorf("Unable to create boltdb at path %s", path)
		}
		db.Close()

		err = Clean(conf)
	}

	{
		db, err := bolt.Open(path, 0600, nil)
		if err != nil {
			t.Errorf("Unable to create boltdb at path %s", path)
		}
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(rootBucket))
			if err != nil {
				return errors.Annotatef(err, "could not create root bucket")
			}
			return nil
		})
		if err != nil {
			t.Errorf("Unable to create root bucket %s in boltdb %s", rootBucket, path)
		}
		db.Close()

		err = Clean(conf)
		if err != nil {
			t.Errorf("Error received when cleaning valid boltdb %s with valid root bucket %s: %s", path, rootBucket, err)
		}
	}
	err := os.Remove(path)
	if err != nil {
		t.Logf("Unable to clean boltdb path %s", path)
	}
}

func TestBootstrap(t *testing.T) {
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: os.ErrNotExist,
		},
		{
			name: "temp",
			arg:  Config{Path: filepath.Join(t.TempDir())},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Bootstrap(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Bootstrap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			ff := fields{
				path: tt.arg.Path,
				root: []byte(rootBucket),
			}
			r := mockRepo(t, ff, withOpenRoot)
			defer r.Close()

			err := r.d.View(func(tx *bolt.Tx) error {
				path := tt.arg.Path
				bucket := ff.root
				root := tx.Bucket(bucket)
				if false {
					// NOTICE(marius): these have been disabled in the bootstrap, because they're dynamically created
					activities := root.Bucket([]byte(bucketActivities))
					if activities == nil {
						t.Errorf("Could not find bucket %s/%s at boltdb path %s", bucket, bucketActivities, path)
						return nil
					}
					actors := root.Bucket([]byte(bucketActors))
					if actors == nil {
						t.Errorf("Could not find bucket %s/%s at boltdb path %s", bucket, bucketActors, path)
						return nil
					}
					objects := root.Bucket([]byte(bucketObjects))
					if objects == nil {
						t.Errorf("Could not find bucket %s/%s at boltdb path %s", bucket, bucketObjects, path)
						return nil
					}
				}
				return nil
			})
			if err != nil {
				t.Errorf("Opening boltdb repo for viewing failed: %s", err)
			}
		})
	}
}

func TestClean(t *testing.T) {
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: nil,
		},
		{
			name:    "temp - exists",
			arg:     Config{Path: t.TempDir()},
			wantErr: nil,
		},
		{
			name:    "temp - does not exists",
			arg:     Config{Path: filepath.Join(t.TempDir(), "test")},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Clean(tt.arg); !errors.Is(err, tt.wantErr) {
				t.Errorf("Clean() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
