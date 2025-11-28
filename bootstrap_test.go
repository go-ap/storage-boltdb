package boltdb

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	bolt "go.etcd.io/bbolt"
)

func createForbiddenDir(t *testing.T) string {
	forbiddenPath := filepath.Join(t.TempDir(), "forbidden")
	err := os.MkdirAll(forbiddenPath, 0o000)
	if err != nil {
		t.Fatalf("unable to create forbidden test path %s: %s", forbiddenPath, err)
	}
	return forbiddenPath
}

func TestBootstrap(t *testing.T) {
	forbiddenPath := createForbiddenDir(t)
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
		{
			name:    "forbidden",
			arg:     Config{Path: forbiddenPath},
			wantErr: &fs.PathError{Op: "open", Path: filepath.Join(forbiddenPath, dbFile), Err: syscall.EACCES},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Bootstrap(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Bootstrap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr != nil {
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
			name:    "temp - exists, but empty",
			arg:     Config{Path: t.TempDir()},
			wantErr: nil,
		},
		{
			name:    "temp - does not exists",
			arg:     Config{Path: filepath.Join(t.TempDir(), "test")},
			wantErr: nil,
		},
		{
			name:    "invalid path " + os.DevNull,
			arg:     Config{Path: os.DevNull},
			wantErr: errors.Errorf("path exists, and is not a folder %s", os.DevNull),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Clean(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Clean() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}
