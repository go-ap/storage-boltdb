package boltdb

import (
	"fmt"
	"os"
	"testing"

	vocab "github.com/go-ap/activitypub"
	bolt "go.etcd.io/bbolt"
)

func TestNew(t *testing.T) {
	dir := os.TempDir()

	conf := Config{
		Path:  dir,
		LogFn: func(s string, p ...interface{}) { t.Logf(s, p...) },
		ErrFn: func(s string, p ...interface{}) { t.Errorf(s, p...) },
	}
	repo, _ := New(conf)
	if repo == nil {
		t.Errorf("Nil result from opening boltdb %s", repo.path)
	}
	if repo.d != nil {
		t.Errorf("Non nil boltdb from New")
	}
	if repo.errFn == nil {
		t.Errorf("Nil error log function, expected %T[%p]", t.Errorf, t.Errorf)
	}
	if repo.logFn == nil {
		t.Errorf("Nil log function, expected %T[%p]", t.Logf, t.Logf)
	}
}

func TestRepo_Open(t *testing.T) {
	dir := os.TempDir()
	conf := Config{Path: dir}
	path, _ := Path(conf)
	err := Bootstrap(conf)
	if err != nil {
		t.Errorf("Unable to bootstrap boltdb %s: %s", path, err)
	}
	defer os.Remove(path)
	repo, err := New(conf)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	err = repo.Open()
	if err != nil {
		t.Errorf("Unable to open boltdb %s: %s", path, err)
	}
	if repo.d == nil {
		t.Errorf("Nil %T for path %s", repo.d, path)
	}
}

func TestRepo_Close(t *testing.T) {
	dir := os.TempDir()
	conf := Config{
		Path: dir,
	}
	path, _ := Path(conf)
	err := Bootstrap(conf)
	if err != nil {
		t.Errorf("Unable to bootstrap boltdb %s: %s", path, err)
	}
	defer os.Remove(path)

	repo, err := New(conf)
	if err != nil {
		t.Errorf("Error initializing db: %s", err)
	}
	err = repo.Open()
	if err != nil {
		t.Errorf("Unable to open boltdb %s: %s", path, err)
	}
	err = repo.close()
	if err != nil {
		t.Errorf("Unable to close boltdb %s: %s", path, err)
	}
	os.Remove(path)
}

func TestRepo_Load(t *testing.T) {
	t.Skipf("TODO")
}

func initBoltDBForTesting(t *testing.T) (*repo, error) {
	tempDir, err := Path(Config{Path: t.TempDir()})
	if err != nil {
		return nil, fmt.Errorf("invalid path for initializing boltdb %s: %s", tempDir, err)
	}
	r := &repo{
		root:  []byte(rootBucket),
		path:  tempDir,
		logFn: t.Logf,
		errFn: t.Errorf,
	}
	r.d, err = bolt.Open(tempDir, 0600, nil)
	defer r.d.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to open boltdb database at path %s: %s", tempDir, err)
	}
	err = r.d.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(rootBucket))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create boltdb root bucket %s: %s", r.path, err)
	}

	t.Logf("Initialized test db at %s", r.path)
	return r, nil
}

func Test_repo_AddTo(t *testing.T) {
	type args struct {
		col vocab.IRI
		it  vocab.ItemCollection
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "inbox One IRI",
			args: args{
				col: vocab.IRI("http://example.com/inbox"),
				it:  vocab.ItemCollection{vocab.IRI("http://example.com/1")},
			},
			wantErr: false,
		},
		{
			name: "replies One IRI",
			args: args{
				col: vocab.IRI("http://example.com/replies"),
				it:  vocab.ItemCollection{vocab.IRI("http://example.com/1")},
			},
			wantErr: false,
		},
		{
			name: "replies multiple IRI",
			args: args{
				col: vocab.IRI("http://example.com/replies"),
				it:  vocab.ItemCollection{vocab.IRI("http://example.com/1"), vocab.IRI("http://example.com/2")},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := initBoltDBForTesting(t)
			if err != nil {
				t.Errorf("Unable to initialize boltdb: %s", err)
			}

			for _, it := range tt.args.it {
				toCheck := vocab.Object{ID: it.GetLink()}
				if _, err = r.Save(toCheck); err != nil {
					t.Errorf("unable to save %s: %s", tt.args.it, err)
				}

				if err := r.AddTo(tt.args.col, it); (err != nil) != tt.wantErr {
					t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
				}
				if tt.wantErr {
					return
				}
			}

			res, err := r.Load(tt.args.col.GetLink())
			if err != nil {
				t.Errorf("unable to load %s: %s", tt.args.col, err)
			}
			for _, expected := range tt.args.it {
				err := vocab.OnCollectionIntf(res, func(col vocab.CollectionInterface) error {
					if col.Contains(expected) {
						return nil
					}
					return fmt.Errorf("unable to find expected item in loaded collection: %s", expected.GetLink())
				})
				if err != nil {
					t.Errorf("%s", err)
				}
			}
		})
	}
}
