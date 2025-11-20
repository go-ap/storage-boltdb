package boltdb

import (
	"fmt"
	"os"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
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

func defaultCol(iri vocab.IRI) vocab.CollectionInterface {
	return &vocab.OrderedCollection{
		ID:        iri,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().Round(time.Second).UTC(),
	}
}

func withOrderedCollection(iri vocab.IRI) func(r *repo) *repo {
	return func(r *repo) *repo {
		if _, err := r.Create(defaultCol(iri)); err != nil {
			r.errFn("unable to save collection %s: %s", iri, err.Error())
		}
		return r
	}
}

func withCollection(iri vocab.IRI) func(r *repo) *repo {
	col := &vocab.Collection{
		ID:        iri,
		Type:      vocab.CollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().Round(time.Second).UTC(),
	}
	return func(r *repo) *repo {
		if _, err := r.Create(col); err != nil {
			r.errFn("unable to save collection %s: %s", iri, err)
		}
		return r
	}
}

func withOrderedCollectionHavingItems(r *repo) *repo {
	colIRI := vocab.IRI("https://example.com/followers")
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if _, err := r.Create(&col); err != nil {
		r.errFn("unable to save collection %s: %s", colIRI, err)
	}
	obIRI := vocab.IRI("https://example.com")
	ob, err := save(r, vocab.Object{ID: obIRI})
	if err != nil {
		r.errFn("unable to save item %s: %s", obIRI, err)
	}
	if err := r.AddTo(col.ID, ob); err != nil {
		r.errFn("unable to add item to collection %s -> %s : %s", obIRI, colIRI, err)
	}
	return r
}

func withCollectionHavingItems(r *repo) *repo {
	colIRI := vocab.IRI("https://example.com/followers")
	col := vocab.Collection{
		ID:        colIRI,
		Type:      vocab.CollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if _, err := r.Create(&col); err != nil {
		r.errFn("unable to save collection %s: %s", colIRI, err)
	}
	obIRI := vocab.IRI("https://example.com")
	ob, err := save(r, vocab.Object{ID: obIRI})
	if err != nil {
		r.errFn("unable to save item %s: %s", obIRI, err)
	}
	if err := r.AddTo(col.ID, ob); err != nil {
		r.errFn("unable to add item to collection %s -> %s : %s", obIRI, colIRI, err)
	}
	return r
}

var withJdoeInbox = withOrderedCollection("https://example.com/~jdoe/inbox")

func withItems(items ...vocab.Item) initFn {
	return func(r *repo) *repo {
		for _, it := range items {
			if _, err := save(r, it); err != nil {
				r.errFn("unable to save item %s: %s", it.GetLink(), err)
			}
		}
		return r
	}
}

func Test_repo_RemoveFrom(t *testing.T) {
	type args struct {
		colIRI vocab.IRI
		it     vocab.Item
	}

	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "not open",
			path:    t.TempDir(),
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot},
			args:     args{},
			wantErr:  errors.NotFoundf("Unable to load root bucket"),
		},
		{
			name:     "collection doesn't exist",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("Unable to load root bucket"),
		},
		{
			name:     "item doesn't exist in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withOrderedCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item exists in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withOrderedCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item doesn't exist in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil, // if the item doesn't exist, we don't error out, unsure if that makes sense
		},
		{
			name:     "item exists in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			defer r.Close()

			err := r.RemoveFrom(tt.args.colIRI, tt.args.it)
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("RemoveFrom() error = %s", cmp.Diff(tt.wantErr, err))
				return
			}
			if tt.wantErr != nil {
				// NOTE(marius): if we expected an error we don't need to following tests
				return
			}

			it, err := r.Load(tt.args.colIRI)
			if err != nil {
				t.Errorf("Load() after RemoveFrom() error = %v", err)
				return
			}

			col, ok := it.(vocab.CollectionInterface)
			if !ok {
				t.Errorf("Load() after RemoveFrom(), didn't return a CollectionInterface type")
				return
			}

			if col.Contains(tt.args.it) {
				t.Errorf("Load() after RemoveFrom(), the item is still in collection %#v", col.Collection())
			}

			// NOTE(marius): this is a bit of a hackish way to skip testing of the object when we didn't
			// save it to the disk
			if vocab.IsObject(tt.args.it) {
				ob, err := r.Load(tt.args.it.GetLink())
				if err != nil {
					t.Errorf("Load() of the object after RemoveFrom() error = %v", err)
					return
				}
				if !vocab.ItemsEqual(ob, tt.args.it) {
					t.Errorf("Loaded item after RemoveFrom(), is not equal %#v with the one provided %#v", ob, tt.args.it)
				}
			}
		})
	}
}

func Test_repo_AddTo(t *testing.T) {
	type args struct {
		colIRI vocab.IRI
		it     vocab.Item
	}

	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		setup    func(*repo) error
		args     args
		wantErr  error
	}{
		{
			name:    "not open",
			path:    t.TempDir(),
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot},
			args:     args{},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name:     "collection doesn't exist",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name:     "item doesn't exist in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("invalid item to add to collection"),
		},
		{
			name:     "item doesn't exist in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withOrderedCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("invalid item to add to collection"),
		},
		{
			name:     "item exists in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withOrderedCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item exists in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item to non-existent hidden collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withItems(&vocab.Object{ID: "https://example.com/example", Type: vocab.NoteType})},
			args: args{
				colIRI: "https://example.com/~jdoe/blocked",
				it:     vocab.IRI("https://example.com/example"),
			},
			wantErr: nil,
		},
		{
			name:     "item to hidden collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollection("https://example.com/~jdoe/blocked"), withItems(&vocab.Object{ID: "https://example.com/example", Type: vocab.NoteType})},
			args: args{
				colIRI: "https://example.com/~jdoe/blocked",
				it:     vocab.IRI("https://example.com/example"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			defer r.Close()

			err := r.AddTo(tt.args.colIRI, tt.args.it)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			it, err := r.Load(tt.args.colIRI)
			if err != nil {
				t.Errorf("Load() after AddTo() error = %v", err)
				return
			}

			col, ok := it.(vocab.CollectionInterface)
			if !ok {
				t.Errorf("Load() after AddTo(), didn't return a CollectionInterface type")
				return
			}

			if !col.Contains(tt.args.it) {
				t.Errorf("Load() after AddTo(), the item is not in collection %#v", col.Collection())
			}

			ob, err := r.Load(tt.args.it.GetLink())
			if err != nil {
				t.Errorf("Load() of the object after AddTo() error = %v", err)
				return
			}
			if !vocab.ItemsEqual(ob, tt.args.it) {
				t.Errorf("Loaded item after AddTo(), is not equal %#v with the one provided %#v", ob, tt.args.it)
			}
		})
	}
}

func Test_repo_Save(t *testing.T) {
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		it       vocab.Item
		want     vocab.Item
		wantErr  error
	}
	tests := []test{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty item can't be saved",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.Newf("Unable to save nil element"),
		},
		{
			name:     "save item collection",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			fields:   fields{path: t.TempDir()},
			it:       mockItems,
			want:     mockItems,
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name:     fmt.Sprintf("save %d %T to repo", i, mockIt),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			fields:   fields{path: t.TempDir()},
			it:       mockIt,
			want:     mockIt,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			got, err := r.Save(tt.it)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Save() error = %s", cmp.Diff(tt.wantErr, err))
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Save() got = %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_Delete(t *testing.T) {
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		it       vocab.Item
		wantErr  error
	}
	tests := []test{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty item won't return an error",
			setupFns: []initFn{withOpenRoot},
			fields:   fields{path: t.TempDir()},
		},
		{
			name:     "delete item collection",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withItems(mockItems)},
			it:       mockItems,
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name:     fmt.Sprintf("delete %d %T from repo", i, mockIt),
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withMockItems},
			it:       mockIt,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.Delete(tt.it); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
