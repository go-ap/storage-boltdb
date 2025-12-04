package boltdb

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/google/go-cmp/cmp"
)

func Test_New(t *testing.T) {
	dir := os.TempDir()

	conf := Config{
		Path:  dir,
		LogFn: func(s string, p ...interface{}) { t.Logf(s, p...) },
		ErrFn: func(s string, p ...interface{}) { t.Errorf(s, p...) },
	}
	repo, _ := New(conf)
	if repo == nil {
		t.Errorf("Nil result from opening boltdb %s", conf.Path)
		return
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

func withCreatePath(t *testing.T, r *repo) *repo {
	validPath, err := Path(Config{Path: r.path})
	if err != nil && r.errFn != nil {
		t.Errorf("Unable to build path from %s: %s", r.path, err)
	} else {
		r.path = validPath
	}
	return r
}

func Test_repo_Open(t *testing.T) {
	tempDir := t.TempDir()
	errTempIsDir := &fs.PathError{
		Op:   "open",
		Path: tempDir,
		Err:  syscall.EISDIR,
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: &fs.PathError{Op: "open", Path: "", Err: syscall.ENOENT},
		},
		{
			name:    "with invalid path",
			fields:  fields{path: tempDir},
			wantErr: errTempIsDir,
		},
		{
			name:     "with valid path",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withCreatePath},
		},
	}

	t.Run("Error on nil repo", func(t *testing.T) {
		var r *repo
		wantErr := errors.Newf("Unable to open uninitialized db")
		if err := r.Open(); !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("Open() error = %s", cmp.Diff(wantErr, err, EquateWeakErrors))
		}
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// NOTE(marius): we don't use the mockRepo function here as we need to check failure cases
			r := &repo{
				d:     tt.fields.d,
				root:  tt.fields.root,
				path:  tt.fields.path,
				logFn: t.Logf,
				errFn: t.Errorf,
			}
			for _, fn := range tt.setupFns {
				_ = fn(t, r)
			}

			if err := r.Open(); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Open() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
			if tt.wantErr != nil {
				return
			}
			if tt.fields.path != "" && r.d == nil {
				t.Errorf("Open() boltdb is nil for path %s: %T: %v", tt.fields.path, r.d, r.d)
			}
		})
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

func withOrderedCollection(iri vocab.IRI) initFn {
	return func(t *testing.T, r *repo) *repo {
		if _, err := r.Create(defaultCol(iri)); err != nil {
			t.Errorf("unable to save collection %s: %s", iri, err.Error())
		}
		return r
	}
}

func withCollection(iri vocab.IRI) initFn {
	col := &vocab.Collection{
		ID:        iri,
		Type:      vocab.CollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().Round(time.Second).UTC(),
	}
	return func(t *testing.T, r *repo) *repo {
		if _, err := r.Create(col); err != nil {
			t.Errorf("unable to save collection %s: %s", iri, err)
		}
		return r
	}
}

func withOrderedCollectionHavingItems(t *testing.T, r *repo) *repo {
	colIRI := vocab.IRI("https://example.com/followers")
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if _, err := r.Create(&col); err != nil {
		t.Errorf("unable to save collection %s: %s", colIRI, err)
	}
	obIRI := vocab.IRI("https://example.com")
	ob, err := save(r, vocab.Object{ID: obIRI})
	if err != nil {
		t.Errorf("unable to save item %s: %s", obIRI, err)
	}
	if err := r.AddTo(col.ID, ob); err != nil {
		t.Errorf("unable to add item to collection %s -> %s : %s", obIRI, colIRI, err)
	}
	return r
}

func withCollectionHavingItems(t *testing.T, r *repo) *repo {
	colIRI := vocab.IRI("https://example.com/followers")
	col := vocab.Collection{
		ID:        colIRI,
		Type:      vocab.CollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if _, err := r.Create(&col); err != nil {
		t.Errorf("unable to save collection %s: %s", colIRI, err)
	}
	obIRI := vocab.IRI("https://example.com")
	ob, err := save(r, vocab.Object{ID: obIRI})
	if err != nil {
		t.Errorf("unable to save item %s: %s", obIRI, err)
	}
	if err := r.AddTo(col.ID, ob); err != nil {
		t.Errorf("unable to add item to collection %s -> %s : %s", obIRI, colIRI, err)
	}
	return r
}

func withItems(items ...vocab.Item) initFn {
	return func(t *testing.T, r *repo) *repo {
		for _, it := range items {
			if _, err := save(r, it); err != nil {
				t.Errorf("unable to save item %s: %s", it.GetLink(), err)
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
			t.Cleanup(r.Close)

			err := r.RemoveFrom(tt.args.colIRI, tt.args.it)
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("RemoveFrom() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
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
			t.Cleanup(r.Close)

			err := r.AddTo(tt.args.colIRI, tt.args.it)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
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

func Test_repo_Load_UnhappyPath(t *testing.T) {
	type args struct {
		iri vocab.IRI
		fil filters.Checks
	}
	tests := []struct {
		name     string
		args     args
		setupFns []initFn
		want     vocab.Item
		wantErr  error
	}{
		{
			name:    "not opened",
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.NotFoundf("file not found"),
		},
		{
			name:     "empty iri gives us not found",
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.NotFoundf("file not found"),
		},
		{
			name:     "not bootstrapped",
			args:     args{iri: "https://example.com"},
			setupFns: []initFn{withOpenRoot},
			wantErr:  ErrorInvalidRoot(nil),
		},
		{
			name:     "invalid iri gives 404",
			args:     args{iri: "https://example.com/dsad"},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.NotFoundf("example.com not found"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: t.TempDir()}, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.Load(tt.args.iri, tt.args.fil...)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !vocab.ItemsEqual(got, tt.want) {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_repo_Load(t *testing.T) {
	// NOTE(marius): happy path tests for a fully mocked repo
	r := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withGeneratedMocks)
	t.Cleanup(r.Close)

	type args struct {
		iri vocab.IRI
		fil filters.Checks
	}
	tests := []struct {
		name    string
		args    args
		want    vocab.Item
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name:    "empty iri gives us not found",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name: "root iri gives us the root",
			args: args{iri: "https://example.com"},
			want: root,
		},
		{
			name:    "invalid iri gives 404",
			args:    args{iri: "https://example.com/dsad"},
			want:    nil,
			wantErr: errors.NotFoundf("dsad not found"),
		},
		{
			name: "first Person",
			args: args{iri: "https://example.com/person/1"},
			want: filter(*allActors.Load(), filters.HasType("Person")).First(),
		},
		{
			name: "first Follow",
			args: args{iri: "https://example.com/follow/1"},
			want: filter(*allActivities.Load(), filters.HasType("Follow")).First(),
		},
		{
			name: "first Image",
			args: args{iri: "https://example.com/image/1"},
			want: filter(*allObjects.Load(), filters.SameID("https://example.com/image/1")).First(),
		},
		{
			name: "full outbox",
			args: args{iri: rootOutboxIRI},
			want: wantsRootOutbox(),
		},
		{
			name: "limit to max 2 things",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{filters.WithMaxCount(2)},
			},
			want: wantsRootOutboxPage(2, filters.WithMaxCount(2)),
		},
		{
			name: "outbox?type=Create",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
				},
			},
			want: wantsRootOutbox(filters.HasType(vocab.CreateType)),
		},
		{
			name: "outbox?type=Create&actor.name=Hank",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Actor(filters.NameIs("Hank")),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Actor(filters.NameIs("Hank")),
			),
		},
		{
			name: "outbox?type=Create&object.tag=-",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Object(filters.Tag(filters.NilID)),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Object(filters.Tag(filters.NilID)),
			),
		},
		{
			name: "outbox?type=Create&object.tag.name=#test",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Object(filters.Tag(filters.NameIs("#test"))),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Object(filters.Tag(filters.NameIs("#test"))),
			),
		},
		{
			name: "outbox?type=Question&target.type=Note",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.QuestionType),
					filters.Target(filters.HasType(vocab.ImageType)),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Object(filters.HasType(vocab.NoteType)),
			),
		},
		{
			name: "outbox?type=Create&object.type=Note",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Actor(filters.NameIs("Hank")),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Object(filters.HasType(vocab.NoteType)),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Load(tt.args.iri, tt.args.fil...)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(tt.want, got, EquateItemCollections) {
				t.Errorf("Load() got = %s", cmp.Diff(tt.want, got, EquateItemCollections))
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
			t.Cleanup(r.Close)

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
			t.Cleanup(r.Close)

			if err := r.Delete(tt.it); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
