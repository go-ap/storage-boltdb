package boltdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	bolt "go.etcd.io/bbolt"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

type repo struct {
	d     *bolt.DB
	root  []byte
	path  string
	logFn loggerFn
	errFn loggerFn
}

type loggerFn func(string, ...interface{})

const (
	rootBucket       = ":"
	bucketActors     = filters.ActorsType
	bucketActivities = filters.ActivitiesType
	bucketObjects    = filters.ObjectsType
)

// Config
type Config struct {
	Path  string
	LogFn loggerFn
	ErrFn loggerFn
}

var defaultLogFn = func(string, ...interface{}) {}

// New returns a new repo repository
func New(c Config) (*repo, error) {
	p, err := Path(c)
	if err != nil {
		return nil, err
	}
	b := repo{
		root:  []byte(rootBucket),
		path:  p,
		logFn: defaultLogFn,
		errFn: defaultLogFn,
	}
	if c.ErrFn != nil {
		b.errFn = c.ErrFn
	}
	if c.LogFn != nil {
		b.logFn = c.LogFn
	}
	return &b, nil
}

func loadRawItemFromBucket(b *bolt.Bucket) (vocab.Item, error) {
	raw := b.Get([]byte(objectKey))
	if raw == nil {
		return nil, errors.NotFoundf("not found")
	}
	it, err := decodeItemFn(raw)
	if err != nil {
		return nil, err
	}
	if vocab.IsNil(it) {
		return nil, errors.NotFoundf("not found")
	}
	return it, nil
}

func (r *repo) loadItem(b *bolt.Bucket) (vocab.Item, error) {
	// we have found an item
	it, err := loadRawItemFromBucket(b)
	if err != nil {
		return nil, err
	}
	if it.IsCollection() {
		// we need to dereference them, so no further filtering/processing is needed here
		return it, nil
	}
	if vocab.IsIRI(it) {
		if it, _ = r.loadOneFromBucket(it.GetLink()); vocab.IsNil(it) {
			return nil, errors.NotFoundf("not found")
		}
	}
	typ := it.GetType()
	if vocab.ActorTypes.Contains(typ) {
		_ = vocab.OnActor(it, loadFilteredPropsForActor(r))
	}
	if vocab.ObjectTypes.Contains(typ) {
		_ = vocab.OnObject(it, loadFilteredPropsForObject(r))
	}
	if vocab.IntransitiveActivityTypes.Contains(typ) {
		_ = vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(r))
	}
	if vocab.ActivityTypes.Contains(typ) {
		_ = vocab.OnActivity(it, loadFilteredPropsForActivity(r))
	}
	return it, nil
}

func loadFilteredPropsForActor(r *repo) func(a *vocab.Actor) error {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(r))
	}
}

func loadFilteredPropsForObject(r *repo) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := r.loadOneFromBucket(t.GetLink()); err == nil {
					(*col)[i] = ob
				}
			}
			return nil
		})
	}
}

func loadFilteredPropsForActivity(r *repo) func(a *vocab.Activity) error {
	return func(a *vocab.Activity) error {
		if !vocab.IsNil(a.Object) && vocab.IsIRI(a.Object) {
			if ob, err := r.loadOneFromBucket(a.Object.GetLink()); err == nil {
				a.Object = ob
			}
		}
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(r))
	}
}

func loadFilteredPropsForIntransitiveActivity(r *repo) func(a *vocab.IntransitiveActivity) error {
	return func(a *vocab.IntransitiveActivity) error {
		if !vocab.IsNil(a.Actor) && vocab.IsIRI(a.Actor) {
			if act, err := r.loadOneFromBucket(a.Actor.GetLink()); err == nil {
				a.Actor = act
			}
		}
		if !vocab.IsNil(a.Target) && vocab.IsIRI(a.Target) {
			if t, err := r.loadOneFromBucket(a.Target.GetLink()); err == nil {
				a.Target = t
			}
		}
		return nil
	}
}

func (r *repo) loadItemsElements(f vocab.IRI, iris ...vocab.Item) (vocab.ItemCollection, error) {
	col := make(vocab.ItemCollection, 0)
	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return ErrorInvalidRoot(r.root)
		}
		var err error
		for _, iri := range iris {
			var b *bolt.Bucket
			remainderPath := itemBucketPath(iri.GetLink())
			b, remainderPath, err = descendInBucket(rb, remainderPath, false)
			if err != nil || b == nil {
				continue
			}
			it, err := r.loadItem(b)
			if err != nil || vocab.IsNil(it) {
				continue
			}
			col = append(col, it)
		}
		return nil
	})
	return col, err
}

func (r *repo) loadOneFromBucket(iri vocab.IRI) (vocab.Item, error) {
	col, err := r.loadFromBucket(iri)
	if err != nil {
		return nil, err
	}
	if vocab.IsNil(col) {
		return nil, errors.NotFoundf("not found")
	}
	if !col.IsCollection() {
		return col, nil
	}
	var it vocab.Item
	err = vocab.OnCollectionIntf(col, func(c vocab.CollectionInterface) error {
		it = c.Collection().First()
		return nil
	})

	return it, err
}

var orderedCollectionTypes = vocab.ActivityVocabularyTypes{vocab.OrderedCollectionPageType, vocab.OrderedCollectionType}
var collectionTypes = vocab.ActivityVocabularyTypes{vocab.CollectionPageType, vocab.CollectionType}

func (r *repo) iterateInBucket(b *bolt.Bucket, iri vocab.IRI) (vocab.Item, uint, error) {
	if b == nil {
		return nil, 0, errors.Errorf("invalid bucket to load from")
	}
	col, err := loadRawItemFromBucket(b)
	if err != nil {
		return nil, 0, err
	}
	// try to iterate in the current collection
	isObjectKey := func(k []byte) bool {
		return string(k) == objectKey || string(k) == metaDataKey
	}
	c := b.Cursor()
	if c == nil {
		return nil, 0, errors.Errorf("Invalid bucket cursor")
	}
	items := make(vocab.ItemCollection, 0)
	// if no path was returned from descendIntoBucket we iterate over all keys in the current bucket
	for key, _ := c.First(); key != nil; key, _ = c.Next() {
		ob := b
		// TODO(marius): we need to see if we can avoid iterating in a bucket for both the UUID, and the underlying
		//  __raw object, because currently they both get loaded and we need to use col.Contains to avoid duplication
		//  when loading some of the actor collections.
		if !isObjectKey(key) {
			if ob = b.Bucket(key); ob == nil {
				continue
			}
		}
		it, err := r.loadItem(ob)
		if err != nil || vocab.IsNil(it) {
			continue
		}
		if it.IsCollection() {
			_ = vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
				itCol, err := r.loadItemsElements(iri, c.Collection()...)
				if err != nil {
					return err
				}
				for _, it := range itCol {
					if items.Contains(it.GetLink()) {
						continue
					}
					_ = items.Append(it)
				}
				return nil
			})
		} else if !items.Contains(it.GetLink()) {
			_ = items.Append(it)
		}
	}

	if orderedCollectionTypes.Contains(col.GetType()) {
		err = vocab.OnOrderedCollection(col, buildOrderedCollection(items))
	} else {
		err = vocab.OnCollection(col, buildCollection(items))
	}
	return col, uint(len(items)), err
}

var ErrorInvalidRoot = func(b []byte) error {
	return errors.NotFoundf("Invalid root bucket %s", b)
}

func (r *repo) loadFromBucket(iri vocab.IRI) (vocab.Item, error) {
	var it vocab.Item
	err := r.d.View(func(tx *bolt.Tx) error {
		rb := tx.Bucket(r.root)
		if rb == nil {
			return ErrorInvalidRoot(r.root)
		}

		// This is the case where the Filter points to a single AP Object IRI
		// TODO(marius): Ideally this should support the case where we use the IRI to point to a bucket path
		//     and on top of that apply the other filters
		fullPath := itemBucketPath(iri)
		var remainderPath []byte

		var err error
		var b *bolt.Bucket

		// Assume bucket exists and has keys
		b, remainderPath, err = descendInBucket(rb, fullPath, false)
		if err != nil {
			return err
		}
		if b == nil {
			return errors.Errorf("Invalid bucket %s", fullPath)
		}

		if isStorageCollectionKey(string(fullPath)) {
			fromBucket, _, err := r.iterateInBucket(b, iri)
			if err != nil {
				return err
			}
			_ = vocab.OnObject(fromBucket, func(ob *vocab.Object) error {
				ob.ID = iri
				return nil
			})
			it = fromBucket
		} else {
			if len(remainderPath) == 0 {
				// we have found an item
				it, err = r.loadItem(b)
				if err != nil {
					return err
				}
				if it.IsCollection() {
					return vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
						it, err = r.loadItemsElements(iri, c.Collection()...)
						return err
					})
				}
				return nil
			}
		}
		return nil
	})

	return it, err
}

// Load
func (r *repo) Load(i vocab.IRI, fil ...filters.Check) (vocab.Item, error) {
	ret, err := r.loadFromBucket(i)
	return filters.Checks(fil).Run(ret), err
}

var pathSeparator = []byte{'/'}

func descendInBucket(root *bolt.Bucket, path []byte, create bool) (*bolt.Bucket, []byte, error) {
	if root == nil {
		return nil, path, errors.Newf("trying to descend into nil bucket")
	}
	if len(path) == 0 {
		return root, path, nil
	}
	bucketNames := bytes.Split(bytes.TrimRight(path, string(pathSeparator)), pathSeparator)

	lvl := 0
	b := root
	// descend the bucket tree up to the last found bucket
	for _, name := range bucketNames {
		lvl++
		if len(name) == 0 {
			continue
		}
		if b == nil {
			return root, path, errors.Errorf("trying to load from nil bucket")
		}
		var cb *bolt.Bucket
		if create {
			cb, _ = b.CreateBucketIfNotExists(name)
		} else {
			cb = b.Bucket(name)
		}
		if cb == nil {
			lvl--
			break
		}
		b = cb
	}
	remBuckets := bucketNames[lvl:]
	path = bytes.Join(remBuckets, pathSeparator)
	if len(remBuckets) > 0 && !filters.HiddenCollections.Contains(vocab.CollectionPath(path)) {
		return b, path, errors.NotFoundf("%s not found", remBuckets[0])
	}
	return b, path, nil
}

const objectKey = "__raw"
const metaDataKey = "__meta_data"

func delete(r *repo, it vocab.Item) error {
	if it.IsCollection() {
		return vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
			var err error
			for _, it := range c.Collection() {
				if err = deleteItem(r, it); err != nil {
					r.logFn("Unable to remove item %s", it.GetLink())
				}
			}
			return nil
		})
	}

	return deleteItem(r, it.GetLink())
}

// Create
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	var err error

	cPath := itemBucketPath(col.GetLink())
	err = r.d.Update(func(tx *bolt.Tx) error {
		root, err := rootFromTx(tx, r.root)
		if err != nil {
			return err
		}
		b, _, err := descendInBucket(root, cPath, true)
		if err != nil {
			return errors.Annotatef(err, "Unable to find path %s/%s", r.root, cPath)
		}
		return saveRawItem(col, b)
	})
	return col, err
}

func itemBucketPath(iri vocab.IRI) []byte {
	url, err := iri.URL()
	if err != nil {
		return nil
	}
	return []byte(url.Host + url.Path)
}

func createCollection(b *bolt.Bucket, colIRI vocab.IRI, owner vocab.Item) (vocab.CollectionInterface, error) {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if !vocab.IsNil(owner) {
		col.AttributedTo = owner.GetLink()
	}
	return saveCollection(b, &col)
}

func saveCollection(b *bolt.Bucket, col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if err := saveRawItem(col, b); err != nil {
		return nil, err
	}

	err := vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
		col = c
		return nil
	})
	return col, err
}

func saveNewCollection(it vocab.Item, b *bolt.Bucket, owner vocab.Item) (vocab.Item, error) {
	colObject, err := loadRawItemFromBucket(b)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	if colObject == nil {
		it, err = createCollection(b, it.GetLink(), owner)
	}
	return it.GetLink(), nil
}

func createCollectionInBucket(parent *bolt.Bucket, it vocab.Item, owner vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	p := []byte(filepath.Base(it.GetLink().String()))
	b, err := parent.CreateBucketIfNotExists(p)
	if err != nil {
		return nil, err
	}

	return saveNewCollection(it, b, owner)
}

func deleteBucket(b *bolt.Bucket, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	p := []byte(it.GetLink())
	return b.DeleteBucket(p)
}

func createCollectionsInBucket(b *bolt.Bucket, it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	// create collections
	if vocab.ActorTypes.Contains(it.GetType()) {
		_ = vocab.OnActor(it, func(p *vocab.Actor) error {
			if p.Inbox != nil {
				p.Inbox, _ = createCollectionInBucket(b, vocab.Inbox.IRI(p), p)
			}
			if p.Outbox != nil {
				p.Outbox, _ = createCollectionInBucket(b, vocab.Outbox.IRI(p), p)
			}
			if p.Followers != nil {
				p.Followers, _ = createCollectionInBucket(b, vocab.Followers.IRI(p), p)
			}
			if p.Following != nil {
				p.Following, _ = createCollectionInBucket(b, vocab.Following.IRI(p), p)
			}
			if p.Liked != nil {
				p.Liked, _ = createCollectionInBucket(b, vocab.Liked.IRI(p), p)
			}
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		if o.Replies != nil {
			o.Replies, _ = createCollectionInBucket(b, vocab.Replies.IRI(o), o)
		}
		if o.Likes != nil {
			o.Likes, _ = createCollectionInBucket(b, vocab.Likes.IRI(o), o)
		}
		if o.Shares != nil {
			o.Shares, _ = createCollectionInBucket(b, vocab.Shares.IRI(o), o)
		}
		return nil
	})
}

// deleteItem
func deleteItem(r *repo, it vocab.Item) error {
	pathInBucket := itemBucketPath(it.GetLink())
	return r.d.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(r.root)
		if root == nil {
			return ErrorInvalidRoot(r.root)
		}
		if !root.Writable() {
			return errors.Errorf("Non writeable bucket %s", r.root)
		}
		b, _, err := descendInBucket(root, pathInBucket, true)
		if err != nil {
			return errors.Annotatef(err, "Unable to find %s in root bucket", pathInBucket)
		}
		if !b.Writable() {
			return errors.Errorf("Non writeable bucket %s", pathInBucket)
		}
		return deleteBucket(b, it)
	})
}

// deleteCollectionsFromBucket
func deleteCollectionsFromBucket(b *bolt.Bucket, it vocab.Item) error {
	if vocab.ActorTypes.Contains(it.GetType()) {
		return vocab.OnActor(it, func(p *vocab.Actor) error {
			var err error
			err = deleteBucket(b, vocab.Inbox.IRI(p))
			err = deleteBucket(b, vocab.Outbox.IRI(p))
			err = deleteBucket(b, vocab.Followers.IRI(p))
			err = deleteBucket(b, vocab.Following.IRI(p))
			err = deleteBucket(b, vocab.Liked.IRI(p))
			return err
		})
	}
	if vocab.ObjectTypes.Contains(it.GetType()) {
		return vocab.OnObject(it, func(o *vocab.Object) error {
			var err error
			err = deleteBucket(b, vocab.Replies.IRI(o))
			err = deleteBucket(b, vocab.Likes.IRI(o))
			err = deleteBucket(b, vocab.Shares.IRI(o))
			return err
		})
	}
	return nil
}

func saveRawItem(it vocab.Item, b *bolt.Bucket) error {
	if !b.Writable() {
		return errors.Errorf("Non writeable bucket")
	}

	entryBytes, err := encodeItemFn(it)
	if err != nil {
		return errors.Annotatef(err, "could not marshal object")
	}
	err = b.Put([]byte(objectKey), entryBytes)
	if err != nil {
		return errors.Annotatef(err, "could not store encoded object")
	}

	return nil
}

func rootFromTx(tx *bolt.Tx, path []byte) (*bolt.Bucket, error) {
	root, err := tx.CreateBucketIfNotExists(path)
	if err != nil {
		return root, errors.Errorf("Not able to write to root bucket %s", path)
	}
	if root == nil {
		return root, ErrorInvalidRoot(path)
	}
	if !root.Writable() {
		return root, errors.Errorf("Non writeable bucket %s", path)
	}
	return root, nil
}

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	pathInBucket := itemBucketPath(it.GetLink())
	err := r.d.Update(func(tx *bolt.Tx) error {
		root, err := rootFromTx(tx, r.root)
		if err != nil {
			return errors.Annotatef(err, "Unable to load root bucket")
		}
		b, uuid, err := descendInBucket(root, pathInBucket, true)
		if err != nil {
			return errors.Annotatef(err, "Unable to find %s in root bucket", pathInBucket)
		}
		if !b.Writable() {
			return errors.Errorf("Non writeable bucket %s", pathInBucket)
		}
		if len(uuid) == 0 {
			if err := createCollectionsInBucket(b, it); err != nil {
				return errors.Annotatef(err, "could not create object's collections")
			}
		}

		return saveRawItem(it, b)
	})

	return it, err
}

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	it, err := save(r, it)
	if err == nil {
		op := "Updated"
		if id := it.GetID(); !id.IsValid() {
			op = "Added new"
		}
		r.logFn("%s %s", op, it.GetLink())
	}

	return it, err
}

// RemoveFrom
func (r *repo) RemoveFrom(colIRI vocab.IRI, it vocab.Item) error {
	pathInBucket := itemBucketPath(colIRI.GetLink())
	return r.d.Update(func(tx *bolt.Tx) error {
		root, err := rootFromTx(tx, r.root)
		if err != nil {
			return errors.Annotatef(err, "Unable to load root bucket")
		}
		b, _, err := descendInBucket(root, pathInBucket, true)
		if err != nil {
			return errors.Annotatef(err, "Unable to find %s in root bucket", pathInBucket)
		}
		if !b.Writable() {
			return errors.Errorf("Non writeable bucket %s", pathInBucket)
		}
		col, err := loadRawItemFromBucket(b)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		if col == nil {
			col, err = createCollection(b, colIRI, nil)
			if err != nil {
				return err
			}
		}

		err = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			items := make(vocab.ItemCollection, 0)
			for _, iri := range c.OrderedItems {
				if !iri.GetLink().Equals(it.GetLink(), false) {
					items.Append(iri.GetLink())
				}
			}
			c.TotalItems -= 1
			c.OrderedItems = items
			return nil
		})
		if err != nil {
			return err
		}
		_, err = saveNewCollection(col, b, nil)
		return err
	})
}

func buildCollection(items vocab.ItemCollection) vocab.WithCollectionFn {
	return func(col *vocab.Collection) error {
		col.Items = items
		col.TotalItems = uint(len(items))
		return nil
	}
}

func buildOrderedCollection(items vocab.ItemCollection) vocab.WithOrderedCollectionFn {
	return func(col *vocab.OrderedCollection) error {
		col.OrderedItems = items
		col.TotalItems = uint(len(items))
		return nil
	}
}

func iriIsStorageCollection(i vocab.IRI) bool {
	_, lst := vocab.Split(i)
	return isStorageCollectionKey(string(lst))
}

func isStorageCollectionKey(p string) bool {
	lst := vocab.CollectionPath(filepath.Base(p))
	return filters.FedBOXCollections.Contains(lst) || vocab.OfActor.Contains(lst) || vocab.OfObject.Contains(lst)
}

var allStorageCollections = append(vocab.ActivityPubCollections, filters.FedBOXCollections...)

// AddTo
func (r *repo) AddTo(colIRI vocab.IRI, it vocab.Item) error {
	pathInBucket := itemBucketPath(colIRI.GetLink())
	return r.d.Update(func(tx *bolt.Tx) error {
		root, err := rootFromTx(tx, r.root)
		if err != nil {
			return errors.Annotatef(err, "Unable to load root bucket")
		}
		b, _, err := descendInBucket(root, pathInBucket, true)
		if err != nil {
			return errors.Annotatef(err, "Unable to find %s in root bucket", pathInBucket)
		}
		if !b.Writable() {
			return errors.Errorf("Non writeable bucket %s", pathInBucket)
		}
		col, err := loadRawItemFromBucket(b)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		if col == nil {
			col, err = createCollection(b, colIRI, nil)
			if err != nil {
				return err
			}
		}

		err = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			if !c.Contains(it.GetLink()) {
				c.Append(it.GetLink())
				c.TotalItems += 1
			}
			return nil
		})
		if err != nil {
			return err
		}
		return saveRawItem(col, b)
	})
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	return delete(r, it)
}

// Open opens the boltdb database if possible.
func (r *repo) Open() error {
	if r == nil {
		return errors.Newf("Unable to open uninitialized db")
	}
	if r.d == nil {
		var err error
		r.d, err = bolt.Open(r.path, 0600, nil)
		if err != nil {
			return errors.Annotatef(err, "Could not open db %s", r.path)
		}
	}
	return nil
}

func (r *repo) close() error {
	fmt.Fprintf(os.Stderr, "Closing boltdb %s[%t]", r.path, r.d != nil)
	if r == nil {
		return errors.Newf("Unable to close uninitialized db")
	}
	if r.d == nil {
		return nil
	}
	err := r.d.Close()
	r.d = nil
	return err
}

func Path(c Config) (string, error) {
	if !filepath.IsAbs(c.Path) {
		c.Path, _ = filepath.Abs(c.Path)
	}
	if err := mkDirIfNotExists(c.Path); err != nil {
		return "", err
	}
	p := filepath.Join(c.Path, "storage.bdb")
	return p, nil
}

func mkDirIfNotExists(p string) error {
	fi, err := os.Stat(p)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(p, os.ModeDir|os.ModePerm|0700)
	}
	if err != nil {
		return err
	}
	fi, err = os.Stat(p)
	if err != nil {
		return err
	} else if !fi.IsDir() {
		return errors.Errorf("path exists, and is not a folder %s", p)
	}
	return nil
}
