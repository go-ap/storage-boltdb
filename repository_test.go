package boltdb

import (
	"os"
	"testing"
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
	url := "random-string-not-an-URL"
	conf := Config{Path: dir}
	path, _ := Path(conf)
	err := Bootstrap(conf, url)
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
	url := "random-string-not-an-URL"
	conf := Config{
		Path: dir,
	}
	path, _ := Path(conf)
	err := Bootstrap(conf, url)
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

func TestRepo_GenerateID(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_Load(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_LoadActivities(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_LoadActors(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_LoadCollection(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_LoadObjects(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_UpdateObject(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_DeleteObject(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_SaveObject(t *testing.T) {
	t.Skipf("TODO")
}

func TestRepo_SaveActivity(t *testing.T) {
	t.Skipf("TODO")
}
