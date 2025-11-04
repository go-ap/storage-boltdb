//go:build conformance

package boltdb

import (
	"testing"

	conformance "github.com/go-ap/storage-conformance-suite"
)

func initStorage(t *testing.T) conformance.ActivityPubStorage {
	storage, err := New(Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("unable to initialize storage: %s", err)
	}
	if err = storage.Open(); err != nil {
		t.Fatalf("unable to open storage: %s", err)
	}
	return storage
}

func Test_Conformance(t *testing.T) {
	conformance.Suite(
		conformance.TestActivityPub, conformance.TestMetadata,
		conformance.TestKey, conformance.TestOAuth, conformance.TestPassword,
	).Run(t, initStorage(t))
}
