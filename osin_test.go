package boltdb

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
)

func Test_repo_Clone(t *testing.T) {
	s := new(repo)
	ss := s.Clone()
	s1, ok := ss.(*repo)
	if !ok {
		t.Errorf("Error when cloning repoage, unable to convert interface back to %T: %T", s, ss)
	}
	if !reflect.DeepEqual(s, s1) {
		t.Errorf("Error when cloning repoage, invalid pointer returned %p: %p", s, s1)
	}
}

func Test_repo_LoadAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errors.NotFoundf("Empty access code"),
		},
		{
			name:     "load access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization, withAccess},
			code:     "access-666",
			want:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadAccess(tt.code)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("LoadAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadAccess() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_LoadXXX_with_brokenDecode(t *testing.T) {
	wantErr := errors.Newf("broken")

	rr := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withMockItems, withMetadataJDoe, withClient, withAuthorization, withAccess)
	oldDecode := decodeFn
	decodeFn = func(_ []byte, m any) error {
		return wantErr
	}
	t.Cleanup(func() {
		rr.Close()
		decodeFn = oldDecode
	})

	t.Run("GetClient", func(t *testing.T) {
		_, err := rr.GetClient("test-client")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("GetClient() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadAuthorize", func(t *testing.T) {
		_, err := rr.LoadAuthorize("test-code")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("LoadAuthorize() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadAccess", func(t *testing.T) {
		_, err := rr.LoadAccess("access-666")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("LoadAccess() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadRefresh", func(t *testing.T) {
		_, err := rr.LoadRefresh("refresh-666")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("LoadRefresh() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadMetadata", func(t *testing.T) {
		err := rr.LoadMetadata("https://example.com/~jdoe", Metadata{Pw: []byte("asd"), PrivateKey: pkcs8Pk})
		if !errors.Is(err, wantErr) {
			t.Errorf("LoadMetadata() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadKey", func(t *testing.T) {
		_, err := rr.LoadKey("https://example.com/~jdoe")
		if !errors.Is(err, wantErr) {
			t.Errorf("LoadKey() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("PasswordCheck", func(t *testing.T) {
		err := rr.PasswordCheck("https://example.com/~jdoe", []byte("asd"))
		if !errors.Is(err, wantErr) {
			t.Errorf("PasswordCheck() error = %v, wantErr %v", err, wantErr)
		}
	})
}

func Test_repo_LoadRefresh(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "not open",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			fields:   fields{},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.NotFoundf("Empty refresh code"),
		},
		{
			name:     "with refresh",
			fields:   fields{path: t.TempDir()},
			code:     "refresh-666",
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization, withAccess},
			want:     mockAccess("access-666", defaultClient),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadRefresh(tt.code)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadRefresh() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_RemoveAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "remove access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.RemoveAccess(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "remove auth",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization},
			code:     "test-auth",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveAuthorize(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveClient(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "remove client",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient},
			code:     "test-client",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveClient(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveRefresh(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "not open",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty not open",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			fields:   fields{},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			code:     "test",
		},
		{
			name:     "mock access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.RemoveRefresh(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveXXX_with_brokenEncode(t *testing.T) {
	wantErr := errors.Newf("broken")

	rr := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withMockItems)
	oldEncode := encodeFn
	encodeFn = func(v any) ([]byte, error) {
		return nil, wantErr
	}
	t.Cleanup(func() {
		rr.Close()
		encodeFn = oldEncode
	})

	t.Run("CreateClient", func(t *testing.T) {
		err := rr.CreateClient(defaultClient)
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("CreateClient() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveAuthorize", func(t *testing.T) {
		err := rr.SaveAuthorize(mockAuth("test", defaultClient))
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveAccess", func(t *testing.T) {
		err := rr.SaveAccess(mockAccess("test-access", defaultClient))
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveAccess() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveMetadata", func(t *testing.T) {
		err := rr.SaveMetadata("https://example.com/~jdoe", Metadata{Pw: []byte("asd"), PrivateKey: pkcs8Pk})
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveMetadata() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("PasswordSet", func(t *testing.T) {
		err := rr.PasswordSet("https://example.com/~jdoe", []byte("dsa"))
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("PasswordSet() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveKey", func(t *testing.T) {
		_, err := rr.SaveKey("https://example.com/~jdoe", pk)
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveKey() error = %v, wantErr %v", err, wantErr)
		}
	})
}

func Test_repo_SaveAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		auth     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name:    "not open",
			path:    t.TempDir(),
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Errorf("unable to save nil authorization data"),
		},
		{
			name:     "save mock auth",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withClient},
			auth:     mockAuth("test-code123", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			t.Cleanup(r.Close)

			err := r.SaveAuthorize(tt.auth)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			got, err := r.LoadAuthorize(tt.auth.Code)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			gotJson, _ := json.Marshal(got)
			wantJson, _ := json.Marshal(tt.auth)
			if !bytes.Equal(gotJson, wantJson) {
				t.Errorf("SaveAuthorize() got =\n%s\n====\n%s", gotJson, wantJson)
			}
		})
	}
}

func Test_repo_LoadAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		code     string
		want     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name: "empty",
			path: t.TempDir(),
		},
		{
			name:     "authorized",
			path:     t.TempDir(),
			code:     "test-code",
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization},
			want:     mockAuth("test-code", defaultClient),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadAuthorize(tt.code)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("LoadAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("LoadAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			gotJson, _ := json.Marshal(got)
			wantJson, _ := json.Marshal(tt.want)
			if !bytes.Equal(gotJson, wantJson) {
				t.Errorf("LoadAuthorize() got =\n%s\n====\n%s", gotJson, wantJson)
			}
		})
	}
}

func Test_repo_SaveAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		data     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "save access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization},
			data:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.SaveAccess(tt.data); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_ListClients(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		want     []osin.Client
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			setupFns: []initFn{withOpenRoot, withClient},
			fields:   fields{path: t.TempDir()},
			want:     []osin.Client{defaultClient},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.ListClients()
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("ListClients() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListClients() got = %v, want %v", got, tt.want)
			}
		})
	}
}
