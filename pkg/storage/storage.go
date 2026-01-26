package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrBucketReferenceMissing = errors.New("bucket_id or bucket_name is required")
	ErrBucketIDMissing        = errors.New("bucket_id is required")
	ErrObjectKeyMissing       = errors.New("object_key is required")
)

// Store defines the attribute persistence contract. Implementations can back
// this with in-memory maps, embedded key-value databases, or external services.
type Store interface {
	GetBucketAttributes(ctx context.Context, ref BucketRef) (map[string][]byte, error)
	SetBucketAttributes(ctx context.Context, ref BucketRef, toAdd map[string][]byte, toDelete []string, replace bool) error
	PurgeBucketAttributes(ctx context.Context, ref BucketRef) error

	GetObjectAttributes(ctx context.Context, ref ObjectRef) (map[string][]byte, error)
	SetObjectAttributes(ctx context.Context, ref ObjectRef, toAdd map[string][]byte, toDelete []string, newInstance bool) error
	PurgeObjectAttributes(ctx context.Context, ref ObjectRef) error
}

// BucketRef identifies a bucket using either bucket ID or bucket name (optionally scoped by user).
type BucketRef struct {
	UserID     string
	BucketID   string
	BucketName string
}

// ObjectRef identifies an object instance inside a bucket.
type ObjectRef struct {
	Bucket           BucketRef
	ObjectKey        string
	ObjectInstanceID string
}

// NewInMemoryStore builds a thread-safe in-memory attribute store.
func NewInMemoryStore() Store {
	return &inMemoryStore{
		buckets: map[string]map[string][]byte{},
		objects: map[string]map[string][]byte{},
	}
}

type inMemoryStore struct {
	mu      sync.RWMutex
	buckets map[string]map[string][]byte
	objects map[string]map[string][]byte
}

func (s *inMemoryStore) GetBucketAttributes(_ context.Context, ref BucketRef) (map[string][]byte, error) {
	key, err := bucketKey(ref)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneMap(s.buckets[key]), nil
}

func (s *inMemoryStore) SetBucketAttributes(_ context.Context, ref BucketRef, toAdd map[string][]byte, toDelete []string, replace bool) error {
	key, err := bucketKey(ref)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	base := map[string][]byte{}
	if !replace {
		base = cloneMap(s.buckets[key])
	}

	for _, k := range toDelete {
		delete(base, k)
	}
	for k, v := range toAdd {
		base[k] = cloneBytes(v)
	}

	s.buckets[key] = base
	return nil
}

func (s *inMemoryStore) PurgeBucketAttributes(_ context.Context, ref BucketRef) error {
	key, err := bucketKey(ref)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buckets, key)
	return nil
}

func (s *inMemoryStore) GetObjectAttributes(_ context.Context, ref ObjectRef) (map[string][]byte, error) {
	key, err := objectKey(ref)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneMap(s.objects[key]), nil
}

func (s *inMemoryStore) SetObjectAttributes(_ context.Context, ref ObjectRef, toAdd map[string][]byte, toDelete []string, newInstance bool) error {
	key, err := objectKey(ref)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	base := map[string][]byte{}
	if !newInstance {
		base = cloneMap(s.objects[key])
	}

	for _, k := range toDelete {
		delete(base, k)
	}
	for k, v := range toAdd {
		base[k] = cloneBytes(v)
	}

	s.objects[key] = base
	return nil
}

func (s *inMemoryStore) PurgeObjectAttributes(_ context.Context, ref ObjectRef) error {
	key, err := objectKey(ref)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func bucketKey(ref BucketRef) (string, error) {
	if ref.BucketID == "" {
		return "", ErrBucketIDMissing
	}
	return "id:" + ref.BucketID, nil
}

func objectKey(ref ObjectRef) (string, error) {
	if ref.ObjectKey == "" {
		return "", ErrObjectKeyMissing
	}
	bucket, err := bucketKey(ref.Bucket)
	if err != nil {
		return "", err
	}
	// The key must incorporate both object_key and object_instance_id so distinct
	// versions of the same object key do not collide. An empty instance ID is
	// valid (e.g., non-versioned buckets) and is preserved as-is.
	return fmt.Sprintf("%s|object:%s|instance:%s", bucket, ref.ObjectKey, ref.ObjectInstanceID), nil
}

func cloneMap(src map[string][]byte) map[string][]byte {
	if len(src) == 0 {
		return map[string][]byte{}
	}
	dst := make(map[string][]byte, len(src))
	for k, v := range src {
		dst[k] = cloneBytes(v)
	}
	return dst
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return []byte{}
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
