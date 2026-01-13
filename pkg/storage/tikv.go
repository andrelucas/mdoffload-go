package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/txnkv"
)

const defaultPDAddress = "http://127.0.0.1:2379"

// TiKVStore persists attributes in TiKV using the transactional API.
type TiKVStore struct {
	client *txnkv.Client
}

// NewTiKVStore constructs a TiKV-backed Store. If no PD addresses are provided,
// it defaults to the local PD at http://127.0.0.1:2379 (no TLS).
func NewTiKVStore(pdAddrs ...string) (*TiKVStore, error) {
	if len(pdAddrs) == 0 {
		pdAddrs = []string{defaultPDAddress}
	}

	cli, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		return nil, fmt.Errorf("create TiKV client: %w", err)
	}

	return &TiKVStore{client: cli}, nil
}

// Close releases the underlying TiKV client resources.
func (s *TiKVStore) Close() error {
	return s.client.Close()
}

func (s *TiKVStore) GetBucketAttributes(ctx context.Context, ref BucketRef) (map[string][]byte, error) {
	prefix, err := bucketAttrPrefix(ref)
	if err != nil {
		return nil, err
	}

	txn, err := s.client.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = txn.Rollback() }()

	iter, err := txn.Iter(prefix, prefixEnd(prefix))
	if err != nil {
		return nil, err
	}

	attrs := map[string][]byte{}
	for iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
		name := string(iter.Key()[len(prefix):])
		attrs[name] = cloneBytes(iter.Value())
		iter.Next()
	}

	return attrs, nil
}

func (s *TiKVStore) SetBucketAttributes(ctx context.Context, ref BucketRef, toAdd map[string][]byte, toDelete []string, replace bool) error {
	prefix, err := bucketAttrPrefix(ref)
	if err != nil {
		return err
	}

	txn, err := s.client.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	if replace {
		if err := deletePrefix(txn, prefix); err != nil {
			return err
		}
	} else {
		for _, k := range toDelete {
			if err := txn.Delete(composeKey(prefix, k)); err != nil {
				return err
			}
		}
	}

	for k, v := range toAdd {
		if err := txn.Set(composeKey(prefix, k), cloneBytes(v)); err != nil {
			return err
		}
	}

	return txn.Commit(ctx)
}

func (s *TiKVStore) PurgeBucketAttributes(ctx context.Context, ref BucketRef) error {
	prefix, err := bucketAttrPrefix(ref)
	if err != nil {
		return err
	}

	txn, err := s.client.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	if err := deletePrefix(txn, prefix); err != nil {
		return err
	}

	return txn.Commit(ctx)
}

func (s *TiKVStore) GetObjectAttributes(ctx context.Context, ref ObjectRef) (map[string][]byte, error) {
	prefix, err := objectAttrPrefix(ref)
	if err != nil {
		return nil, err
	}

	txn, err := s.client.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = txn.Rollback() }()

	iter, err := txn.Iter(prefix, prefixEnd(prefix))
	if err != nil {
		return nil, err
	}

	attrs := map[string][]byte{}
	for iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
		name := string(iter.Key()[len(prefix):])
		attrs[name] = cloneBytes(iter.Value())
		iter.Next()
	}

	return attrs, nil
}

func (s *TiKVStore) SetObjectAttributes(ctx context.Context, ref ObjectRef, toAdd map[string][]byte, toDelete []string, newInstance bool) error {
	prefix, err := objectAttrPrefix(ref)
	if err != nil {
		return err
	}

	txn, err := s.client.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	if newInstance {
		if err := deletePrefix(txn, prefix); err != nil {
			return err
		}
	} else {
		for _, k := range toDelete {
			if err := txn.Delete(composeKey(prefix, k)); err != nil {
				return err
			}
		}
	}

	for k, v := range toAdd {
		if err := txn.Set(composeKey(prefix, k), cloneBytes(v)); err != nil {
			return err
		}
	}

	return txn.Commit(ctx)
}

func (s *TiKVStore) PurgeObjectAttributes(ctx context.Context, ref ObjectRef) error {
	prefix, err := objectAttrPrefix(ref)
	if err != nil {
		return err
	}

	txn, err := s.client.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	if err := deletePrefix(txn, prefix); err != nil {
		return err
	}

	return txn.Commit(ctx)
}

func bucketAttrPrefix(ref BucketRef) ([]byte, error) {
	key, err := bucketKey(ref)
	if err != nil {
		return nil, err
	}
	return []byte("bucket|" + key + "|attr|"), nil
}

func objectAttrPrefix(ref ObjectRef) ([]byte, error) {
	key, err := objectKey(ref)
	if err != nil {
		return nil, err
	}
	return []byte("object|" + key + "|attr|"), nil
}

func composeKey(prefix []byte, name string) []byte {
	k := make([]byte, len(prefix)+len(name))
	copy(k, prefix)
	copy(k[len(prefix):], name)
	return k
}

func prefixEnd(prefix []byte) []byte {
	return append(append([]byte{}, prefix...), 0xFF)
}

func deletePrefix(txn *txnkv.KVTxn, prefix []byte) error {
	iter, err := txn.Iter(prefix, prefixEnd(prefix))
	if err != nil {
		return err
	}

	for iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
		if err := txn.Delete(iter.Key()); err != nil {
			return err
		}
		iter.Next()
	}

	return nil
}
