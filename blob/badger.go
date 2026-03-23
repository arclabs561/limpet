package blob

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// BadgerStore wraps a badger.DB as a KVStore.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore opens a badger database at the given directory.
func NewBadgerStore(opts badger.Options) (*BadgerStore, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &NotFoundError{Key: string(key)}
			}
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

func (s *BadgerStore) Set(key, val []byte, ttl time.Duration) error {
	return s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, val).WithDiscard()
		if ttl > 0 {
			entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

func (s *BadgerStore) Delete(key []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (s *BadgerStore) List(prefix []byte) ([]KVEntry, error) {
	var entries []KVEntry
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if len(prefix) > 0 && !hasPrefix(k, prefix) {
				break
			}
			entries = append(entries, KVEntry{
				Key:       item.KeyCopy(nil),
				Size:      item.ValueSize(),
				ExpiresAt: item.ExpiresAt(),
			})
		}
		return nil
	})
	return entries, err
}

func hasPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := range prefix {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

// RunGC triggers badger value log garbage collection.
func (s *BadgerStore) RunGC(discardRatio float64) error {
	return s.db.RunValueLogGC(discardRatio)
}
