package db

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/digisan/data-block/store/impl"
	"github.com/digisan/gotk/filedir"
	goi "github.com/digisan/gotk/io"
	"github.com/pkg/errors"
)

func NewBadgerDB(folderPath string) (*badger.DB, error) {
	log.Println("opening BadgerDB database...")

	if err := os.MkdirAll(folderPath, os.ModePerm); err != nil {
		return nil, err
	}
	options := badger.DefaultOptions(folderPath)
	// options = options.WithSyncWrites(false) // speed optimization if required
	// options = options.WithNumVersionsToKeep(3)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	// log.Println("--- db batch count = ", db.MaxBatchCount(), " ---")

	return db, err
}

func fetch(raw []byte) (result interface{}, err error) {
	resultStr := string(raw[1:])
	switch raw[0] {
	case 's':
		result = resultStr
	case 'b':
		result, err = strconv.ParseBool(resultStr)
	case 'i':
		result, err = strconv.ParseInt(resultStr, 10, 64)
	case 'f':
		result, err = strconv.ParseFloat(resultStr, 64)
	case 'c':
		result, err = strconv.ParseComplex(resultStr, 128)
	case 'n':
		result = nil
	case 'e':
		result = struct{}{}
	default:
		panic("Invalid Type @ Badger Storage")
	}
	return
}

func RemoveToBadger(kv impl.Ikv, db *badger.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil, removed nothing")
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	switch m := kv.(type) {
	case *impl.M:
		for key := range *m {
			kp, err := impl.DBPrefix(key)
			if err != nil {
				panic(errors.Wrap(err, "key type is not supported @ M RemoveToBadger"))
			}
			kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
			wb.Delete(kBuf)
		}
	case *impl.SM:
		((*sync.Map)(m)).Range(func(key, value interface{}) bool {
			kp, err := impl.DBPrefix(key)
			if err != nil {
				panic(errors.Wrap(err, "key type is not supported @ SM RemoveToBadger"))
			}
			kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
			wb.Delete(kBuf)
			return true
		})
	default:
		panic("kv must be map or sync.Map")
	}

	return nil
}

func BadgerSearch(db *badger.DB, vFilter func(k, v interface{}) bool) (map[interface{}]interface{}, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil, found nothing")
	}

	m := impl.NewM()
	err := SyncFromBadger(m, db, vFilter)
	return (map[interface{}]interface{})(*m), err
}

// vFilter args number must be converted to [int64], [float64]
func SyncFromBadger(kv impl.Ikv, db *badger.DB, vFilter func(k, v interface{}) bool) error {
	if db == nil {
		return fmt.Errorf("db is nil, sync nothing")
	}

	kv.Clear()
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(v []byte) error {

				// fmt.Printf(" ------------ key=%s, value=%s\n", k, v)

				realKey, err := fetch(k)
				if err != nil {
					return errors.Wrap(err, "Key")
				}
				realVal, err := fetch(v)
				if err != nil {
					return errors.Wrap(err, "Value")
				}

				if vFilter != nil && !vFilter(realKey, realVal) {
					return nil
				}

				kv.Set(realKey, realVal)
				return nil

			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func BadgerSearchByKey(db *badger.DB, key interface{}, vFilter func(k, v interface{}) bool) (map[interface{}]interface{}, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil, found nothing")
	}

	m := impl.NewM()
	err := SyncFromBadgerByKey(m, db, key, vFilter)
	return (map[interface{}]interface{})(*m), err
}

// vFilter args number must be converted to [int64], [float64]
func SyncFromBadgerByKey(kv impl.Ikv, db *badger.DB, key interface{}, vFilter func(k, v interface{}) bool) error {
	if db == nil {
		return fmt.Errorf("db is nil, sync nothing")
	}

	kv.Clear()
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		var tKey byte
		switch key.(type) {
		case string:
			tKey = 's'
		case bool:
			tKey = 'b'
		case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint, uintptr:
			tKey = 'i'
		case float32, float64:
			tKey = 'f'
		case complex64, complex128:
			tKey = 'c'
		case nil:
			tKey = 'n'
		case struct{}:
			tKey = 'e'
		default:
			panic("key type is not supported @ SyncFromBadgerByKey")
		}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if k := item.Key(); k[0] == tKey && string(k[1:]) == fmt.Sprint(key) { // skip first byte for type-indicator
				if err := item.Value(func(v []byte) error {

					// fmt.Printf(" ------------ key=%s, value=%s\n", k, v)

					realKey, err := fetch(k)
					if err != nil {
						return errors.Wrap(err, "Key")
					}
					realVal, err := fetch(v)
					if err != nil {
						return errors.Wrap(err, "Value")
					}

					if vFilter != nil && !vFilter(realKey, realVal) {
						return nil
					}

					kv.Set(realKey, realVal)
					return nil

				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func BadgerSearchByPrefix(db *badger.DB, prefix string, vFilter func(k, v interface{}) bool) (map[interface{}]interface{}, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil, found nothing")
	}

	m := impl.NewM()
	err := SyncFromBadgerByPrefix(m, db, prefix, vFilter)
	return (map[interface{}]interface{})(*m), err
}

// only string key available for prefix search
// vFilter args number must be converted to [int64], [float64]
func SyncFromBadgerByPrefix(kv impl.Ikv, db *badger.DB, prefix string, vFilter func(k, v interface{}) bool) error {
	if db == nil {
		return fmt.Errorf("db is nil, sync nothing")
	}

	kv.Clear()
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefixBuf := []byte("s" + prefix) // only string key available for prefix search
		for it.Seek(prefixBuf); it.ValidForPrefix(prefixBuf); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(v []byte) error {

				// fmt.Printf("key=%s, value=%s\n", k, v)
				realKey, err := fetch(k)
				if err != nil {
					return errors.Wrap(err, "Key")
				}
				realVal, err := fetch(v)
				if err != nil {
					return errors.Wrap(err, "Value")
				}

				if vFilter != nil && !vFilter(realKey, realVal) {
					return nil
				}

				kv.Set(realKey, realVal)
				return nil

			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func BadgerDump(db *badger.DB, w io.Writer) error {
	if db == nil {
		return fmt.Errorf("db is nil, dumped nothing")
	}
	m, err := BadgerSearch(db, nil)
	if err != nil {
		return errors.Wrap(err, "@BadgerDump")
	}
	for k, v := range m {
		fmt.Fprintf(w, "%v --- %v\n", k, v)
	}
	return nil
}

func BadgerDumpFile(db *badger.DB, file string) error {
	file, _ = filedir.AbsPath(file, false)
	goi.MustCreateDir(filepath.Dir(file))
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "@BadgerDumpFile")
	}
	defer f.Close()
	return BadgerDump(db, f)
}
