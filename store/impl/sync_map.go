package impl

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

type SM sync.Map

func NewSM() *SM {
	return &SM{}
}

func (sm *SM) Len() int {
	cnt := 0
	((*sync.Map)(sm)).Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func (sm *SM) Set(key, value interface{}) bool {
	if sm != nil {
		((*sync.Map)(sm)).Store(key, value)
		if prt {
			fmt.Printf("\n+++ [%v]: [%v] is set in SM\n", key, value)
		}
		return true
	}
	return false
}

func (sm *SM) firstValue(keys ...interface{}) (interface{}, bool) {
	for _, k := range keys {
		if value, ok := ((*sync.Map)(sm)).Load(k); ok {
			return value, ok
		}
	}
	return nil, false
}

func (sm *SM) Get(key interface{}) (interface{}, bool) {
	if sm != nil {
		switch k := key.(type) {
		case int8:
			return sm.firstValue(k, int64(k))
		case uint8:
			return sm.firstValue(k, int64(k))
		case int16:
			return sm.firstValue(k, int64(k))
		case uint16:
			return sm.firstValue(k, int64(k))
		case int32:
			return sm.firstValue(k, int64(k))
		case uint32:
			return sm.firstValue(k, int64(k))
		case int64:
			return sm.firstValue(k, int64(k))
		case uint64:
			return sm.firstValue(k, int64(k))
		case int:
			return sm.firstValue(k, int64(k))
		case uint:
			return sm.firstValue(k, int64(k))
		case uintptr:
			return sm.firstValue(k, int64(k))
		case float32:
			return sm.firstValue(k, float64(k))
		case float64:
			return sm.firstValue(k, float64(k))
		case complex64:
			return sm.firstValue(k, complex128(k))
		case complex128:
			return sm.firstValue(k, complex128(k))
		default:
			return sm.firstValue(k)
		}
	}
	return nil, false
}

func (sm *SM) Remove(key interface{}) {
	if value, ok := sm.Get(key); ok {
		((*sync.Map)(sm)).Delete(key)
		if prt {
			fmt.Printf("\n--- [%v]: [%v] is removed from SM\n", key, value)
		}
	}
}

func (sm *SM) Clear() {
	keys := []interface{}{}
	((*sync.Map)(sm)).Range(func(key, value interface{}) bool {
		keys = append(keys, key)
		return true
	})
	for _, k := range keys {
		((*sync.Map)(sm)).Delete(k)
	}
}

func (sm *SM) OnConflict(f func(existing, coming interface{}) (bool, interface{})) func(existing, coming interface{}) (bool, interface{}) {
	if f != nil {
		return f
	}
	return func(existing, coming interface{}) (bool, interface{}) {
		return true, coming
	}
}

func (sm *SM) IsPersistent() bool {
	return false
}

///////////////////////////////////////////////////////////////////

func (sm *SM) SyncToBadgerWriteBatch(wb *badger.WriteBatch) (err error) {
	if wb == nil {
		return fmt.Errorf("writebatch is nil, flushed nothing")
	}

	((*sync.Map)(sm)).Range(func(key, value interface{}) bool {
		kp, e := DBPrefix(key)
		if e != nil {
			panic(errors.Wrap(e, "key type is not supported @ SM FlushToBadger"))
		}
		vp, e := DBPrefix(value)
		if e != nil {
			panic(errors.Wrap(e, "value type is not supported @ SM FlushToBadger"))
		}
		kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
		vBuf := append([]byte{vp}, []byte(fmt.Sprint(value))...)
		if err = wb.Set(kBuf, vBuf); err != nil {
			return false
		}
		return true
	})

	return err
}

func (sm *SM) FlushToBadger(db *badger.DB) (err error) {
	if db == nil {
		return fmt.Errorf("db is nil, flushed nothing")
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	return sm.SyncToBadgerWriteBatch(wb)
}
