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
		// switch k := key.(type) {
		// case []byte:
		// 	switch v := value.(type) {
		// 	case []byte:
		// 		((*sync.Map)(sm)).Store(string(k), string(v))
		// 	default:
		// 		((*sync.Map)(sm)).Store(string(k), value)
		// 	}
		// default:
		// 	((*sync.Map)(sm)).Store(key, value)
		// }
		((*sync.Map)(sm)).Store(key, value)
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
	((*sync.Map)(sm)).Delete(key)
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

func (sm *SM) FlushToBadger(db *badger.DB) {
	wb := db.NewWriteBatch()
	defer wb.Flush()

	((*sync.Map)(sm)).Range(func(key, value interface{}) bool {
		kp, err := DBPrefix(key)
		if err != nil {
			panic(errors.Wrap(err, "key type is not supported @ SM FlushToBadger"))
		}
		vp, err := DBPrefix(value)
		if err != nil {
			panic(errors.Wrap(err, "value type is not supported @ SM FlushToBadger"))
		}
		kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
		vBuf := append([]byte{vp}, []byte(fmt.Sprint(value))...)
		wb.Set(kBuf, vBuf)
		return true
	})
}
