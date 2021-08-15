package impl

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
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

		tKey := make([]byte, 1)
		switch key.(type) {
		case string:
			tKey[0] = 's'
		case bool:
			tKey[0] = 'b'
		case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint, uintptr:
			tKey[0] = 'i'
		case float32, float64:
			tKey[0] = 'f'
		case complex64, complex128:
			tKey[0] = 'c'
		case nil:
			tKey[0] = 'n'
		default:
			panic("key type is not supported @ SM FlushToBadger")
		}

		tVal := make([]byte, 1)
		switch value.(type) {
		case string:
			tVal[0] = 's'
		case bool:
			tVal[0] = 'b'
		case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint, uintptr:
			tVal[0] = 'i'
		case float32, float64:
			tVal[0] = 'f'
		case complex64, complex128:
			tVal[0] = 'c'
		case nil:
			tVal[0] = 'n'
		default:
			panic("value type is not supported @ SM FlushToBadger")
		}

		kBuf := append(tKey, []byte(fmt.Sprint(key))...)
		vBuf := append(tVal, []byte(fmt.Sprint(value))...)
		wb.Set(kBuf, vBuf)
		return true
	})
}
