package impl

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

type M map[interface{}]interface{}

func NewM() *M {
	m := make(M)
	return &m
}

func (m *M) Len() int {
	return len(*(*map[interface{}]interface{})(m))
}

func (m *M) Set(key, value interface{}) bool {
	if m != nil {
		// switch k := key.(type) {
		// case []byte:
		// 	switch v := value.(type) {
		// 	case []byte:
		// 		(*m)[string(k)] = string(v)
		// 	default:
		// 		(*m)[string(k)] = value
		// 	}
		// default:
		// 	(*m)[key] = value
		// }
		(*m)[key] = value
		return true
	}
	return false
}

func (m *M) firstValue(keys ...interface{}) (interface{}, bool) {
	for _, k := range keys {
		if value, ok := (*m)[k]; ok {
			return value, ok
		}
	}
	return nil, false
}

func (m *M) Get(key interface{}) (interface{}, bool) {
	if m != nil {
		switch k := key.(type) {
		case int8:
			return m.firstValue(k, int64(k))
		case uint8:
			return m.firstValue(k, int64(k))
		case int16:
			return m.firstValue(k, int64(k))
		case uint16:
			return m.firstValue(k, int64(k))
		case int32:
			return m.firstValue(k, int64(k))
		case uint32:
			return m.firstValue(k, int64(k))
		case int64:
			return m.firstValue(k, int64(k))
		case uint64:
			return m.firstValue(k, int64(k))
		case int:
			return m.firstValue(k, int64(k))
		case uint:
			return m.firstValue(k, int64(k))
		case uintptr:
			return m.firstValue(k, int64(k))
		case float32:
			return m.firstValue(k, float64(k))
		case float64:
			return m.firstValue(k, float64(k))
		case complex64:
			return m.firstValue(k, complex128(k))
		case complex128:
			return m.firstValue(k, complex128(k))
		default:
			return m.firstValue(k)
		}
	}
	return nil, false
}

func (m *M) Remove(key interface{}) {
	delete(*m, key)
}

func (m *M) Clear() {
	keys := []interface{}{}
	for k := range *m {
		keys = append(keys, k)
	}
	for _, k := range keys {
		delete(*m, k)
	}
}

func (m *M) OnConflict(f func(existing, coming interface{}) (bool, interface{})) func(existing, coming interface{}) (bool, interface{}) {
	if f != nil {
		return f
	}
	return func(existing, coming interface{}) (bool, interface{}) {
		return true, coming
	}
}

func (m *M) IsPersistent() bool {
	return false
}

///////////////////////////////////////////////////////////////////

// for badger db storage for differentiate data type
func DBPrefix(input interface{}) (prefix byte, err error) {
	switch i := input.(type) {
	case string:
		prefix = 's'
	case bool:
		prefix = 'b'
	case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint, uintptr:
		prefix = 'i'
	case float32, float64:
		prefix = 'f'
	case complex64, complex128:
		prefix = 'c'
	case nil:
		prefix = 'n'
	case struct{}:
		prefix = 'e'
	default:
		err = fmt.Errorf("%v is not supported for prefix", i)
	}
	return
}

func (m *M) FlushToBadger(db *badger.DB) {
	wb := db.NewWriteBatch()
	defer wb.Flush()

	for key, value := range *m {
		kp, err := DBPrefix(key)
		if err != nil {
			panic(errors.Wrap(err, "key type is not supported @ M FlushToBadger"))
		}
		vp, err := DBPrefix(value)
		if err != nil {
			panic(errors.Wrap(err, "value type is not supported @ M FlushToBadger"))
		}
		kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
		vBuf := append([]byte{vp}, []byte(fmt.Sprint(value))...)
		wb.Set(kBuf, vBuf)
	}
}
