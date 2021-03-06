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
		(*m)[key] = value
		if prt {
			fmt.Printf("\n+++ [%v]: [%v] is set in M\n", key, value)
		}
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
	if value, ok := m.Get(key); ok {
		delete(*m, key)
		if prt {
			fmt.Printf("\n--- [%v]: [%v] is removed from M\n", key, value)
		}
	}
}

func (m *M) Range(f func(key, value interface{}) bool) {
	for k, v := range *m {
		if !f(k, v) {
			break
		}
	}
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

func (m *M) SyncToBadgerWriteBatch(wb *badger.WriteBatch) (err error) {
	if wb == nil {
		return fmt.Errorf("writebatch is nil, flushed nothing")
	}

	for key, value := range *m {
		kp, e := DBPrefix(key)
		if e != nil {
			panic(errors.Wrap(e, "key type is not supported @ M FlushToBadger"))
		}
		vp, e := DBPrefix(value)
		if e != nil {
			panic(errors.Wrap(e, "value type is not supported @ M FlushToBadger"))
		}
		kBuf := append([]byte{kp}, []byte(fmt.Sprint(key))...)
		vBuf := append([]byte{vp}, []byte(fmt.Sprint(value))...)
		if err = wb.Set(kBuf, vBuf); err != nil {
			break
		}
	}

	return err
}

func (m *M) FlushToBadger(db *badger.DB) (err error) {
	if db == nil {
		return fmt.Errorf("db is nil, flushed nothing")
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	return m.SyncToBadgerWriteBatch(wb)
}
