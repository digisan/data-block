package impl

import "sync"

type SM sync.Map

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
		return true
	}
	return false
}

func (sm *SM) Get(key interface{}) (interface{}, bool) {
	if sm != nil {
		if value, ok := ((*sync.Map)(sm)).Load(key); ok {
			return value, ok
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
