package impl

type Ikv interface {
	Len() int
	Set(key, value interface{}) bool
	Get(key interface{}) (interface{}, bool)
	Clear()
	OnConflict(f func(existing, coming interface{}) (bool, interface{})) func(existing, coming interface{}) (bool, interface{})
	IsPersistent() bool
}
