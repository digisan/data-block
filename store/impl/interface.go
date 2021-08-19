package impl

type Ikv interface {
	Len() int
	Set(key, value interface{}) bool
	Get(key interface{}) (interface{}, bool)
	Remove(key interface{})
	Clear()
	OnConflict(f func(existing, coming interface{}) (bool, interface{})) func(existing, coming interface{}) (bool, interface{})
	IsPersistent() bool
}

var prt = false

func SetPrint(print bool) {
	prt = print
}
