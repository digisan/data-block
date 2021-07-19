package impl

type Ikv interface {
	Len() int
	Set(key, value interface{}) bool
	Get(key interface{}) (interface{}, bool)
	Clear()
	OnConflict(existing, coming interface{}) (bool, interface{})
}
