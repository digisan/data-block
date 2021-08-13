package impl

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
		return true
	}
	return false
}

func (m *M) Get(key interface{}) (interface{}, bool) {
	if m != nil {
		if value, ok := (*m)[key]; ok {
			return value, ok
		}
	}
	return nil, false
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
