package store

import (
	"fmt"
	"path/filepath"
)

type (
	SPOV struct {
		subject   interface{}
		predicate interface{}
		object    interface{}
		version   int64
		kv        *KVStorage
	}

	SP struct {
		subject   interface{}
		predicate interface{}
		version   int64
	}

	SO struct {
		subject interface{}
		object  interface{}
		version int64
	}

	PO struct {
		predicate interface{}
		object    interface{}
		version   int64
	}
)

var (
	spoDumpDir string
	onConflict = func() func(existing, coming interface{}) (bool, interface{}) {
		return func(existing, coming interface{}) (bool, interface{}) {
			return true, fmt.Sprintf("%v\n%v", existing, coming)
		}
	}
)

func NewSPO(dir string) *SPOV {
	spoDumpDir = dir
	return &SPOV{version: 0, kv: NewKV(spoDumpDir, "", false, false, onConflict)}
}

func (spov *SPOV) pso() int64 {
	spov.kv.dir = filepath.Join(spoDumpDir, "P")
	spov.kv.Save(
		spov.predicate,
		&SO{
			spov.subject,
			spov.object,
			spov.version,
		},
		false,
	)
	return 0
}

func (spov *SPOV) osp() int64 {
	spov.kv.dir = filepath.Join(spoDumpDir, "O")
	spov.kv.Save(
		spov.object,
		&SP{
			spov.subject,
			spov.predicate,
			spov.version,
		},
		true,
	)
	return 0
}

func (spov *SPOV) spo() int64 {
	spov.kv.dir = filepath.Join(spoDumpDir, "S")
	spov.kv.Save(
		spov.subject,
		&PO{
			spov.predicate,
			spov.object,
			spov.version,
		},
		true,
	)
	return 0
}

func (spov *SPOV) Save(sub, pred, obj interface{}) int64 {
	spov.subject = sub
	spov.predicate = pred
	spov.object = obj
	{
		spov.pso()
		spov.osp()
		// spov.spo()
	}
	return 0
}
