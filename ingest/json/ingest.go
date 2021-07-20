package json

import (
	"io"

	"github.com/digisan/data-block/store"
	jt "github.com/digisan/json-tool"
	lk "github.com/digisan/logkit"
)

func fac4solver() func(existing, coming interface{}) (bool, interface{}) {
	return func(existing, coming interface{}) (bool, interface{}) {
		return false, ""
	}
}

var (
	warn = lk.Fac4GrpIdxLogF("INGEST", 0, lk.WARN, false)
	kv   = store.NewKV("dump/S", "json", true, true)
	spo  = store.NewSPO("dump")
)

func init() {

}

func FlattenStore(r io.Reader) {
	results, _ := jt.ScanObject(r, false, true, jt.OUT_MIN)
	for rst := range results {
		if rst.Err != nil {
			warn("%v", rst.Err)
			continue
		}
		m, err := jt.FlattenObject(rst.Obj)
		if err != nil {
			warn("%v", err)
		}
		id := m["glossary.id"].(string) // gjson.Get(json, "glossary.id").String()
		for k, v := range m {
			spo.Save(id, k, v)
		}
		kv.Save(id, rst.Obj)
	}
}
