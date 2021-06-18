package json

import (
	"fmt"
	"io"

	lckv "github.com/digisan/data-block/local-kv"
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
	opt  = lckv.NewOption("../tuples", "", fac4solver, true, true)
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
		for k, v := range m {
			opt.Save(k, fmt.Sprint(v), true)
		}
	}
}
