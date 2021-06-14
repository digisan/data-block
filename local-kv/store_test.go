package lckv

import (
	"fmt"
	"testing"
)

func fac4AppendJA() func(existing, coming interface{}) (bool, interface{}) {
	return func(existing, coming interface{}) (bool, interface{}) {
		switch existing := existing.(type) {
		case string:
			if len(existing) > 0 {
				switch existing[0] {
				case '{':
					return true, fmt.Sprintf("[%s,%s]", existing, coming)
				case '[':
					return true, fmt.Sprintf("%s,%s]", existing[:len(existing)-1], coming)
				default:
					panic("error in existing JSON storage")
				}
			}
			return true, coming
		default:
			return false, ""
		}
	}
}

func TestOption_FileSyncToMap(t *testing.T) {
	opt := NewOption("../in", "json", fac4AppendJA, true, true)
	opt.FileSyncToMap()
	fmt.Println(opt.M["5"])
	fmt.Println(opt.SM.Load("5"))
}

func TestOption_AppendJSONFromFile(t *testing.T) {
	opt := NewOption("../in1", "json", fac4AppendJA, true, true)
	opt.AppendJSONFromFile("../in")
}
