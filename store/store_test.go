package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/digisan/data-block/store/impl"
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

func TestSave(t *testing.T) {

	kv := NewKV("./test_out", "", true, true)
	kv.OnConflict = func(existing, coming interface{}) (bool, interface{}) {
		return true, fmt.Sprintf("%v\n%v", existing, coming)
	}
	kv.Save("1", "test111", true)
	kv.Save("1", "test222", true)
	kv.Save(2, 123, true)
	kv.SaveWithIDKey(1234)
	kv.SaveWithTSKey(2234)

	go func() {
		for cnt := range kv.ChangedNotifier() {
			fmt.Println(cnt)
		}
	}()

	done := make(chan struct{})
	go func() {
		for cnt := range kv.UnchangedTickerNotifier(800, false, done) {
			fmt.Println(" --- ", cnt)
		}
	}()

	go func() {
		time.Sleep(8 * time.Second)
		done <- struct{}{}
	}()

	time.Sleep(4 * time.Second)
	kv.Save(5, 555, true)
	kv.Save(6, 666, true)

	time.Sleep(10 * time.Second)

	fmt.Println(kv.KVs[0].Get("1"))
	fmt.Println(kv.KVs[1].Get(2))
}

// func TestKVStorage_FileSyncToMap(t *testing.T) {
// 	kv := NewKV("../in", "json", fac4AppendJA, true, true)
// 	kv.FileSyncToMap()
// 	fmt.Println(kv.M["5"])
// 	fmt.Println(kv.SM.Load("5"))
// }

// func TestKVStorage_AppendJSONFromFile(t *testing.T) {
// 	kv := NewKV("../in1", "json", fac4AppendJA, true, true)
// 	kv.AppendJSONFromFile("../in")
// }

func TestClear(t *testing.T) {
	m := impl.NewM()
	m.Set(1, 2)
}
