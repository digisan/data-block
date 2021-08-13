package store

import (
	"context"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kv := NewKV(true, true)
	kv.AppendFS("./test_out", ".txt", true)
	kv.OnConflict(func(existing, coming interface{}) (bool, interface{}) {
		// return true, "overwrite"
		return true, fmt.Sprintf("%v\n%v", existing, coming)
	})

	kv.Save("1", "test111")
	kv.Save("1", "test222")
	kv.Save(2, 123)
	kv.SaveWithIDKey(1234)
	kv.SaveWithTSKey(2234)

	go func() {
		for cnt := range kv.ChangedNotifier() {
			fmt.Println(cnt)
		}
	}()

	go func() {
		for cnt := range kv.UnchangedTickerNotifier(ctx, 500, false) {
			fmt.Println(" --- ", cnt)
		}
	}()

	go func() {
		time.Sleep(8 * time.Second)
		cancel()
	}()

	time.Sleep(4 * time.Second)
	kv.Save(5, 555)
	kv.Save(6, 666)

	time.Sleep(10 * time.Second)

	fmt.Println(kv.KVs[IdxM].Get("1"))
	fmt.Println(kv.KVs[IdxSM].Get(2))
	fmt.Println(kv.KVs[IdxFS].Get(1))
}

// func TestKVStorage_FileSyncToMap(t *testing.T) {
// 	kv := NewKV("../in", "json", true, true)
// 	kv.FileSyncToMap()
// 	fmt.Println(kv.M["5"])
// 	fmt.Println(kv.SM.Load("5"))
// }

// func TestKVStorage_AppendJSONFromFile(t *testing.T) {
// 	kv := NewKV("../in1", "json", true, true)
// 	kv.AppendJSONFromFile("../in")
// }

func TestClear(t *testing.T) {
	m := impl.NewM()
	m.Set(1, 2)
}
