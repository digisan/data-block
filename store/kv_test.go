package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/digisan/data-block/store/db"
	"github.com/digisan/data-block/store/impl"
)

// func fac4AppendJA() func(existing, coming interface{}) (bool, interface{}) {
// 	return func(existing, coming interface{}) (bool, interface{}) {
// 		switch existing := existing.(type) {
// 		case string:
// 			if len(existing) > 0 {
// 				switch existing[0] {
// 				case '{':
// 					return true, fmt.Sprintf("[%s,%s]", existing, coming)
// 				case '[':
// 					return true, fmt.Sprintf("%s,%s]", existing[:len(existing)-1], coming)
// 				default:
// 					panic("error in existing JSON storage")
// 				}
// 			}
// 			return true, coming
// 		default:
// 			return false, ""
// 		}
// 	}
// }

func TestSave(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	badgerDB, err := db.NewBadgerDB("./data/badger")
	if err != nil {
		panic(err)
	}
	defer badgerDB.Close()

	kv := NewKV(true, true)
	kv.AppendFS("./data/test_out", ".txt", true)
	kv.OnConflict(func(existing, coming interface{}) (bool, interface{}) {
		return true, coming
		// return true, fmt.Sprintf("%v\n%v", existing, coming)
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
		for cnt := range kv.UnchangedTickerNotifier(ctx, 500, true) {
			fmt.Println(" --- ", cnt)

			kv.KVs[IdxM].(*impl.M).FlushToBadger(badgerDB)
			// kv.KVs[IdxSM].(*impl.SM).FlushToBadger(badgerDB)
			// kv.KVs[IdxFS].(*impl.FileStore).FlushToBadger(badgerDB, "txt")
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

	kv.Remove("1")

	fmt.Println(kv.KVs[IdxM].Get("1"))
	fmt.Println(kv.KVs[IdxSM].Get(2))
	fmt.Println(kv.KVs[IdxFS].Get(1))
}

func TestBadgerLoad(t *testing.T) {

	badgerDB, err := db.NewBadgerDB("./data/badger")
	if err != nil {
		panic(err)
	}

	// m := impl.NewM()
	// m := impl.NewSM()
	// m := impl.NewFS("./data/test_out_from_badger", "txt", false)

	// db.SyncFromBadger(m, badgerDB, func(v interface{}) bool {
	// 	return v == int64(123)
	// })
	// db.SyncFromBadgerByKey(m, badgerDB, 2, func(v interface{}) bool {
	// 	return v == int64(123)
	// })
	// db.SyncFromBadgerByPrefix(m, badgerDB, "2", nil)

	// fmt.Println("-----------------------")
	// fmt.Println("---", *m)
	// fmt.Println("-----------------------")
	// fmt.Println(m.Len())
	// fmt.Println("-----------------------")
	// fmt.Println(m.Get("1"))
	// fmt.Println(m.Get(2))
	// fmt.Println(m.Get(5))
	// fmt.Println(m.Get(6))

	m, err := db.BadgerSearch(badgerDB, nil)
	if err != nil {
		panic(err)
	}
	for k, v := range m {
		fmt.Printf("%#v, %#v\n", k, v)
	}

	// db.RemoveToBadger(m, badgerDB)
}
