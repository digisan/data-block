package store

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/digisan/data-block/store/impl"
	"github.com/digisan/gotk/slice/ti"
	"github.com/google/uuid"
)

var (
	IdxM  = -1
	IdxSM = -1
	IdxFS = -1
)

type KVStorage struct {
	length     int                                                    // storage count
	cChanged   chan int                                               // if length changed, notify updated length
	cUnchanged chan int                                               // if length has not changed for a while, notify length
	onConflict func(existing, coming interface{}) (bool, interface{}) // conflict solver for file
	KVs        []impl.Ikv
}

func NewKV(wantM, wantSM bool) *KVStorage {

	const N = 10000

	kv := &KVStorage{
		length:     0,
		cChanged:   make(chan int, N),
		cUnchanged: make(chan int, N),
	}

	if wantM {
		kv.KVs = append(kv.KVs, &impl.M{})
		IdxM = len(kv.KVs) - 1
	}
	if wantSM {
		kv.KVs = append(kv.KVs, &impl.SM{})
		IdxSM = len(kv.KVs) - 1
	}

	return kv
}

func (kv *KVStorage) AppendFS(dir, ext string, repeatIdx bool) {
	kv.KVs = append(kv.KVs, impl.NewFS(dir, ext, repeatIdx))
	IdxFS = len(kv.KVs) - 1
}

func (kv *KVStorage) OnConflict(f func(existing, coming interface{}) (bool, interface{})) {
	kv.onConflict = f
	for _, s := range kv.KVs {
		s.OnConflict(f)
	}
}

func (kv *KVStorage) batchSave(key, value interface{}, repeatIdx bool) bool {

	// no key, no saving
	if key == nil && key == "" {
		return false
	}

	var (
		added = false
		done  = make(chan bool)
	)

	for _, s := range kv.KVs {
		if v, ok := s.Get(key); ok { // conflicts
			if save, content := s.OnConflict(kv.onConflict)(v, value); save {
				if s.Set(key, content) && !added {
					kv.length++
					go func() { kv.cChanged <- kv.length; done <- true }()
					added = <-done
				}
			}
		} else { // no conflicts
			if s.Set(key, value) && !added {
				kv.length++
				go func() { kv.cChanged <- kv.length; done <- true }()
				added = <-done
			}
		}
	}

	return added
}

func (kv *KVStorage) Length() int {
	return kv.length
}

func (kv *KVStorage) ChangedNotifier() <-chan int {
	return kv.cChanged
}

// duration : Millisecond
func (kv *KVStorage) UnchangedOnceNotifier(duration int, excl ...int) <-chan int {
	go func() {
		cntPrev := kv.Length()
		d := time.Duration(duration * int(time.Millisecond))
		timer := time.NewTimer(d)
		<-timer.C
		timer.Stop()
		if kv.Length()-cntPrev == 0 {
			if ti.NotIn(kv.Length(), excl...) {
				kv.cUnchanged <- kv.Length()
			}
		}
	}()
	return kv.cUnchanged
}

func (kv *KVStorage) UnchangedTickerNotifier(ctx context.Context, duration int, onceOnSame bool, excl ...int) <-chan int {
	go func() {
		mLenTick := make(map[int]int)
		d := time.Duration(duration * int(time.Millisecond))
		ticker := time.NewTicker(d)
	T:
		for {
			cntPrev := kv.Length()
			select {
			case <-ctx.Done():
				break T
			case <-ticker.C:
				if L := kv.Length(); L-cntPrev == 0 {
					if onceOnSame {
						if _, ok := mLenTick[L]; ok {
							continue T
						}
					}
					if ti.NotIn(L, excl...) {
						kv.cUnchanged <- L
						mLenTick[L]++
					}
				}
			}
		}
		ticker.Stop()
	}()
	return kv.cUnchanged
}

///////////////////////////////////////////////////////

func (kv *KVStorage) Save(key, value interface{}) {
	kv.batchSave(key, value, true)
}

func (kv *KVStorage) Fac4SaveWithIdxKey(start int) func(value interface{}) {
	idx := int64(start - 1)
	return func(value interface{}) {
		kv.batchSave(fmt.Sprintf("%04d", atomic.AddInt64(&idx, 1)), value, false)
	}
}

func (kv *KVStorage) SaveWithTSKey(value interface{}) {
	kv.batchSave(time.Now().Format("2006-01-02 15:04:05.000000"), value, false)
}

func (kv *KVStorage) SaveWithIDKey(value interface{}) {
	kv.batchSave(strings.ReplaceAll(uuid.New().String(), "-", ""), value, false)
}

///////////////////////////////////////////////////////

func (kv *KVStorage) Remove(key interface{}) {
	for _, s := range kv.KVs {
		s.Remove(key)
	}
}

func (kv *KVStorage) Clear(rmPersistent bool) {
	if rmPersistent {
		for _, s := range kv.KVs {
			s.Clear()
		}
	} else {
		for _, s := range kv.KVs {
			if !s.IsPersistent() {
				s.Clear()
			}
		}
	}
}

///////////////////////////////////////////////////////

// func (kv *KVStorage) FileSyncToMap() int {
// 	files, _, err := fd.WalkFileDir(kv.dir, true)
// 	if err != nil {
// 		return 0
// 	}
// 	return len(ts.FM(
// 		files,
// 		func(i int, e string) bool { return strings.HasSuffix(e, kv.ext) },
// 		func(i int, e string) string {
// 			fname := filepath.Base(e)
// 			key := fname[:strings.IndexAny(fname, "(.")]
// 			if bytes, err := os.ReadFile(e); err == nil {
// 				value := string(bytes)
// 				for _, s := range kv.KVs {
// 					s.Set(key, value)
// 				}
// 			} else {
// 				log.Fatalln(err)
// 			}
// 			return key
// 		},
// 	))
// }

// func (kv *KVStorage) AppendJSONFromFile(dir string) int {
// 	files, _, err := fd.WalkFileDir(dir, true)
// 	if err != nil {
// 		return 0
// 	}
// 	return len(ts.FM(
// 		files,
// 		func(i int, e string) bool { return strings.HasSuffix(e, fd.DotExt("json")) },
// 		func(i int, e string) string {
// 			fname := filepath.Base(e)
// 			key := fname[:strings.IndexAny(fname, "(.")]
// 			file, err := os.OpenFile(e, os.O_RDONLY, os.ModePerm)
// 			if err != nil {
// 				log.Fatalln(err)
// 			}
// 			defer file.Close()

// 			ResultOfScan, _ := jt.ScanObject(file, false, true, jt.OUT_MIN)
// 			for rst := range ResultOfScan {
// 				if rst.Err == nil {
// 					kv.Save(key, rst.Obj)
// 				}
// 			}

// 			return key
// 		},
// 	))
// }
