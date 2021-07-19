package store

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/digisan/data-block/store/impl"
	fd "github.com/digisan/gotk/filedir"
	"github.com/digisan/gotk/io"
	"github.com/digisan/gotk/slice/ti"
	"github.com/digisan/gotk/slice/ts"
	jt "github.com/digisan/json-tool"
	"github.com/google/uuid"
)

type KVStorage struct {
	length     int                                                    // storage count
	cChanged   chan int                                               // if length changed, notify updated length
	cUnchanged chan int                                               // if length has not changed for a while, notify length
	dir, ext   string                                                 // file directory & file extension
	OnConflict func(existing, coming interface{}) (bool, interface{}) // conflict solver for file
	kvs        []impl.Ikv
}

func NewKV(dir, ext string, wantM, wantSM bool) *KVStorage {

	const N = 10000

	kv := &KVStorage{
		length:     0,
		cChanged:   make(chan int, N),
		cUnchanged: make(chan int, N),
	}

	if dir != "" {
		kv.dir = dir
		kv.ext = ext
	}

	if wantM {
		kv.kvs = append(kv.kvs, &impl.M{})
	}

	if wantSM {
		kv.kvs = append(kv.kvs, &impl.SM{})
	}

	return kv
}

func (kv *KVStorage) file(key interface{}, value string, repeatIdx bool) bool {
	if kv.dir != "" {
		absdir, _ := fd.AbsPath(kv.dir, false)
		fullpath := filepath.Join(absdir, fmt.Sprint(key)) // full abs file name path without extension
		ext := strings.TrimLeft(kv.ext, ".")               // extension without prefix '.'
		prevpath := ""

		if repeatIdx {
			// record duplicate key number in fullpath as .../key(number).ext
			if matches, err := filepath.Glob(fullpath + "(*)" + ext); err == nil {
				if len(matches) > 0 {
					prevpath = matches[0]
					fname := filepath.Base(prevpath)
					pO, pC := strings.Index(fname, "("), strings.Index(fname, ")")
					num, _ := strconv.Atoi(fname[pO+1 : pC])
					fullpath = filepath.Join(absdir, fmt.Sprintf("%s(%d)", fname[:pO], num+1))
				} else {
					fullpath = fmt.Sprintf("%s(1)", fullpath)
				}
			}
		}

		// add extension
		fullpath = strings.TrimRight(fullpath+"."+ext, ".") // if no ext, remove last '.'

		if prevpath == "" {
			prevpath = fullpath
		}
		io.MustWriteFile(prevpath, []byte(value))
		os.Rename(prevpath, fullpath)
		return true
	}
	return false
}

func (kv *KVStorage) fileFetch(key interface{}, repeatIdx bool) (string, bool) {
	if kv.dir != "" {
		absdir, _ := fd.AbsPath(kv.dir, false)
		fullpath := filepath.Join(absdir, fmt.Sprint(key))
		ext := strings.TrimLeft(kv.ext, ".")

		if repeatIdx {
			// search path with .../key(number).ext
			if matches, err := filepath.Glob(fullpath + "(*)" + ext); err == nil {
				if len(matches) > 0 {
					fullpath = matches[0]
				}
			}
		} else {
			// add extension
			fullpath = strings.TrimRight(fullpath+"."+ext, ".") // if no ext, remove last '.'
		}

		if fd.FileExists(fullpath) {
			if bytes, err := os.ReadFile(fullpath); err == nil {
				return string(bytes), true
			}
		}
	}
	return "", false
}

// ----------------------- //

func (kv *KVStorage) batchSave(key, value interface{}, repeatIdx bool) bool {

	// no key, no saving
	if key == nil && key == "" {
		return false
	}

	var (
		added = false
		done  = make(chan bool)
	)

	if solver := kv.OnConflict; solver != nil {
		if str, ok := kv.fileFetch(key, repeatIdx); ok { // conflicts
			if save, content := solver(str, value); save {
				switch cont := content.(type) {
				case string:
					if kv.file(key, cont, repeatIdx) && !added {
						kv.length++
						go func() { kv.cChanged <- kv.length; done <- true }()
						added = <-done
					}
				default:
					panic("solver error unimplemented")
				}
			}
			goto OTHERS
		}
	}
	if kv.file(key, fmt.Sprint(value), repeatIdx) && !added {
		kv.length++
		go func() { kv.cChanged <- kv.length; done <- true }()
		added = <-done
	}

	/////////////////////

OTHERS:
	for _, s := range kv.kvs {
		if v, ok := s.Get(key); ok { // conflicts
			if save, content := s.OnConflict(v, value); save {
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
func (kv *KVStorage) UnchangedNotifier(duration int, once bool, tickerstop chan struct{}, excl ...int) <-chan int {
	go func() {

		cntPrev := kv.Length()
		d := time.Duration(duration * int(time.Millisecond))

		if once {

			timer := time.NewTimer(d)
			<-timer.C
			timer.Stop()
			if kv.Length()-cntPrev == 0 {
				if ti.NotIn(kv.Length(), excl...) {
					kv.cUnchanged <- kv.Length()
				}
			}

		} else {

			ticker := time.NewTicker(d)
		T:
			for {
				select {
				case <-tickerstop:
					break T
				case <-ticker.C:
					if kv.Length()-cntPrev == 0 {
						if ti.NotIn(kv.Length(), excl...) {
							kv.cUnchanged <- kv.Length()
						}
					}
				}
			}
			ticker.Stop()
		}
	}()

	return kv.cUnchanged
}

///////////////////////////////////////////////////////

func (kv *KVStorage) Save(key, value interface{}, fileNameRepeatIdx bool) {
	kv.batchSave(key, value, fileNameRepeatIdx)
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

func withDot(str string) string {
	return "." + strings.TrimLeft(str, ".")
}

func (kv *KVStorage) FileSyncToMap() int {
	files, _, err := fd.WalkFileDir(kv.dir, true)
	if err != nil {
		return 0
	}
	return len(ts.FM(
		files,
		func(i int, e string) bool { return strings.HasSuffix(e, withDot(kv.ext)) },
		func(i int, e string) string {
			fname := filepath.Base(e)
			key := fname[:strings.IndexAny(fname, "(.")]
			if bytes, err := os.ReadFile(e); err == nil {
				value := string(bytes)
				for _, s := range kv.kvs {
					s.Set(key, value)
				}
			} else {
				log.Fatalln(err)
			}
			return key
		},
	))
}

///////////////////////////////////////////////////////

func (kv *KVStorage) Clear(rmPersistent bool) {
	if rmPersistent {
		if fd.DirExists(kv.dir) {
			os.RemoveAll(kv.dir)
		}
	}
	for _, s := range kv.kvs {
		s.Clear()
	}
}

///////////////////////////////////////////////////////

func (kv *KVStorage) AppendJSONFromFile(dir string) int {
	files, _, err := fd.WalkFileDir(dir, true)
	if err != nil {
		return 0
	}
	return len(ts.FM(
		files,
		func(i int, e string) bool { return strings.HasSuffix(e, withDot("json")) },
		func(i int, e string) string {
			fname := filepath.Base(e)
			key := fname[:strings.IndexAny(fname, "(.")]
			file, err := os.OpenFile(e, os.O_RDONLY, os.ModePerm)
			if err != nil {
				log.Fatalln(err)
			}
			defer file.Close()

			ResultOfScan, _ := jt.ScanObject(file, false, true, jt.OUT_MIN)
			for rst := range ResultOfScan {
				if rst.Err == nil {
					kv.Save(key, rst.Obj, true)
				}
			}

			return key
		},
	))
}
