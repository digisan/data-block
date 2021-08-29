package impl

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v3"
	fd "github.com/digisan/gotk/filedir"
	"github.com/digisan/gotk/io"
	"github.com/digisan/gotk/slice/ti"
	"github.com/digisan/gotk/slice/ts"
)

type FileStore struct {
	dir       string
	ext       string
	repeatIdx bool
}

func NewFS(dir, ext string, repeatIdx bool) *FileStore {
	abspath, _ := fd.AbsPath(dir, false)
	io.MustCreateDir(abspath)
	return &FileStore{dir: abspath, ext: fd.DotExt(ext), repeatIdx: repeatIdx}
}

func (fs *FileStore) Len() int {
	files, _, err := fd.WalkFileDir(fs.dir, false)
	if err != nil {
		panic("error @ FileStore Len")
	}
	files = ts.FM(files, func(i int, e string) bool { return strings.HasSuffix(e, fs.ext) }, nil)
	return len(files)
}

func (fs *FileStore) Set(key, value interface{}) bool {
	if fs.dir != "" {

		fullpath := filepath.Join(fs.dir, fmt.Sprint(key)) // full abs file name path without extension
		ext := fs.ext                                      // extension with prefix '.', if empty, then no '.'
		prevpath := ""

		if fs.repeatIdx {
			// record duplicate key number in fullpath as .../key(number).ext
			if matches, err := filepath.Glob(fullpath + "(*)" + ext); err == nil {
				if len(matches) > 0 {
					prevpath = matches[0]
					fname := filepath.Base(prevpath)
					pO, pC := strings.Index(fname, "("), strings.Index(fname, ")")
					num, _ := strconv.Atoi(fname[pO+1 : pC])
					fullpath = filepath.Join(fs.dir, fmt.Sprintf("%s(%d)", fname[:pO], num+1))
				} else {
					fullpath = fmt.Sprintf("%s(1)", fullpath)
				}
			}
		}

		// add extension
		fullpath = fullpath + ext

		if prevpath == "" {
			prevpath = fullpath
		}
		io.MustWriteFile(prevpath, []byte(fmt.Sprint(value)))
		os.Rename(prevpath, fullpath)
		return true
	}
	return false
}

func (fs *FileStore) Get(key interface{}) (interface{}, bool) {
	if fs.dir != "" {
		fullpath := filepath.Join(fs.dir, fmt.Sprint(key))
		ext := fs.ext

		if fs.repeatIdx {
			// search path with .../key(number).ext
			if matches, err := filepath.Glob(fullpath + "(*)" + ext); err == nil {
				if len(matches) > 0 {
					fullpath = matches[0]
				}
			}
		} else {
			// add extension
			fullpath = fullpath + ext
		}

		if fd.FileExists(fullpath) {
			if bytes, err := os.ReadFile(fullpath); err == nil {
				return string(bytes), true
			}
		}
	}
	return "", false
}

func (fs *FileStore) Remove(key interface{}) {
	file := filepath.Join(fs.dir, key.(string)+fs.ext)
	if os.RemoveAll(file) != nil {
		fmt.Printf("%v cannot be removed successfully", key)
	}

	file = filepath.Join(fs.dir, key.(string)+"(*)"+fs.ext)
	files, err := filepath.Glob(file)
	if err == nil {
		for _, f := range files {
			if os.RemoveAll(f) != nil {
				fmt.Printf("%v cannot be removed successfully", key)
			}
		}
	}
}

func (fs *FileStore) Range(f func(key, value interface{}) bool) {
	if files, _, err := fd.WalkFileDir(fs.dir, true); err == nil {
		for _, file := range files {
			bytes, err := os.ReadFile(file)
			if err != nil {
				fmt.Printf("%s cannot be read, [%v]\n", file, err)
				continue
			}
			file = filepath.Base(file)
			file = strings.TrimSuffix(file, filepath.Ext(file))
			if p := strings.LastIndex(file, "("); p >= 0 {
				file = file[:p]
			}
			if !f(file, string(bytes)) {
				break
			}
		}
	}
}

func (fs *FileStore) Clear() {
	files, _, err := fd.WalkFileDir(fs.dir, false)
	if err != nil {
		panic("error @ FileStore Clear")
	}
	files = ts.FM(files, func(i int, e string) bool { return strings.HasSuffix(e, fs.ext) }, nil)
	for _, file := range files {
		if os.Remove(file) != nil {
			panic("error @ FileStore Clear")
		}
	}
}

func (fs *FileStore) OnConflict(f func(existing, coming interface{}) (bool, interface{})) func(existing, coming interface{}) (bool, interface{}) {
	if f != nil {
		return f
	}
	return func(existing, coming interface{}) (bool, interface{}) {
		return true, coming
	}
}

func (fs *FileStore) IsPersistent() bool {
	return true
}

func (fs *FileStore) SyncToBadgerWriteBatch(wb *badger.WriteBatch, ext string) error {
	if wb == nil {
		return fmt.Errorf("writebatch is nil, flushed nothing")
	}

	files, _, err := fd.WalkFileDir(fs.dir, false)
	if err != nil {
		panic(err)
	}
	ts.FM(files, func(i int, e string) bool {

		if strings.HasSuffix(e, ext) {
			fn := filepath.Base(e)
			p1 := strings.LastIndex(fn, "(")
			p2 := strings.LastIndex(fn, ext)
			ps := ti.FM([]int{p1, p2}, func(i, e int) bool { return e > -1 }, nil)
			key := fn[:ti.Min(ps...)]
			bytes, e := os.ReadFile(e)
			if e != nil {
				panic(e)
			}

			kBuf := append([]byte("s"), []byte(fmt.Sprint(key))...)
			vBuf := append([]byte("s"), bytes...)
			if err = wb.Set(kBuf, vBuf); err != nil {
				return false
			}
		}
		return true

	}, nil)

	return err
}

func (fs *FileStore) FlushToBadger(db *badger.DB, ext string) error {
	if db == nil {
		return fmt.Errorf("db is nil, flushed nothing")
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	return fs.SyncToBadgerWriteBatch(wb, ext)
}
