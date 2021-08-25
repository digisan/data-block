package db

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
)

func TestNewBadgerDB(t *testing.T) {
	type args struct {
		folderPath string
	}
	tests := []struct {
		name    string
		args    args
		want    *badger.DB
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "NewBadgerDB",
			args: args{
				folderPath: "./data/badger",
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			NewBadgerDB(tt.args.folderPath)
		})
	}
}

func TestBadgerDumpFile(t *testing.T) {
	badgerDB, err := NewBadgerDB("../data/badger")
	if err != nil {
		panic(err)
	}
	defer badgerDB.Close()

	type args struct {
		db   *badger.DB
		file string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "BadgerDumpFile",
			args: args{
				db:   badgerDB,
				file: "./dump.txt",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := BadgerDumpFile(tt.args.db, tt.args.file); (err != nil) != tt.wantErr {
				t.Errorf("BadgerDumpFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
