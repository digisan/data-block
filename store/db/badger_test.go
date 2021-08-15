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
