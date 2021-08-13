package impl

import (
	"testing"
)

func TestNewFS(t *testing.T) {
	type args struct {
		dir string
		ext string
	}
	tests := []struct {
		name string
		args args
		want *FileStore
	}{
		// TODO: Add test cases.
		{
			name: "NewFS",
			args: args{
				dir: "./temp",
				ext: "json",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			NewFS(tt.args.dir, tt.args.ext, true)
		})
	}
}
