package json

import (
	"os"
	"testing"
)

func TestFlattenStore(t *testing.T) {
	file, err := os.Open("../../data/glossary.json")
	if err == nil {
		FlattenStore(file)
	}
}
