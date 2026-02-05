package template

import (
	"embed"
	"io/fs"
)

//go:embed .dbharness/**
var embedded embed.FS

func Root() (fs.FS, error) {
	return fs.Sub(embedded, ".dbharness")
}
