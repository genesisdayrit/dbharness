package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/genesisdayrit/dbharness/internal/template"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "init":
		runInit(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: dbharness init [--force]")
}

func runInit(args []string) {
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	force := flags.Bool("force", false, "Overwrite an existing .dbharness folder.")
	_ = flags.Parse(args)

	targetDir := filepath.Join(".", ".dbharness")
	if err := installTemplate(targetDir, *force); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Installed .dbharness to %s\n", targetDir)
}

func installTemplate(targetDir string, force bool) error {
	if info, err := os.Stat(targetDir); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("target exists and is not a directory: %s", targetDir)
		}
		if !force {
			return fmt.Errorf("target already exists: %s (use --force to overwrite)", targetDir)
		}
		if err := os.RemoveAll(targetDir); err != nil {
			return fmt.Errorf("remove existing target: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check target: %w", err)
	}

	root, err := template.Root()
	if err != nil {
		return fmt.Errorf("load template: %w", err)
	}

	return copyFS(root, targetDir)
}

func copyFS(source fs.FS, targetDir string) error {
	return fs.WalkDir(source, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		destPath := filepath.Join(targetDir, path)
		if entry.IsDir() {
			return os.MkdirAll(destPath, 0o755)
		}

		data, err := fs.ReadFile(source, path)
		if err != nil {
			return err
		}

		return os.WriteFile(destPath, data, 0o644)
	})
}
