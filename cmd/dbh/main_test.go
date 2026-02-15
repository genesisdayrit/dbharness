package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallTemplateForceCreatesFullSnapshot(t *testing.T) {
	projectDir := t.TempDir()
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("chdir to temp project: %v", err)
	}
	defer func() {
		_ = os.Chdir(originalWD)
	}()

	targetDir := ".dbharness"
	if err := os.MkdirAll(filepath.Join(targetDir, "context", "connections"), 0o755); err != nil {
		t.Fatalf("mkdir existing .dbharness: %v", err)
	}

	oldConfig := `{"connections":[{"name":"legacy"}]}`
	if err := os.WriteFile(filepath.Join(targetDir, "config.json"), []byte(oldConfig), 0o644); err != nil {
		t.Fatalf("write existing config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "README.md"), []byte("legacy readme\n"), 0o644); err != nil {
		t.Fatalf("write existing README: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, ".gitignore"), []byte("*.tmp\n"), 0o644); err != nil {
		t.Fatalf("write existing dotfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "legacy.txt"), []byte("legacy marker\n"), 0o644); err != nil {
		t.Fatalf("write marker file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "context", "connections", "notes.txt"), []byte("nested data\n"), 0o644); err != nil {
		t.Fatalf("write nested file: %v", err)
	}

	snapshotPath, err := installTemplate(targetDir, true)
	if err != nil {
		t.Fatalf("installTemplate(..., true) error = %v", err)
	}
	if strings.TrimSpace(snapshotPath) == "" {
		t.Fatalf("installTemplate(..., true) snapshotPath is empty")
	}
	if got := filepath.Base(filepath.Dir(snapshotPath)); got != ".dbharness-snapshots" {
		t.Fatalf("snapshot parent = %q, want %q", got, ".dbharness-snapshots")
	}

	assertFileContent(t, filepath.Join(snapshotPath, "config.json"), oldConfig)
	assertFileContent(t, filepath.Join(snapshotPath, "README.md"), "legacy readme\n")
	assertFileContent(t, filepath.Join(snapshotPath, ".gitignore"), "*.tmp\n")
	assertFileContent(t, filepath.Join(snapshotPath, "context", "connections", "notes.txt"), "nested data\n")

	// The old directory should be replaced by the template (legacy marker removed).
	if _, err := os.Stat(filepath.Join(targetDir, "legacy.txt")); !os.IsNotExist(err) {
		t.Fatalf("legacy file should be removed by force install, stat err = %v", err)
	}

	newConfig, err := os.ReadFile(filepath.Join(targetDir, "config.json"))
	if err != nil {
		t.Fatalf("read new config: %v", err)
	}
	if string(newConfig) == oldConfig {
		t.Fatalf("new config should not equal previous config after force install")
	}

	gitignoreData, err := os.ReadFile(".gitignore")
	if err != nil {
		t.Fatalf("read project .gitignore: %v", err)
	}
	if !strings.Contains(string(gitignoreData), ".dbharness-snapshots/") {
		t.Fatalf("project .gitignore should include .dbharness-snapshots/, got:\n%s", string(gitignoreData))
	}
}

func assertFileContent(t *testing.T, path, expected string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(data) != expected {
		t.Fatalf("content of %s = %q, want %q", path, string(data), expected)
	}
}
