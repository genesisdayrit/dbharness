package contextgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/genesisdayrit/dbharness/internal/discovery"
	"gopkg.in/yaml.v3"
)

func TestGenerate_WritesDefaultDatabaseFieldWhenProvided(t *testing.T) {
	baseDir := t.TempDir()

	schemas := []discovery.SchemaInfo{
		{
			Name: "public",
			Tables: []discovery.TableInfo{
				{Name: "users", TableType: "BASE TABLE"},
			},
		},
	}

	opts := Options{
		ConnectionName: "my-db",
		DatabaseName:   "analytics",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	if err := Generate(schemas, opts); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "my-db")
	if df.DefaultDatabase != "analytics" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "analytics")
	}
	if !strings.Contains(raw, "default_database: analytics") {
		t.Fatalf("expected _databases.yml to contain default_database field, got:\n%s", raw)
	}
}

func TestGenerate_UsesDefaultSentinelWhenDatabaseMissing(t *testing.T) {
	baseDir := t.TempDir()

	schemas := []discovery.SchemaInfo{
		{
			Name: "public",
			Tables: []discovery.TableInfo{
				{Name: "users", TableType: "BASE TABLE"},
			},
		},
	}

	opts := Options{
		ConnectionName: "my-db",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	if err := Generate(schemas, opts); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "my-db")
	if df.DefaultDatabase != "_default" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "_default")
	}
	if !strings.Contains(raw, "default_database: _default") {
		t.Fatalf("expected _databases.yml to contain default_database fallback, got:\n%s", raw)
	}
}

func TestUpdateDatabasesFile_WritesDefaultDatabaseFieldWhenProvided(t *testing.T) {
	baseDir := t.TempDir()

	opts := Options{
		ConnectionName: "warehouse",
		DatabaseName:   "core",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if _, err := UpdateDatabasesFile([]string{"core", "sandbox"}, opts); err != nil {
		t.Fatalf("UpdateDatabasesFile() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "warehouse")
	if df.DefaultDatabase != "core" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "core")
	}
	if !strings.Contains(raw, "default_database: core") {
		t.Fatalf("expected _databases.yml to contain default_database field, got:\n%s", raw)
	}
}

func TestUpdateDatabasesFile_UsesFirstDiscoveredDatabaseWhenDefaultMissing(t *testing.T) {
	baseDir := t.TempDir()

	opts := Options{
		ConnectionName: "warehouse",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if _, err := UpdateDatabasesFile([]string{"core", "sandbox"}, opts); err != nil {
		t.Fatalf("UpdateDatabasesFile() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "warehouse")
	if df.DefaultDatabase != "core" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "core")
	}
	if !strings.Contains(raw, "default_database: core") {
		t.Fatalf("expected _databases.yml to contain fallback default_database field, got:\n%s", raw)
	}
}

func TestUpdateDatabasesFile_PreservesExistingDefaultWhenConfigDefaultMissing(t *testing.T) {
	baseDir := t.TempDir()

	initialOpts := Options{
		ConnectionName: "warehouse",
		DatabaseName:   "reporting",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if _, err := UpdateDatabasesFile([]string{"core", "sandbox"}, initialOpts); err != nil {
		t.Fatalf("initial UpdateDatabasesFile() error = %v", err)
	}

	updateOpts := Options{
		ConnectionName: "warehouse",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if _, err := UpdateDatabasesFile([]string{"core", "sandbox", "zeta"}, updateOpts); err != nil {
		t.Fatalf("second UpdateDatabasesFile() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "warehouse")
	if df.DefaultDatabase != "reporting" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "reporting")
	}
	if !strings.Contains(raw, "default_database: reporting") {
		t.Fatalf("expected _databases.yml to preserve default_database field, got:\n%s", raw)
	}
}

func readDatabasesFile(t *testing.T, baseDir, connection string) (DatabasesFile, string) {
	t.Helper()

	path := filepath.Join(baseDir, "context", "connections", connection, "databases", "_databases.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read _databases.yml: %v", err)
	}

	var df DatabasesFile
	if err := yaml.Unmarshal(data, &df); err != nil {
		t.Fatalf("unmarshal _databases.yml: %v", err)
	}

	return df, string(data)
}
