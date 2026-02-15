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

func TestGenerate_RequiresDefaultDatabaseSelectionWhenMissing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		databaseType string
	}{
		{name: "postgres", databaseType: "postgres"},
		{name: "snowflake", databaseType: "snowflake"},
	}

	schemas := []discovery.SchemaInfo{
		{
			Name: "public",
			Tables: []discovery.TableInfo{
				{Name: "users", TableType: "BASE TABLE"},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			opts := Options{
				ConnectionName: "my-db",
				DatabaseType:   tc.databaseType,
				BaseDir:        baseDir,
			}

			err := Generate(schemas, opts)
			if err == nil {
				t.Fatalf("Generate() error = nil, want missing default database error")
			}
			if !strings.Contains(err.Error(), "select a database from this connection") {
				t.Fatalf("Generate() error = %q, want selection guidance", err)
			}

			databasesPath := filepath.Join(baseDir, "context", "connections", "my-db", "databases", "_databases.yml")
			if _, statErr := os.Stat(databasesPath); !os.IsNotExist(statErr) {
				t.Fatalf("_databases.yml should not be created on error, stat err = %v", statErr)
			}
		})
	}
}

func TestGenerate_SortsSchemasAndTablesAndIncludesTableDetails(t *testing.T) {
	baseDir := t.TempDir()

	schemas := []discovery.SchemaInfo{
		{
			Name: "zeta",
			Tables: []discovery.TableInfo{
				{Name: "events", TableType: "BASE TABLE"},
				{Name: "daily_metrics", TableType: "VIEW"},
			},
		},
		{
			Name: "analytics",
			Tables: []discovery.TableInfo{
				{Name: "users", TableType: "BASE TABLE"},
				{Name: "accounts", TableType: "BASE TABLE"},
			},
		},
	}

	opts := Options{
		ConnectionName: "my-db",
		DatabaseName:   "warehouse",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	if err := Generate(schemas, opts); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}

	sf := readSchemasFile(t, baseDir, "my-db", "warehouse")
	if got, want := len(sf.Schemas), 2; got != want {
		t.Fatalf("schema count = %d, want %d", got, want)
	}

	if sf.Schemas[0].Name != "analytics" || sf.Schemas[1].Name != "zeta" {
		t.Fatalf("schema order = [%s, %s], want [analytics, zeta]", sf.Schemas[0].Name, sf.Schemas[1].Name)
	}

	analytics := sf.Schemas[0]
	if analytics.TableCount != 2 || analytics.ViewCount != 0 {
		t.Fatalf("analytics counts = tables:%d views:%d, want tables:2 views:0", analytics.TableCount, analytics.ViewCount)
	}
	if len(analytics.Tables) != 2 {
		t.Fatalf("analytics table list len = %d, want 2", len(analytics.Tables))
	}
	if analytics.Tables[0].Name != "accounts" || analytics.Tables[0].Type != "BASE TABLE" {
		t.Fatalf("first analytics table = %+v, want accounts BASE TABLE", analytics.Tables[0])
	}
	if analytics.Tables[1].Name != "users" || analytics.Tables[1].Type != "BASE TABLE" {
		t.Fatalf("second analytics table = %+v, want users BASE TABLE", analytics.Tables[1])
	}
	if analytics.Tables[0].Description != "" || analytics.Tables[1].Description != "" {
		t.Fatalf("analytics table descriptions should be blank placeholders, got %+v", analytics.Tables)
	}

	zeta := sf.Schemas[1]
	if zeta.TableCount != 1 || zeta.ViewCount != 1 {
		t.Fatalf("zeta counts = tables:%d views:%d, want tables:1 views:1", zeta.TableCount, zeta.ViewCount)
	}
	if len(zeta.Tables) != 2 {
		t.Fatalf("zeta table list len = %d, want 2", len(zeta.Tables))
	}
	if zeta.Tables[0].Name != "daily_metrics" || zeta.Tables[0].Type != "VIEW" {
		t.Fatalf("first zeta table = %+v, want daily_metrics VIEW", zeta.Tables[0])
	}
	if zeta.Tables[1].Name != "events" || zeta.Tables[1].Type != "BASE TABLE" {
		t.Fatalf("second zeta table = %+v, want events BASE TABLE", zeta.Tables[1])
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

func TestUpdateDatabasesFile_UsesOnlyDatabaseWhenDefaultMissing(t *testing.T) {
	baseDir := t.TempDir()

	opts := Options{
		ConnectionName: "warehouse",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if _, err := UpdateDatabasesFile([]string{"core"}, opts); err != nil {
		t.Fatalf("UpdateDatabasesFile() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "warehouse")
	if df.DefaultDatabase != "core" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "core")
	}
	if !strings.Contains(raw, "default_database: core") {
		t.Fatalf("expected _databases.yml to contain single-database fallback default, got:\n%s", raw)
	}
}

func TestUpdateDatabasesFile_UsesSentinelWhenMultipleAndDefaultMissing(t *testing.T) {
	baseDir := t.TempDir()

	opts := Options{
		ConnectionName: "warehouse",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if _, err := UpdateDatabasesFile([]string{"core", "sandbox", "zeta"}, opts); err != nil {
		t.Fatalf("UpdateDatabasesFile() error = %v", err)
	}

	df, raw := readDatabasesFile(t, baseDir, "warehouse")
	if df.DefaultDatabase != "_default" {
		t.Fatalf("default database = %q, want %q", df.DefaultDatabase, "_default")
	}
	if !strings.Contains(raw, "default_database: _default") {
		t.Fatalf("expected _databases.yml to contain _default sentinel, got:\n%s", raw)
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

func readSchemasFile(t *testing.T, baseDir, connection, database string) SchemasFile {
	t.Helper()

	path := filepath.Join(
		baseDir,
		"context",
		"connections",
		connection,
		"databases",
		sanitizeName(database),
		"schemas",
		"_schemas.yml",
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read _schemas.yml: %v", err)
	}

	var sf SchemasFile
	if err := yaml.Unmarshal(data, &sf); err != nil {
		t.Fatalf("unmarshal _schemas.yml: %v", err)
	}
	return sf
}
