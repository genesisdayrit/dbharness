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
		{name: "mysql", databaseType: "mysql"},
		{name: "bigquery", databaseType: "bigquery"},
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
	if analytics.Tables[0].AIDescription != "" || analytics.Tables[1].AIDescription != "" {
		t.Fatalf("analytics table ai_description should be blank placeholders, got %+v", analytics.Tables)
	}
	if analytics.Tables[0].DBDescription != "" || analytics.Tables[1].DBDescription != "" {
		t.Fatalf("analytics table db_description should be blank placeholders, got %+v", analytics.Tables)
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

func TestGenerateTableDetails_WritesColumnsAndSampleFiles(t *testing.T) {
	baseDir := t.TempDir()

	tables := []TableDetailInput{
		{
			Schema: "public",
			Table:  "users",
			Columns: []discovery.ColumnInfo{
				{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1, ColumnDefault: "nextval('users_id_seq'::regclass)"},
				{Name: "name", DataType: "character varying", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "email", DataType: "character varying", IsNullable: "NO", OrdinalPosition: 3},
			},
			Sample: &discovery.SampleResult{
				Columns: []string{"id", "name", "email"},
				Rows: [][]string{
					{"1", "Alice", "alice@example.com"},
					{"2", "Bob", "bob@example.com"},
				},
			},
		},
	}

	opts := Options{
		ConnectionName: "my-db",
		DatabaseName:   "analytics",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	if err := GenerateTableDetails(tables, opts); err != nil {
		t.Fatalf("GenerateTableDetails() error = %v", err)
	}

	tableDir := filepath.Join(baseDir, "context", "connections", "my-db", "databases", "analytics", "schemas", "public", "users")

	// Verify columns YAML
	colPath := filepath.Join(tableDir, "users__columns.yml")
	colData, err := os.ReadFile(colPath)
	if err != nil {
		t.Fatalf("read columns file: %v", err)
	}

	var cf ColumnsFile
	if err := yaml.Unmarshal(colData, &cf); err != nil {
		t.Fatalf("unmarshal columns: %v", err)
	}
	if cf.Schema != "public" || cf.Table != "users" {
		t.Fatalf("columns file header = schema:%q table:%q, want public/users", cf.Schema, cf.Table)
	}
	if len(cf.Columns) != 3 {
		t.Fatalf("column count = %d, want 3", len(cf.Columns))
	}
	if cf.Columns[0].Name != "id" || cf.Columns[0].DataType != "integer" {
		t.Fatalf("first column = %+v, want id/integer", cf.Columns[0])
	}
	if cf.Columns[1].IsNullable != "YES" {
		t.Fatalf("second column is_nullable = %q, want YES", cf.Columns[1].IsNullable)
	}

	// Verify header comment
	colStr := string(colData)
	if !strings.Contains(colStr, "Columns for table: public.users") {
		t.Fatalf("columns file should contain header comment, got:\n%s", colStr[:200])
	}

	// Verify sample XML
	samplePath := filepath.Join(tableDir, "users__sample.xml")
	sampleData, err := os.ReadFile(samplePath)
	if err != nil {
		t.Fatalf("read sample file: %v", err)
	}

	sampleStr := string(sampleData)
	if !strings.Contains(sampleStr, `<?xml version="1.0" encoding="UTF-8"?>`) {
		t.Fatalf("sample XML should contain XML header")
	}
	if !strings.Contains(sampleStr, `schema="public"`) {
		t.Fatalf("sample XML should contain schema attr")
	}
	if !strings.Contains(sampleStr, `row_count="2"`) {
		t.Fatalf("sample XML should contain row_count attr, got:\n%s", sampleStr)
	}
	if !strings.Contains(sampleStr, `name="email"`) {
		t.Fatalf("sample XML should contain field names")
	}
	if !strings.Contains(sampleStr, "alice@example.com") {
		t.Fatalf("sample XML should contain sample data values")
	}
}

func TestGenerateTableDetails_CreatesTableDirectories(t *testing.T) {
	baseDir := t.TempDir()

	tables := []TableDetailInput{
		{
			Schema: "analytics",
			Table:  "events",
			Columns: []discovery.ColumnInfo{
				{Name: "event_id", DataType: "bigint", IsNullable: "NO", OrdinalPosition: 1},
			},
			Sample: &discovery.SampleResult{
				Columns: []string{"event_id"},
				Rows:    [][]string{{"42"}},
			},
		},
		{
			Schema: "analytics",
			Table:  "sessions",
			Columns: []discovery.ColumnInfo{
				{Name: "session_id", DataType: "uuid", IsNullable: "NO", OrdinalPosition: 1},
			},
			Sample: nil, // no sample data
		},
	}

	opts := Options{
		ConnectionName: "warehouse",
		DatabaseName:   "prod",
		DatabaseType:   "snowflake",
		BaseDir:        baseDir,
	}

	if err := GenerateTableDetails(tables, opts); err != nil {
		t.Fatalf("GenerateTableDetails() error = %v", err)
	}

	eventsDir := filepath.Join(baseDir, "context", "connections", "warehouse", "databases", "prod", "schemas", "analytics", "events")
	sessionsDir := filepath.Join(baseDir, "context", "connections", "warehouse", "databases", "prod", "schemas", "analytics", "sessions")

	// Both directories should exist
	if _, err := os.Stat(eventsDir); os.IsNotExist(err) {
		t.Fatalf("events table directory should exist")
	}
	if _, err := os.Stat(sessionsDir); os.IsNotExist(err) {
		t.Fatalf("sessions table directory should exist")
	}

	// Events should have both files
	if _, err := os.Stat(filepath.Join(eventsDir, "events__columns.yml")); os.IsNotExist(err) {
		t.Fatalf("events columns file should exist")
	}
	if _, err := os.Stat(filepath.Join(eventsDir, "events__sample.xml")); os.IsNotExist(err) {
		t.Fatalf("events sample file should exist")
	}

	// Sessions should have columns but no sample
	if _, err := os.Stat(filepath.Join(sessionsDir, "sessions__columns.yml")); os.IsNotExist(err) {
		t.Fatalf("sessions columns file should exist")
	}
	if _, err := os.Stat(filepath.Join(sessionsDir, "sessions__sample.xml")); !os.IsNotExist(err) {
		t.Fatalf("sessions sample file should not exist (no sample data)")
	}
}

func TestGenerateTableDetails_SanitizesDirectoryNames(t *testing.T) {
	baseDir := t.TempDir()

	tables := []TableDetailInput{
		{
			Schema: "My Schema",
			Table:  "User.Events",
			Columns: []discovery.ColumnInfo{
				{Name: "id", DataType: "int", IsNullable: "NO", OrdinalPosition: 1},
			},
		},
	}

	opts := Options{
		ConnectionName: "test-conn",
		DatabaseName:   "test_db",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	if err := GenerateTableDetails(tables, opts); err != nil {
		t.Fatalf("GenerateTableDetails() error = %v", err)
	}

	expected := filepath.Join(baseDir, "context", "connections", "test-conn", "databases", "test_db", "schemas", "my_schema", "user_events")
	if _, err := os.Stat(expected); os.IsNotExist(err) {
		t.Fatalf("sanitized table directory should exist at %s", expected)
	}
	if _, err := os.Stat(filepath.Join(expected, "user_events__columns.yml")); os.IsNotExist(err) {
		t.Fatalf("sanitized columns file should exist")
	}
}

func TestWriteEnrichedColumnsFile_WritesEnrichedMetrics(t *testing.T) {
	baseDir := t.TempDir()

	input := EnrichedColumnsInput{
		Schema: "public",
		Table:  "users",
		Columns: []discovery.EnrichedColumnInfo{
			{
				Name:                  "id",
				DataType:              "integer",
				IsNullable:            "NO",
				OrdinalPosition:       1,
				ColumnDefault:         "nextval('users_id_seq'::regclass)",
				AIDescription:         "",
				DBDescription:         "",
				TotalRows:             200,
				NullCount:             0,
				NonNullCount:          200,
				DistinctNonNullCount:  200,
				DistinctOfNonNullPct:  100,
				NullOfTotalRowsPct:    0,
				NonNullOfTotalRowsPct: 100,
				SampleValues:          []string{"1", "2", "3"},
			},
			{
				Name:                  "email",
				DataType:              "character varying",
				IsNullable:            "YES",
				OrdinalPosition:       2,
				AIDescription:         "",
				DBDescription:         "",
				TotalRows:             200,
				NullCount:             20,
				NonNullCount:          180,
				DistinctNonNullCount:  175,
				DistinctOfNonNullPct:  97.2222,
				NullOfTotalRowsPct:    10,
				NonNullOfTotalRowsPct: 90,
				SampleValues:          []string{"alice@example.com"},
			},
		},
	}

	opts := Options{
		ConnectionName: "my-db",
		DatabaseName:   "analytics",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	path, err := WriteEnrichedColumnsFile(input, opts)
	if err != nil {
		t.Fatalf("WriteEnrichedColumnsFile() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read enriched columns file: %v", err)
	}

	var file EnrichedColumnsFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		t.Fatalf("unmarshal enriched columns file: %v", err)
	}

	if file.Schema != "public" || file.Table != "users" {
		t.Fatalf("header schema/table = %s/%s, want public/users", file.Schema, file.Table)
	}
	if len(file.Columns) != 2 {
		t.Fatalf("column count = %d, want 2", len(file.Columns))
	}
	if file.Columns[0].DistinctNonNullCount != 200 {
		t.Fatalf("first column distinct count = %d, want 200", file.Columns[0].DistinctNonNullCount)
	}
	if file.Columns[1].NullOfTotalRowsPct != 10 {
		t.Fatalf("second column null pct = %v, want 10", file.Columns[1].NullOfTotalRowsPct)
	}
	if file.Columns[1].AIDescription != "" {
		t.Fatalf("ai_description should be blank placeholder, got %q", file.Columns[1].AIDescription)
	}
	if file.Columns[1].DBDescription != "" {
		t.Fatalf("db_description should be blank placeholder, got %q", file.Columns[1].DBDescription)
	}
	if !strings.Contains(string(data), "Enriched columns for table: public.users") {
		t.Fatalf("expected enriched header comment in file")
	}
	if !strings.Contains(string(data), `ai_description: ""`) {
		t.Fatalf(`expected explicit blank ai_description field in YAML, got:
%s`, string(data))
	}
	if !strings.Contains(string(data), `db_description: ""`) {
		t.Fatalf(`expected explicit blank db_description field in YAML, got:
%s`, string(data))
	}
}

func TestWriteEnrichedColumnsFile_RejectsEmptyInput(t *testing.T) {
	baseDir := t.TempDir()

	opts := Options{
		ConnectionName: "my-db",
		DatabaseName:   "analytics",
		DatabaseType:   "postgres",
		BaseDir:        baseDir,
	}

	_, err := WriteEnrichedColumnsFile(EnrichedColumnsInput{
		Schema:  "public",
		Table:   "users",
		Columns: nil,
	}, opts)
	if err == nil {
		t.Fatalf("WriteEnrichedColumnsFile() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "no columns provided") {
		t.Fatalf("unexpected error: %v", err)
	}
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
