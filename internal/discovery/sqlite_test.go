package discovery

import (
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"testing"
)

func TestFactoryFunctionsSupportSQLite(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)

	discoverer, err := New(DatabaseConfig{
		Type:     "sqlite",
		Database: dbPath,
	})
	if err != nil {
		t.Fatalf("New(sqlite) error = %v", err)
	}
	defer discoverer.Close()

	tableDiscoverer, err := NewTableDetailDiscoverer(DatabaseConfig{
		Type:     "sqlite",
		Database: dbPath,
	})
	if err != nil {
		t.Fatalf("NewTableDetailDiscoverer(sqlite) error = %v", err)
	}
	defer tableDiscoverer.Close()

	lister, err := NewDatabaseLister(DatabaseConfig{
		Type:     "sqlite",
		Database: dbPath,
	})
	if err != nil {
		t.Fatalf("NewDatabaseLister(sqlite) error = %v", err)
	}
	defer lister.Close()
}

func TestSQLiteDiscoverer_Discover(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)
	discoverer, err := newSQLite(DatabaseConfig{Database: dbPath})
	if err != nil {
		t.Fatalf("newSQLite() error = %v", err)
	}
	defer discoverer.Close()

	ctx := context.Background()
	schemas, err := discoverer.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(schemas) != 1 {
		t.Fatalf("schema count = %d, want 1", len(schemas))
	}
	if schemas[0].Name != "main" {
		t.Fatalf("schema name = %q, want %q", schemas[0].Name, "main")
	}

	var gotUsers, gotView, gotInternal bool
	for _, table := range schemas[0].Tables {
		switch table.Name {
		case "users":
			gotUsers = true
			if table.TableType != "BASE TABLE" {
				t.Fatalf("users table type = %q, want %q", table.TableType, "BASE TABLE")
			}
		case "user_emails":
			gotView = true
			if table.TableType != "VIEW" {
				t.Fatalf("user_emails table type = %q, want %q", table.TableType, "VIEW")
			}
		case "sqlite_sequence":
			gotInternal = true
		}
	}
	if !gotUsers {
		t.Fatalf("expected users table to be discovered")
	}
	if !gotView {
		t.Fatalf("expected user_emails view to be discovered")
	}
	if gotInternal {
		t.Fatalf("sqlite internal tables should be excluded from discovery")
	}
}

func TestSQLiteDiscoverer_GetColumns(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)
	discoverer, err := newSQLite(DatabaseConfig{Database: dbPath})
	if err != nil {
		t.Fatalf("newSQLite() error = %v", err)
	}
	defer discoverer.Close()

	ctx := context.Background()
	columns, err := discoverer.GetColumns(ctx, "main", "users")
	if err != nil {
		t.Fatalf("GetColumns() error = %v", err)
	}
	if len(columns) != 3 {
		t.Fatalf("column count = %d, want 3", len(columns))
	}

	if columns[0].Name != "id" || columns[0].OrdinalPosition != 1 {
		t.Fatalf("first column = %+v, want id at ordinal 1", columns[0])
	}
	if columns[0].IsNullable != "NO" {
		t.Fatalf("id is_nullable = %q, want %q", columns[0].IsNullable, "NO")
	}

	if columns[1].Name != "name" || columns[1].DataType != "TEXT" {
		t.Fatalf("second column = %+v, want name TEXT", columns[1])
	}
	if columns[1].IsNullable != "NO" {
		t.Fatalf("name is_nullable = %q, want %q", columns[1].IsNullable, "NO")
	}

	if columns[2].Name != "email" || columns[2].DataType != "TEXT" {
		t.Fatalf("third column = %+v, want email TEXT", columns[2])
	}
	if columns[2].IsNullable != "YES" {
		t.Fatalf("email is_nullable = %q, want %q", columns[2].IsNullable, "YES")
	}
}

func TestSQLiteDiscoverer_GetColumnEnrichment(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)
	discoverer, err := newSQLite(DatabaseConfig{Database: dbPath})
	if err != nil {
		t.Fatalf("newSQLite() error = %v", err)
	}
	defer discoverer.Close()

	ctx := context.Background()
	columns, err := discoverer.GetColumns(ctx, "main", "users")
	if err != nil {
		t.Fatalf("GetColumns() error = %v", err)
	}

	var emailColumn ColumnInfo
	found := false
	for _, c := range columns {
		if c.Name == "email" {
			emailColumn = c
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("email column not found")
	}

	profile, err := discoverer.GetColumnEnrichment(ctx, "main", "users", emailColumn)
	if err != nil {
		t.Fatalf("GetColumnEnrichment() error = %v", err)
	}

	if profile.TotalRows != 3 {
		t.Fatalf("total_rows = %d, want 3", profile.TotalRows)
	}
	if profile.NullCount != 1 {
		t.Fatalf("null_count = %d, want 1", profile.NullCount)
	}
	if profile.NonNullCount != 2 {
		t.Fatalf("non_null_count = %d, want 2", profile.NonNullCount)
	}
	if profile.DistinctNonNullCount != 2 {
		t.Fatalf("distinct_non_null_count = %d, want 2", profile.DistinctNonNullCount)
	}

	if profile.NullOfTotalRowsPct != 33.3333 {
		t.Fatalf("null_of_total_rows_pct = %v, want %v", profile.NullOfTotalRowsPct, 33.3333)
	}
	if profile.NonNullOfTotalRowsPct != 66.6667 {
		t.Fatalf("non_null_of_total_rows_pct = %v, want %v", profile.NonNullOfTotalRowsPct, 66.6667)
	}
	if profile.DistinctOfNonNullPct != 100 {
		t.Fatalf("distinct_of_non_null_pct = %v, want %v", profile.DistinctOfNonNullPct, 100.0)
	}

	if len(profile.SampleValues) == 0 {
		t.Fatalf("expected sample values for email column")
	}
	if !slices.Contains(profile.SampleValues, "alice@example.com") {
		t.Fatalf("sample values %v should include alice@example.com", profile.SampleValues)
	}
	if !slices.Contains(profile.SampleValues, "cara@example.com") {
		t.Fatalf("sample values %v should include cara@example.com", profile.SampleValues)
	}
}

func TestSQLiteDiscoverer_GetSampleRows(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)
	discoverer, err := newSQLite(DatabaseConfig{Database: dbPath})
	if err != nil {
		t.Fatalf("newSQLite() error = %v", err)
	}
	defer discoverer.Close()

	sample, err := discoverer.GetSampleRows(context.Background(), "main", "users", 2)
	if err != nil {
		t.Fatalf("GetSampleRows() error = %v", err)
	}
	if len(sample.Columns) != 3 {
		t.Fatalf("sample column count = %d, want 3", len(sample.Columns))
	}
	if len(sample.Rows) == 0 {
		t.Fatalf("sample row count = 0, want at least 1")
	}
	if len(sample.Rows) > 2 {
		t.Fatalf("sample row count = %d, want <= 2", len(sample.Rows))
	}
}

func TestSQLiteDatabaseLister_IncludesAttachedDatabases(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)
	attachedPath := createAttachedSQLiteDatabase(t)

	lister, err := newSQLiteDatabaseLister(DatabaseConfig{Database: dbPath})
	if err != nil {
		t.Fatalf("newSQLiteDatabaseLister() error = %v", err)
	}
	defer lister.Close()

	ctx := context.Background()
	if _, err := lister.db.ExecContext(ctx, "ATTACH DATABASE ? AS analytics", attachedPath); err != nil {
		t.Fatalf("attach database error = %v", err)
	}

	databases, err := lister.ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases() error = %v", err)
	}
	if len(databases) != 2 {
		t.Fatalf("database count = %d, want 2", len(databases))
	}
	if databases[0] != "main" || databases[1] != "analytics" {
		t.Fatalf("databases = %v, want [main analytics]", databases)
	}
}

func TestSQLiteDiscoverer_IncludesAttachedSchemas(t *testing.T) {
	dbPath := createSQLiteTestDatabase(t)
	attachedPath := createAttachedSQLiteDatabase(t)

	discoverer, err := newSQLite(DatabaseConfig{Database: dbPath})
	if err != nil {
		t.Fatalf("newSQLite() error = %v", err)
	}
	defer discoverer.Close()

	ctx := context.Background()
	if _, err := discoverer.db.ExecContext(ctx, "ATTACH DATABASE ? AS analytics", attachedPath); err != nil {
		t.Fatalf("attach database error = %v", err)
	}

	schemas, err := discoverer.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	if len(schemas) != 2 {
		t.Fatalf("schema count = %d, want 2", len(schemas))
	}
	if schemas[0].Name != "main" || schemas[1].Name != "analytics" {
		t.Fatalf("schemas = %v, want [main analytics]", []string{schemas[0].Name, schemas[1].Name})
	}

	foundEvents := false
	for _, table := range schemas[1].Tables {
		if table.Name == "events" && table.TableType == "BASE TABLE" {
			foundEvents = true
			break
		}
	}
	if !foundEvents {
		t.Fatalf("expected analytics.events table in attached schema, got %+v", schemas[1].Tables)
	}
}

func createSQLiteTestDatabase(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "main.db")
	db := openSQLiteForTest(t, path)
	defer db.Close()

	execSQLite(t, db, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT
		);
	`)
	execSQLite(t, db, `
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', NULL),
			('Cara', 'cara@example.com');
	`)
	execSQLite(t, db, `
		CREATE VIEW user_emails AS
		SELECT id, email
		FROM users
		WHERE email IS NOT NULL;
	`)

	return path
}

func createAttachedSQLiteDatabase(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "attached.db")
	db := openSQLiteForTest(t, path)
	defer db.Close()

	execSQLite(t, db, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			event_name TEXT NOT NULL
		);
	`)

	return path
}

func openSQLiteForTest(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open(sqlite) error = %v", err)
	}
	return db
}

func execSQLite(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("sqlite exec error (%s): %v", query, err)
	}
}
