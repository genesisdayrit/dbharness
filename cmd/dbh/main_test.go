package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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

	assertFileContains(t, filepath.Join(targetDir, "AGENTS.md"), "Recommended traversal order")
	assertFileContains(t, filepath.Join(targetDir, "AGENTS.md"), "10 sample rows")
	assertFileContains(t, filepath.Join(targetDir, "AGENTS.md"), "## Memory Writing")
	assertDirectoryEmpty(t, filepath.Join(targetDir, "context", "workspaces", defaultWorkspaceName, "logs"))

	gitignoreData, err := os.ReadFile(".gitignore")
	if err != nil {
		t.Fatalf("read project .gitignore: %v", err)
	}
	if !strings.Contains(string(gitignoreData), ".dbharness-snapshots/") {
		t.Fatalf("project .gitignore should include .dbharness-snapshots/, got:\n%s", string(gitignoreData))
	}
}

func TestInstallTemplateFreshIncludesAgentsGuide(t *testing.T) {
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
	snapshotPath, err := installTemplate(targetDir, false)
	if err != nil {
		t.Fatalf("installTemplate(..., false) error = %v", err)
	}
	if snapshotPath != "" {
		t.Fatalf("snapshotPath = %q, want empty", snapshotPath)
	}

	assertFileContains(t, filepath.Join(targetDir, "AGENTS.md"), "Multi-connection rule")
	assertFileContains(t, filepath.Join(targetDir, "AGENTS.md"), "always start with the primary/default connection")
	assertFileContains(t, filepath.Join(targetDir, "AGENTS.md"), "## Memory Writing")
	assertDirectoryEmpty(t, filepath.Join(targetDir, "context", "workspaces", defaultWorkspaceName, "logs"))
}

func TestEnsureConnectionMemoryFileCreatesTemplate(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), ".dbharness")
	connectionName := "analytics"

	if err := ensureConnectionMemoryFile(baseDir, connectionName); err != nil {
		t.Fatalf("ensureConnectionMemoryFile(...) error = %v", err)
	}

	memoryPath := filepath.Join(baseDir, "context", "connections", connectionName, "MEMORY.md")
	assertFileContent(t, memoryPath, "# Long-Term Memory — analytics\n\nFacts, schema quirks, naming conventions, and query preferences discovered during agent sessions.\nPromoted and maintained automatically by coding agents following the criteria in AGENTS.md.\n")
}

func TestEnsureConnectionMemoryFileDoesNotOverwriteExistingFile(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), ".dbharness")
	connectionName := "warehouse"
	memoryPath := filepath.Join(baseDir, "context", "connections", connectionName, "MEMORY.md")

	if err := os.MkdirAll(filepath.Dir(memoryPath), 0o755); err != nil {
		t.Fatalf("mkdir memory directory: %v", err)
	}

	const customContent = "# Existing memory\n- keep this\n"
	if err := os.WriteFile(memoryPath, []byte(customContent), 0o644); err != nil {
		t.Fatalf("write existing memory file: %v", err)
	}

	if err := ensureConnectionMemoryFile(baseDir, connectionName); err != nil {
		t.Fatalf("ensureConnectionMemoryFile(...) error = %v", err)
	}

	assertFileContent(t, memoryPath, customContent)
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

func assertFileContains(t *testing.T, path, expectedSubstring string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(data), expectedSubstring) {
		t.Fatalf("content of %s did not contain %q", path, expectedSubstring)
	}
}

func assertDirectoryEmpty(t *testing.T, path string) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", path)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("read dir %s: %v", path, err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected %s to be empty, found %d entries", path, len(entries))
	}
}

func TestPrintConnections(t *testing.T) {
	cfg := config{
		Connections: []databaseConfig{
			{
				Name: "primary",
				Type: "postgres",
				Host: "db.internal",
				Port: 5432,
			},
			{
				Name:    "warehouse",
				Type:    "snowflake",
				Account: "acme-org",
			},
			{
				Name: "local",
				Type: "sqlite",
			},
		},
	}

	var out bytes.Buffer
	printConnections(&out, cfg)

	got := out.String()
	expectedLines := []string{
		"NAME\tTYPE\tHOST_URL",
		"primary\tpostgres\tdb.internal:5432",
		"warehouse\tsnowflake\thttps://acme-org.snowflakecomputing.com",
		"local\tsqlite\t-",
	}
	for _, line := range expectedLines {
		if !strings.Contains(got, line) {
			t.Fatalf("expected output to contain %q, got:\n%s", line, got)
		}
	}
}

func TestPrintConnectionsEmpty(t *testing.T) {
	var out bytes.Buffer

	printConnections(&out, config{})

	if got := out.String(); got != "No connections configured.\n" {
		t.Fatalf("printConnections(...) = %q, want %q", got, "No connections configured.\n")
	}
}

func TestConnectionHostURL(t *testing.T) {
	tests := []struct {
		name  string
		entry databaseConfig
		want  string
	}{
		{
			name: "postgres with host and port",
			entry: databaseConfig{
				Type: "postgres",
				Host: "localhost",
				Port: 5432,
			},
			want: "localhost:5432",
		},
		{
			name: "postgres with host only",
			entry: databaseConfig{
				Type: "postgres",
				Host: "localhost",
			},
			want: "localhost",
		},
		{
			name: "redshift with host and port",
			entry: databaseConfig{
				Type: "redshift",
				Host: "redshift-cluster.amazonaws.com",
				Port: 5439,
			},
			want: "redshift-cluster.amazonaws.com:5439",
		},
		{
			name: "snowflake from account",
			entry: databaseConfig{
				Type:    "snowflake",
				Account: "my-org",
			},
			want: "https://my-org.snowflakecomputing.com",
		},
		{
			name: "snowflake keeps explicit URL",
			entry: databaseConfig{
				Type:    "snowflake",
				Account: "https://my-org.snowflakecomputing.com",
			},
			want: "https://my-org.snowflakecomputing.com",
		},
		{
			name: "unknown type without host",
			entry: databaseConfig{
				Type: "sqlite",
			},
			want: "",
		},
		{
			name: "bigquery connection uri",
			entry: databaseConfig{
				Type:      "bigquery",
				ProjectID: "my-project",
				Schema:    "analytics",
			},
			want: "bigquery://my-project/analytics",
		},
		{
			name: "bigquery falls back to database field and default dataset marker",
			entry: databaseConfig{
				Type:     "bigquery",
				Database: "fallback-project",
			},
			want: "bigquery://fallback-project/_default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connectionHostURL(tt.entry)
			if got != tt.want {
				t.Fatalf("connectionHostURL(...) = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRequiresExplicitDatabaseSelection(t *testing.T) {
	tests := []struct {
		name         string
		databaseType string
		want         bool
	}{
		{name: "postgres", databaseType: "postgres", want: true},
		{name: "redshift", databaseType: "redshift", want: true},
		{name: "snowflake", databaseType: "snowflake", want: true},
		{name: "mysql", databaseType: "mysql", want: true},
		{name: "bigquery", databaseType: "bigquery", want: true},
		{name: "sqlite", databaseType: "sqlite", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requiresExplicitDatabaseSelection(tt.databaseType); got != tt.want {
				t.Fatalf("requiresExplicitDatabaseSelection(%q) = %v, want %v", tt.databaseType, got, tt.want)
			}
		})
	}
}

func TestResolveSQLiteDefaultDatabase(t *testing.T) {
	tests := []struct {
		name      string
		databases []string
		want      string
	}{
		{
			name:      "prefers main",
			databases: []string{"analytics", "main", "reporting"},
			want:      "main",
		},
		{
			name:      "falls back to first discovered",
			databases: []string{"analytics", "reporting"},
			want:      "analytics",
		},
		{
			name:      "empty list uses main",
			databases: nil,
			want:      "main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveSQLiteDefaultDatabase(tt.databases); got != tt.want {
				t.Fatalf("resolveSQLiteDefaultDatabase(%v) = %q, want %q", tt.databases, got, tt.want)
			}
		})
	}
}

func TestPingDatabaseSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "local.db")
	if err := pingDatabase(databaseConfig{
		Type:     "sqlite",
		Database: dbPath,
	}); err != nil {
		t.Fatalf("pingDatabase(sqlite) error = %v", err)
	}
}

func TestPingDatabaseSQLiteRequiresFilePath(t *testing.T) {
	err := pingDatabase(databaseConfig{
		Type: "sqlite",
	})
	if err == nil {
		t.Fatalf("pingDatabase(sqlite) error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "file path") {
		t.Fatalf("pingDatabase(sqlite) error = %q, want file path guidance", err)
	}
}

func TestSetPrimaryConnectionSwitchesPrimary(t *testing.T) {
	cfg := config{
		Connections: []databaseConfig{
			{Name: "alpha", Primary: true},
			{Name: "beta", Primary: false},
			{Name: "gamma", Primary: false},
		},
	}

	previous, changed, err := setPrimaryConnection(&cfg, "beta")
	if err != nil {
		t.Fatalf("setPrimaryConnection(...) error = %v", err)
	}
	if previous != "alpha" {
		t.Fatalf("previous primary = %q, want %q", previous, "alpha")
	}
	if !changed {
		t.Fatalf("changed = false, want true")
	}

	if cfg.Connections[0].Primary {
		t.Fatalf("alpha should no longer be primary")
	}
	if !cfg.Connections[1].Primary {
		t.Fatalf("beta should be primary")
	}
	if cfg.Connections[2].Primary {
		t.Fatalf("gamma should not be primary")
	}
}

func TestSetPrimaryConnectionAlreadyPrimaryNoChange(t *testing.T) {
	cfg := config{
		Connections: []databaseConfig{
			{Name: "alpha", Primary: true},
			{Name: "beta", Primary: false},
		},
	}

	previous, changed, err := setPrimaryConnection(&cfg, "alpha")
	if err != nil {
		t.Fatalf("setPrimaryConnection(...) error = %v", err)
	}
	if previous != "alpha" {
		t.Fatalf("previous primary = %q, want %q", previous, "alpha")
	}
	if changed {
		t.Fatalf("changed = true, want false")
	}

	if !cfg.Connections[0].Primary {
		t.Fatalf("alpha should remain primary")
	}
	if cfg.Connections[1].Primary {
		t.Fatalf("beta should remain non-primary")
	}
}

func TestSetPrimaryConnectionMissingConnection(t *testing.T) {
	cfg := config{
		Connections: []databaseConfig{
			{Name: "alpha", Primary: true},
			{Name: "beta", Primary: false},
		},
	}

	previous, changed, err := setPrimaryConnection(&cfg, "missing")
	if err == nil {
		t.Fatalf("setPrimaryConnection(...) error = nil, want non-nil")
	}
	if previous != "" {
		t.Fatalf("previous primary = %q, want empty", previous)
	}
	if changed {
		t.Fatalf("changed = true, want false")
	}

	if !cfg.Connections[0].Primary {
		t.Fatalf("alpha should remain primary after error")
	}
	if cfg.Connections[1].Primary {
		t.Fatalf("beta should remain non-primary after error")
	}
}

func TestResolveCurrentDefaultDatabase(t *testing.T) {
	tests := []struct {
		name          string
		configDefault string
		fileDefault   string
		want          string
	}{
		{
			name:          "config default wins",
			configDefault: "analytics",
			fileDefault:   "reporting",
			want:          "analytics",
		},
		{
			name:          "fallback to file default",
			configDefault: "",
			fileDefault:   "reporting",
			want:          "reporting",
		},
		{
			name:          "default marker treated as empty",
			configDefault: "",
			fileDefault:   "_default",
			want:          "",
		},
		{
			name:          "blank defaults return empty",
			configDefault: "   ",
			fileDefault:   "   ",
			want:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveCurrentDefaultDatabase(tt.configDefault, tt.fileDefault)
			if got != tt.want {
				t.Fatalf("resolveCurrentDefaultDatabase(%q, %q) = %q, want %q", tt.configDefault, tt.fileDefault, got, tt.want)
			}
		})
	}
}

func TestReadDatabasesCatalog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "_databases.yml")
	content := `# Header comment
connection: warehouse
database_type: postgres
default_database: reporting
generated_at: "2026-02-17T10:00:00Z"
databases:
  - name: reporting
  - name: analytics
  - name: reporting
  - name: "  "
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write databases file: %v", err)
	}

	got, err := readDatabasesCatalog(path)
	if err != nil {
		t.Fatalf("readDatabasesCatalog(...) error = %v", err)
	}

	if got.DatabaseType != "postgres" {
		t.Fatalf("DatabaseType = %q, want %q", got.DatabaseType, "postgres")
	}
	if got.DefaultDatabase != "reporting" {
		t.Fatalf("DefaultDatabase = %q, want %q", got.DefaultDatabase, "reporting")
	}

	wantDatabases := []string{"analytics", "reporting"}
	if !reflect.DeepEqual(got.Databases, wantDatabases) {
		t.Fatalf("Databases = %#v, want %#v", got.Databases, wantDatabases)
	}
}

func TestWriteDefaultDatabaseToDatabasesFile(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), ".dbharness")
	databasesDir := filepath.Join(baseDir, "context", "connections", "primary", "databases")
	if err := os.MkdirAll(databasesDir, 0o755); err != nil {
		t.Fatalf("mkdir databases dir: %v", err)
	}

	seedPath := filepath.Join(databasesDir, "_databases.yml")
	seedContent := `connection: primary
database_type: postgres
default_database: myapp
generated_at: "2026-02-16T00:00:00Z"
databases:
  - name: myapp
  - name: analytics
`
	if err := os.WriteFile(seedPath, []byte(seedContent), 0o644); err != nil {
		t.Fatalf("write seed databases file: %v", err)
	}

	if err := writeDefaultDatabaseToDatabasesFile(
		baseDir,
		"primary",
		"postgres",
		"analytics",
		[]string{"myapp", "analytics"},
	); err != nil {
		t.Fatalf("writeDefaultDatabaseToDatabasesFile(...) error = %v", err)
	}

	got, err := readDatabasesCatalog(seedPath)
	if err != nil {
		t.Fatalf("readDatabasesCatalog(...) error = %v", err)
	}

	if got.DefaultDatabase != "analytics" {
		t.Fatalf("DefaultDatabase = %q, want %q", got.DefaultDatabase, "analytics")
	}
	wantDatabases := []string{"analytics", "myapp"}
	if !reflect.DeepEqual(got.Databases, wantDatabases) {
		t.Fatalf("Databases = %#v, want %#v", got.Databases, wantDatabases)
	}
}

func TestBuildConnectionSelectionArgs(t *testing.T) {
	tests := []struct {
		name           string
		connectionName string
		want           []string
	}{
		{
			name:           "empty name",
			connectionName: "",
			want:           nil,
		},
		{
			name:           "whitespace name",
			connectionName: "   ",
			want:           nil,
		},
		{
			name:           "provided name",
			connectionName: "warehouse",
			want:           []string{"-s", "warehouse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildConnectionSelectionArgs(tt.connectionName)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("buildConnectionSelectionArgs(%q) = %#v, want %#v", tt.connectionName, got, tt.want)
			}
		})
	}
}

func TestRunSyncStagesContinuesAfterFailure(t *testing.T) {
	stages := []syncStage{
		{Name: "databases", Subcommand: "databases", Description: "stage one"},
		{Name: "schemas", Subcommand: "schemas", Description: "stage two"},
		{Name: "tables", Subcommand: "tables", Description: "stage three"},
	}

	type invocation struct {
		command string
		args    []string
	}
	var calls []invocation
	runner := func(command string, args []string) error {
		argsCopy := append([]string(nil), args...)
		calls = append(calls, invocation{command: command, args: argsCopy})
		if command == "schemas" {
			return errors.New("schema discovery failed")
		}
		return nil
	}

	var out bytes.Buffer
	results := runSyncStages(stages, []string{"-s", "warehouse"}, runner, &out)

	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("databases stage should succeed, got err %v", results[0].Err)
	}
	if results[1].Err == nil {
		t.Fatalf("schemas stage should fail")
	}
	if results[2].Err != nil {
		t.Fatalf("tables stage should still run and succeed, got err %v", results[2].Err)
	}

	wantCalls := []invocation{
		{command: "databases", args: []string{"-s", "warehouse"}},
		{command: "schemas", args: []string{"-s", "warehouse"}},
		{command: "tables", args: []string{"-s", "warehouse"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("sync stage invocations = %#v, want %#v", calls, wantCalls)
	}

	output := out.String()
	for _, expected := range []string{
		"[1/3] databases: stage one",
		"[2/3] schemas: stage two",
		"[3/3] tables: stage three",
		"schemas failed",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected output to contain %q, got:\n%s", expected, output)
		}
	}
}

func TestPrintSyncSummary(t *testing.T) {
	results := []syncStageResult{
		{StageName: "databases", Duration: 250 * time.Millisecond},
		{StageName: "schemas", Duration: 500 * time.Millisecond, Err: errors.New("boom")},
		{StageName: "tables", Duration: time.Second},
	}

	var out bytes.Buffer
	failed := printSyncSummary(results, &out)
	if failed != 1 {
		t.Fatalf("printSyncSummary(...) failed count = %d, want 1", failed)
	}

	output := out.String()
	for _, expected := range []string{
		"Sync summary:",
		"  - databases: ok (250ms)",
		"  - schemas: failed (500ms)",
		"  - tables: ok (1s)",
		"Sync finished with 1 failed stage(s).",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected output to contain %q, got:\n%s", expected, output)
		}
	}
}
