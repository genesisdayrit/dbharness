// Package contextgen generates LLM-friendly YAML context files from
// database schema metadata. It creates a nested folder structure that
// is easy for AI coding agents to crawl and discover database objects.
package contextgen

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/genesisdayrit/dbharness/internal/discovery"
	"gopkg.in/yaml.v3"
)

// --------------------------------------------------------------------------
// YAML document types
// --------------------------------------------------------------------------

// DatabasesFile is the top-level _databases.yml that lists databases
// available under a connection.
type DatabasesFile struct {
	Connection      string         `yaml:"connection"`
	DatabaseType    string         `yaml:"database_type"`
	DefaultDatabase string         `yaml:"default_database"`
	GeneratedAt     string         `yaml:"generated_at"`
	Databases       []DatabaseItem `yaml:"databases"`
}

// DatabaseItem is one entry in the _databases.yml file.
type DatabaseItem struct {
	Name string `yaml:"name"`
}

// SchemasFile is the _schemas.yml that gives an LLM a quick
// overview of every schema in the database.
type SchemasFile struct {
	Connection   string       `yaml:"connection"`
	Database     string       `yaml:"database"`
	DatabaseType string       `yaml:"database_type"`
	GeneratedAt  string       `yaml:"generated_at"`
	Schemas      []SchemaItem `yaml:"schemas"`
}

// SchemaItem is one entry in the top-level schemas.yml.
type SchemaItem struct {
	Name        string   `yaml:"name"`
	TableCount  int      `yaml:"table_count"`
	ViewCount   int      `yaml:"view_count"`
	Description string   `yaml:"description"` // blank; placeholder for LLM-generated descriptions
	Tables      []string `yaml:"tables"`
}

// TablesFile is written inside each <schema>/_tables.yml and provides
// a detailed listing of every table or view in that schema.
type TablesFile struct {
	Schema       string        `yaml:"schema"`
	Connection   string        `yaml:"connection"`
	Database     string        `yaml:"database"`
	DatabaseType string        `yaml:"database_type"`
	GeneratedAt  string        `yaml:"generated_at"`
	Tables       []TablesEntry `yaml:"tables"`
}

// TablesEntry is one row in a tables.yml file.
type TablesEntry struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`        // BASE TABLE, VIEW, etc.
	Description string `yaml:"description"` // blank placeholder
}

// --------------------------------------------------------------------------
// Generator
// --------------------------------------------------------------------------

// Options configures the context generation.
type Options struct {
	ConnectionName string
	DatabaseName   string
	DatabaseType   string
	BaseDir        string // e.g. ".dbharness"
}

// Generate writes the full context directory tree for the given schemas.
func Generate(schemas []discovery.SchemaInfo, opts Options) error {
	now := time.Now().UTC().Format(time.RFC3339)

	defaultDatabase := resolveDefaultDatabase(opts.DatabaseName, nil)
	dbName := sanitizeName(defaultDatabase)
	headerOpts := opts
	headerOpts.DatabaseName = defaultDatabase

	databasesDir := filepath.Join(opts.BaseDir, "context", "connections", opts.ConnectionName, "databases")
	if err := os.MkdirAll(databasesDir, 0o755); err != nil {
		return fmt.Errorf("create databases dir: %w", err)
	}

	// ---- _databases.yml ----
	df := DatabasesFile{
		Connection:      opts.ConnectionName,
		DatabaseType:    opts.DatabaseType,
		DefaultDatabase: defaultDatabase,
		GeneratedAt:     now,
		Databases:       []DatabaseItem{{Name: defaultDatabase}},
	}

	databasesPath := filepath.Join(databasesDir, "_databases.yml")
	if err := writeYAMLWithHeader(databasesPath, df, databasesHeader(headerOpts)); err != nil {
		return fmt.Errorf("write _databases.yml: %w", err)
	}

	schemasDir := filepath.Join(databasesDir, dbName, "schemas")
	if err := os.MkdirAll(schemasDir, 0o755); err != nil {
		return fmt.Errorf("create schemas dir: %w", err)
	}

	// ---- _schemas.yml ----
	sf := SchemasFile{
		Connection:   opts.ConnectionName,
		Database:     defaultDatabase,
		DatabaseType: opts.DatabaseType,
		GeneratedAt:  now,
	}

	for _, s := range schemas {
		item := SchemaItem{
			Name:        s.Name,
			Description: "",
		}
		for _, t := range s.Tables {
			item.Tables = append(item.Tables, t.Name)
			switch {
			case isView(t.TableType):
				item.ViewCount++
			default:
				item.TableCount++
			}
		}
		sf.Schemas = append(sf.Schemas, item)
	}

	schemasPath := filepath.Join(schemasDir, "_schemas.yml")
	if err := writeYAMLWithHeader(schemasPath, sf, schemasHeader(headerOpts)); err != nil {
		return fmt.Errorf("write _schemas.yml: %w", err)
	}

	// ---- per-schema _tables.yml files ----
	for _, s := range schemas {
		schemaDir := filepath.Join(schemasDir, sanitizeName(s.Name))
		if err := os.MkdirAll(schemaDir, 0o755); err != nil {
			return fmt.Errorf("create schema dir %q: %w", s.Name, err)
		}

		tf := TablesFile{
			Schema:       s.Name,
			Connection:   opts.ConnectionName,
			Database:     defaultDatabase,
			DatabaseType: opts.DatabaseType,
			GeneratedAt:  now,
		}
		for _, t := range s.Tables {
			tf.Tables = append(tf.Tables, TablesEntry{
				Name:        t.Name,
				Type:        t.TableType,
				Description: "",
			})
		}

		tablesPath := filepath.Join(schemaDir, "_tables.yml")
		if err := writeYAMLWithHeader(tablesPath, tf, tablesHeader(headerOpts, s.Name)); err != nil {
			return fmt.Errorf("write _tables.yml for %q: %w", s.Name, err)
		}
	}

	return nil
}

// UpdateDatabasesFile writes or merges a _databases.yml file for the given
// connection. If the file already exists, newly discovered databases are
// appended while existing entries are preserved. If the file does not exist,
// it is created with all discovered databases.
func UpdateDatabasesFile(discoveredDBs []string, opts Options) (added []string, err error) {
	now := time.Now().UTC().Format(time.RFC3339)

	databasesDir := filepath.Join(opts.BaseDir, "context", "connections", opts.ConnectionName, "databases")
	if err := os.MkdirAll(databasesDir, 0o755); err != nil {
		return nil, fmt.Errorf("create databases dir: %w", err)
	}

	databasesPath := filepath.Join(databasesDir, "_databases.yml")

	// Try to read the existing file.
	var existing DatabasesFile
	existingData, readErr := os.ReadFile(databasesPath)
	fileExists := readErr == nil

	if fileExists {
		if err := yaml.Unmarshal(existingData, &existing); err != nil {
			return nil, fmt.Errorf("parse existing _databases.yml: %w", err)
		}
	}

	// Build a set of existing database names for fast lookup.
	existingSet := make(map[string]bool, len(existing.Databases))
	for _, db := range existing.Databases {
		existingSet[db.Name] = true
	}

	// Determine which databases are new.
	var newDBs []string
	for _, name := range discoveredDBs {
		if !existingSet[name] {
			newDBs = append(newDBs, name)
		}
	}

	// Build the merged list: existing entries first (preserving order), then
	// new entries sorted alphabetically.
	merged := make([]DatabaseItem, len(existing.Databases))
	copy(merged, existing.Databases)

	sort.Strings(newDBs)
	for _, name := range newDBs {
		merged = append(merged, DatabaseItem{Name: name})
	}

	defaultDatabase := resolveDefaultDatabase(opts.DatabaseName, merged)

	df := DatabasesFile{
		Connection:      opts.ConnectionName,
		DatabaseType:    opts.DatabaseType,
		DefaultDatabase: defaultDatabase,
		GeneratedAt:     now,
		Databases:       merged,
	}

	if err := writeYAMLWithHeader(databasesPath, df, databasesHeader(opts)); err != nil {
		return nil, fmt.Errorf("write _databases.yml: %w", err)
	}

	return newDBs, nil
}

// --------------------------------------------------------------------------
// helpers
// --------------------------------------------------------------------------

func writeYAMLWithHeader(path string, v interface{}, header string) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}

	var buf strings.Builder
	buf.WriteString(header)
	buf.Write(data)

	return os.WriteFile(path, []byte(buf.String()), 0o644)
}

func databasesHeader(opts Options) string {
	return fmt.Sprintf(`# =============================================================================
# Databases for connection: %s
# Connection: %s | Type: %s
# =============================================================================
#
# This file was generated by dbh to provide LLM-friendly database context.
#
# Structure:
#   _databases.yml (this file)               - List of databases in this connection
#   <database>/schemas/_schemas.yml           - Schemas within each database
#   <database>/schemas/<schema>/_tables.yml   - Tables within each schema
#
# To explore a database, navigate into its directory.
# =============================================================================

`, opts.ConnectionName, opts.ConnectionName, opts.DatabaseType)
}

func schemasHeader(opts Options) string {
	return fmt.Sprintf(`# =============================================================================
# Database Schema Context
# Connection: %s | Database: %s | Type: %s
# =============================================================================
#
# This file was generated by dbh to provide LLM-friendly database context.
#
# Structure:
#   _schemas.yml (this file)                 - Overview of all schemas
#   <schema_name>/_tables.yml                - Tables within each schema
#
# To explore a specific schema, navigate into the schema subdirectory.
# Each schema directory contains a _tables.yml with its table listing.
#
# The "description" fields are empty by default. They can be populated
# with LLM-generated or human-written descriptions for richer context.
# =============================================================================

`, opts.ConnectionName, opts.DatabaseName, opts.DatabaseType)
}

func tablesHeader(opts Options, schemaName string) string {
	return fmt.Sprintf(`# =============================================================================
# Tables in schema: %s
# Connection: %s | Database: %s | Type: %s
# =============================================================================
#
# This file lists all tables and views in the "%s" schema.
#
# The "description" fields are empty by default. They can be populated
# with LLM-generated or human-written descriptions for richer context.
# =============================================================================

`, schemaName, opts.ConnectionName, opts.DatabaseName, opts.DatabaseType, schemaName)
}

func isView(tableType string) bool {
	upper := strings.ToUpper(tableType)
	return strings.Contains(upper, "VIEW")
}

// resolveDefaultDatabase ensures default_database in _databases.yml is never blank.
// The configured value always wins. If there is exactly one database entry,
// that single entry is used as a fallback. Otherwise "_default" is used.
func resolveDefaultDatabase(configured string, databases []DatabaseItem) string {
	if name := strings.TrimSpace(configured); name != "" {
		return name
	}
	if len(databases) == 1 {
		if name := strings.TrimSpace(databases[0].Name); name != "" {
			return name
		}
	}
	return "_default"
}

// sanitizeName replaces characters that are not safe for directory names.
func sanitizeName(name string) string {
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		" ", "_",
		".", "_",
	)
	return strings.ToLower(replacer.Replace(name))
}
