// Package contextgen generates LLM-friendly YAML and XML context files from
// database schema metadata. It creates a nested folder structure that
// is easy for AI coding agents to crawl and discover database objects.
package contextgen

import (
	"encoding/xml"
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
	Name          string            `yaml:"name"`
	TableCount    int               `yaml:"table_count"`
	ViewCount     int               `yaml:"view_count"`
	AIDescription string            `yaml:"ai_description"` // blank; placeholder for AI-generated descriptions
	DBDescription string            `yaml:"db_description"` // DB-native description/comment (if available)
	Tables        []SchemaTableItem `yaml:"tables"`
}

// SchemaTableItem is one table/view entry in a schema listing.
type SchemaTableItem struct {
	Name          string `yaml:"name"`
	Type          string `yaml:"type"`
	AIDescription string `yaml:"ai_description"` // blank; placeholder for AI-generated descriptions
	DBDescription string `yaml:"db_description"` // DB-native description/comment (if available)
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
	Name          string `yaml:"name"`
	Type          string `yaml:"type"` // BASE TABLE, VIEW, etc.
	AIDescription string `yaml:"ai_description"`
	DBDescription string `yaml:"db_description"`
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

	defaultDatabase, err := resolveGenerationDatabase(opts)
	if err != nil {
		return err
	}

	sortedSchemas := sortedSchemaInfos(schemas)

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

	for _, s := range sortedSchemas {
		item := SchemaItem{
			Name:          s.Name,
			AIDescription: "",
			DBDescription: "",
		}
		for _, t := range s.Tables {
			item.Tables = append(item.Tables, SchemaTableItem{
				Name:          t.Name,
				Type:          t.TableType,
				AIDescription: "",
				DBDescription: "",
			})
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
	for _, s := range sortedSchemas {
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
				Name:          t.Name,
				Type:          t.TableType,
				AIDescription: "",
				DBDescription: "",
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
// Table detail YAML/XML types
// --------------------------------------------------------------------------

// ColumnsFile is written as <table_name>__columns.yml inside each table directory.
type ColumnsFile struct {
	Schema       string            `yaml:"schema"`
	Table        string            `yaml:"table"`
	Connection   string            `yaml:"connection"`
	Database     string            `yaml:"database"`
	DatabaseType string            `yaml:"database_type"`
	GeneratedAt  string            `yaml:"generated_at"`
	Columns      []ColumnsFileItem `yaml:"columns"`
}

// ColumnsFileItem is one column entry in a columns YAML file.
type ColumnsFileItem struct {
	Name            string `yaml:"name"`
	DataType        string `yaml:"data_type"`
	IsNullable      string `yaml:"is_nullable"`
	OrdinalPosition int    `yaml:"ordinal_position"`
	ColumnDefault   string `yaml:"column_default,omitempty"`
}

// EnrichedColumnsFile is written as <table_name>__columns.yml when using
// the dbh columns command.
type EnrichedColumnsFile struct {
	Schema       string                    `yaml:"schema"`
	Table        string                    `yaml:"table"`
	Connection   string                    `yaml:"connection"`
	Database     string                    `yaml:"database"`
	DatabaseType string                    `yaml:"database_type"`
	GeneratedAt  string                    `yaml:"generated_at"`
	Columns      []EnrichedColumnsFileItem `yaml:"columns"`
}

// EnrichedColumnsFileItem is one enriched column profile entry.
type EnrichedColumnsFileItem struct {
	Name                  string   `yaml:"name"`
	DataType              string   `yaml:"data_type"`
	IsNullable            string   `yaml:"is_nullable"`
	OrdinalPosition       int      `yaml:"ordinal_position"`
	ColumnDefault         string   `yaml:"column_default,omitempty"`
	AIDescription         string   `yaml:"ai_description"`
	DBDescription         string   `yaml:"db_description"`
	TotalRows             int64    `yaml:"total_rows"`
	NullCount             int64    `yaml:"null_count"`
	NonNullCount          int64    `yaml:"non_null_count"`
	DistinctNonNullCount  int64    `yaml:"distinct_non_null_count"`
	DistinctOfNonNullPct  float64  `yaml:"distinct_of_non_null_pct"`
	NullOfTotalRowsPct    float64  `yaml:"null_of_total_rows_pct"`
	NonNullOfTotalRowsPct float64  `yaml:"non_null_of_total_rows_pct"`
	SampleValues          []string `yaml:"sample_values,omitempty"`
}

// EnrichedColumnsInput holds all enriched columns for one table.
type EnrichedColumnsInput struct {
	Schema  string
	Table   string
	Columns []discovery.EnrichedColumnInfo
}

// SampleXML is the root element for <table_name>__sample.xml files.
type SampleXML struct {
	XMLName     xml.Name       `xml:"table_sample"`
	Schema      string         `xml:"schema,attr"`
	Table       string         `xml:"table,attr"`
	Connection  string         `xml:"connection,attr"`
	Database    string         `xml:"database,attr"`
	RowCount    int            `xml:"row_count,attr"`
	GeneratedAt string         `xml:"generated_at,attr"`
	Rows        []SampleRowXML `xml:"row"`
}

// SampleRowXML is a single row in the sample XML.
type SampleRowXML struct {
	Fields []SampleFieldXML `xml:"field"`
}

// SampleFieldXML is a single field (column value) in a sample row.
type SampleFieldXML struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

// TableDetailInput holds the data needed to generate per-table detail files.
type TableDetailInput struct {
	Schema  string
	Table   string
	Columns []discovery.ColumnInfo
	Sample  *discovery.SampleResult
}

// GenerateTableDetails writes per-table __columns.yml and __sample.xml files
// for the given tables. Files are placed in the directory structure:
//
//	<baseDir>/context/connections/<conn>/databases/<db>/schemas/<schema>/<table>/
func GenerateTableDetails(tables []TableDetailInput, opts Options) error {
	now := time.Now().UTC().Format(time.RFC3339)

	defaultDatabase, err := resolveGenerationDatabase(opts)
	if err != nil {
		return err
	}

	dbName := sanitizeName(defaultDatabase)
	schemasDir := filepath.Join(opts.BaseDir, "context", "connections", opts.ConnectionName, "databases", dbName, "schemas")

	for _, td := range tables {
		schemaDir := sanitizeName(td.Schema)
		tableDir := sanitizeName(td.Table)
		dir := filepath.Join(schemasDir, schemaDir, tableDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create table dir %q/%q: %w", td.Schema, td.Table, err)
		}

		// Write __columns.yml
		if td.Columns != nil {
			cf := ColumnsFile{
				Schema:       td.Schema,
				Table:        td.Table,
				Connection:   opts.ConnectionName,
				Database:     defaultDatabase,
				DatabaseType: opts.DatabaseType,
				GeneratedAt:  now,
			}
			for _, c := range td.Columns {
				cf.Columns = append(cf.Columns, ColumnsFileItem{
					Name:            c.Name,
					DataType:        c.DataType,
					IsNullable:      c.IsNullable,
					OrdinalPosition: c.OrdinalPosition,
					ColumnDefault:   c.ColumnDefault,
				})
			}

			colFileName := sanitizeName(td.Table) + "__columns.yml"
			colPath := filepath.Join(dir, colFileName)
			header := columnsHeader(opts, defaultDatabase, td.Schema, td.Table)
			if err := writeYAMLWithHeader(colPath, cf, header); err != nil {
				return fmt.Errorf("write columns for %q.%q: %w", td.Schema, td.Table, err)
			}
		}

		// Write __sample.xml
		if td.Sample != nil && len(td.Sample.Rows) > 0 {
			sx := SampleXML{
				Schema:      td.Schema,
				Table:       td.Table,
				Connection:  opts.ConnectionName,
				Database:    defaultDatabase,
				RowCount:    len(td.Sample.Rows),
				GeneratedAt: now,
			}
			for _, row := range td.Sample.Rows {
				srow := SampleRowXML{}
				for ci, col := range td.Sample.Columns {
					val := ""
					if ci < len(row) {
						val = row[ci]
					}
					srow.Fields = append(srow.Fields, SampleFieldXML{
						Name:  col,
						Value: val,
					})
				}
				sx.Rows = append(sx.Rows, srow)
			}

			sampleFileName := sanitizeName(td.Table) + "__sample.xml"
			samplePath := filepath.Join(dir, sampleFileName)
			if err := writeXML(samplePath, sx); err != nil {
				return fmt.Errorf("write sample for %q.%q: %w", td.Schema, td.Table, err)
			}
		}
	}

	return nil
}

// WriteEnrichedColumnsFile writes one enriched <table_name>__columns.yml file.
// The file is written atomically so existing files are only replaced after the
// full payload has been successfully serialized.
func WriteEnrichedColumnsFile(input EnrichedColumnsInput, opts Options) (string, error) {
	if strings.TrimSpace(input.Schema) == "" {
		return "", fmt.Errorf("schema is required for enriched columns")
	}
	if strings.TrimSpace(input.Table) == "" {
		return "", fmt.Errorf("table is required for enriched columns")
	}
	if len(input.Columns) == 0 {
		return "", fmt.Errorf("no columns provided for %s.%s", input.Schema, input.Table)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	defaultDatabase, err := resolveGenerationDatabase(opts)
	if err != nil {
		return "", err
	}

	dbName := sanitizeName(defaultDatabase)
	schemaDir := sanitizeName(input.Schema)
	tableDir := sanitizeName(input.Table)
	dir := filepath.Join(
		opts.BaseDir,
		"context",
		"connections",
		opts.ConnectionName,
		"databases",
		dbName,
		"schemas",
		schemaDir,
		tableDir,
	)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create table dir %q/%q: %w", input.Schema, input.Table, err)
	}

	file := EnrichedColumnsFile{
		Schema:       input.Schema,
		Table:        input.Table,
		Connection:   opts.ConnectionName,
		Database:     defaultDatabase,
		DatabaseType: opts.DatabaseType,
		GeneratedAt:  now,
	}

	for _, column := range input.Columns {
		file.Columns = append(file.Columns, EnrichedColumnsFileItem{
			Name:                  column.Name,
			DataType:              column.DataType,
			IsNullable:            column.IsNullable,
			OrdinalPosition:       column.OrdinalPosition,
			ColumnDefault:         column.ColumnDefault,
			AIDescription:         column.AIDescription,
			DBDescription:         column.DBDescription,
			TotalRows:             column.TotalRows,
			NullCount:             column.NullCount,
			NonNullCount:          column.NonNullCount,
			DistinctNonNullCount:  column.DistinctNonNullCount,
			DistinctOfNonNullPct:  column.DistinctOfNonNullPct,
			NullOfTotalRowsPct:    column.NullOfTotalRowsPct,
			NonNullOfTotalRowsPct: column.NonNullOfTotalRowsPct,
			SampleValues:          column.SampleValues,
		})
	}

	colFileName := sanitizeName(input.Table) + "__columns.yml"
	colPath := filepath.Join(dir, colFileName)
	header := enrichedColumnsHeader(opts, defaultDatabase, input.Schema, input.Table)
	if err := writeYAMLWithHeaderAtomic(colPath, file, header); err != nil {
		return "", fmt.Errorf("write enriched columns for %q.%q: %w", input.Schema, input.Table, err)
	}

	return colPath, nil
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

func writeYAMLWithHeaderAtomic(path string, v interface{}, header string) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}

	var buf strings.Builder
	buf.WriteString(header)
	buf.Write(data)

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(buf.String()), 0o644); err != nil {
		return fmt.Errorf("write temp yaml: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp yaml: %w", err)
	}

	return nil
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
# Description fields:
#   ai_description - Intended for AI-authored descriptions.
#   db_description - Intended for database-native descriptions/comments.
# Both are empty when no description data is available.
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
# Description fields:
#   ai_description - Intended for AI-authored descriptions.
#   db_description - Intended for database-native descriptions/comments.
# Both are empty when no description data is available.
# =============================================================================

`, schemaName, opts.ConnectionName, opts.DatabaseName, opts.DatabaseType, schemaName)
}

func writeXML(path string, v interface{}) error {
	data, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal xml: %w", err)
	}

	var buf strings.Builder
	buf.WriteString(xml.Header)
	buf.Write(data)
	buf.WriteString("\n")

	return os.WriteFile(path, []byte(buf.String()), 0o644)
}

func columnsHeader(opts Options, database, schema, table string) string {
	return fmt.Sprintf(`# =============================================================================
# Columns for table: %s.%s
# Connection: %s | Database: %s | Type: %s
# =============================================================================
#
# This file was generated by dbh to provide LLM-friendly table column context.
#
# Column fields:
#   name             - Column name
#   data_type        - Database data type
#   is_nullable      - Whether the column allows NULL values (YES/NO)
#   ordinal_position - Column position in the table
#   column_default   - Default value expression (if any)
# =============================================================================

`, schema, table, opts.ConnectionName, database, opts.DatabaseType)
}

func enrichedColumnsHeader(opts Options, database, schema, table string) string {
	return fmt.Sprintf(`# =============================================================================
# Enriched columns for table: %s.%s
# Connection: %s | Database: %s | Type: %s
# =============================================================================
#
# This file was generated by dbh columns to provide enriched per-column context.
#
# Column fields:
#   name                       - Column name
#   data_type                  - Database data type
#   is_nullable                - Whether NULL is allowed (YES/NO)
#   ordinal_position           - Column position in the table
#   column_default             - Default expression (if any)
#   ai_description             - Blank placeholder for future AI descriptions
#   db_description             - Database-native description/comment (if available)
#   total_rows                 - Total rows in table at profiling time
#   null_count                 - Rows where this column is NULL
#   non_null_count             - Rows where this column is NOT NULL
#   distinct_non_null_count    - Distinct non-NULL values
#   distinct_of_non_null_pct   - distinct_non_null_count / non_null_count * 100
#   null_of_total_rows_pct     - null_count / total_rows * 100
#   non_null_of_total_rows_pct - non_null_count / total_rows * 100
#   sample_values              - Up to 5 truncated example values
# =============================================================================

`, schema, table, opts.ConnectionName, database, opts.DatabaseType)
}

func isView(tableType string) bool {
	upper := strings.ToUpper(tableType)
	return strings.Contains(upper, "VIEW")
}

func resolveGenerationDatabase(opts Options) (string, error) {
	configured := strings.TrimSpace(opts.DatabaseName)
	if configured != "" && !strings.EqualFold(configured, "_default") {
		return configured, nil
	}

	if requiresExplicitDefaultDatabase(opts.DatabaseType) {
		return "", fmt.Errorf(
			`no default database configured for connection %q (%s): select a database from this connection and set it as the default`,
			opts.ConnectionName,
			opts.DatabaseType,
		)
	}

	return resolveDefaultDatabase(configured, nil), nil
}

func requiresExplicitDefaultDatabase(databaseType string) bool {
	switch strings.ToLower(strings.TrimSpace(databaseType)) {
	case "postgres", "snowflake":
		return true
	default:
		return false
	}
}

func sortedSchemaInfos(schemas []discovery.SchemaInfo) []discovery.SchemaInfo {
	sorted := make([]discovery.SchemaInfo, len(schemas))
	for i := range schemas {
		sorted[i] = discovery.SchemaInfo{
			Name:   schemas[i].Name,
			Tables: append([]discovery.TableInfo(nil), schemas[i].Tables...),
		}
		sort.Slice(sorted[i].Tables, func(a, b int) bool {
			return sorted[i].Tables[a].Name < sorted[i].Tables[b].Name
		})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	return sorted
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
