// Package contextgen generates LLM-friendly YAML context files from
// database schema metadata. It creates a nested folder structure that
// is easy for AI coding agents to crawl and discover database objects.
package contextgen

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/genesisdayrit/dbharness/internal/discovery"
	"gopkg.in/yaml.v3"
)

// --------------------------------------------------------------------------
// YAML document types
// --------------------------------------------------------------------------

// SchemasFile is the top-level schemas.yml that gives an LLM a quick
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

// TablesFile is written inside each schemas/<name>/tables.yml and provides
// a detailed listing of every table or view in that schema.
type TablesFile struct {
	Schema       string         `yaml:"schema"`
	Connection   string         `yaml:"connection"`
	Database     string         `yaml:"database"`
	DatabaseType string         `yaml:"database_type"`
	GeneratedAt  string         `yaml:"generated_at"`
	Tables       []TablesEntry  `yaml:"tables"`
}

// TablesEntry is one row in a tables.yml file.
type TablesEntry struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"` // BASE TABLE, VIEW, etc.
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

	contextDir := filepath.Join(opts.BaseDir, "context", opts.ConnectionName)
	if err := os.MkdirAll(contextDir, 0o755); err != nil {
		return fmt.Errorf("create context dir: %w", err)
	}

	// ---- schemas.yml ----
	sf := SchemasFile{
		Connection:   opts.ConnectionName,
		Database:     opts.DatabaseName,
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

	schemasPath := filepath.Join(contextDir, "schemas.yml")
	if err := writeYAMLWithHeader(schemasPath, sf, schemasHeader(opts)); err != nil {
		return fmt.Errorf("write schemas.yml: %w", err)
	}

	// ---- per-schema tables.yml files ----
	for _, s := range schemas {
		schemaDir := filepath.Join(contextDir, "schemas", sanitizeName(s.Name))
		if err := os.MkdirAll(schemaDir, 0o755); err != nil {
			return fmt.Errorf("create schema dir %q: %w", s.Name, err)
		}

		tf := TablesFile{
			Schema:       s.Name,
			Connection:   opts.ConnectionName,
			Database:     opts.DatabaseName,
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

		tablesPath := filepath.Join(schemaDir, "tables.yml")
		if err := writeYAMLWithHeader(tablesPath, tf, tablesHeader(opts, s.Name)); err != nil {
			return fmt.Errorf("write tables.yml for %q: %w", s.Name, err)
		}
	}

	return nil
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

func schemasHeader(opts Options) string {
	return fmt.Sprintf(`# =============================================================================
# Database Schema Context
# Connection: %s | Database: %s | Type: %s
# =============================================================================
#
# This file was generated by dbharness to provide LLM-friendly database context.
#
# Structure:
#   schemas.yml (this file)                  - Overview of all schemas
#   schemas/<schema_name>/tables.yml         - Tables within each schema
#
# To explore a specific schema, navigate into the schemas/ directory.
# Each schema directory contains a tables.yml with its table listing.
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
