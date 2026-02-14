// Package discovery provides database schema and table introspection
// for supported database types (Postgres, Snowflake).
package discovery

import (
	"context"
	"database/sql"
	"fmt"
)

// SchemaInfo holds metadata about a single database schema.
type SchemaInfo struct {
	Name   string
	Tables []TableInfo
}

// TableInfo holds metadata about a single table or view within a schema.
type TableInfo struct {
	Name      string
	TableType string // e.g. "BASE TABLE", "VIEW"
}

// Discoverer retrieves schema and table metadata from a database.
type Discoverer interface {
	// Discover returns all non-system schemas with their tables.
	Discover(ctx context.Context) ([]SchemaInfo, error)
	// Close releases the underlying database connection.
	Close() error
}

// DatabaseLister retrieves the list of databases available in a connection.
type DatabaseLister interface {
	// ListDatabases returns the names of all databases accessible to the
	// current role.
	ListDatabases(ctx context.Context) ([]string, error)
	// Close releases the underlying database connection.
	Close() error
}

// DatabaseConfig holds the connection parameters needed by Discoverer
// implementations. It mirrors the fields from the CLI config.
type DatabaseConfig struct {
	Type     string
	Database string

	// Postgres
	Host     string
	Port     int
	User     string
	Password string
	SSLMode  string

	// Snowflake
	Account       string
	Role          string
	Warehouse     string
	Schema        string
	Authenticator string
}

// New creates a Discoverer for the given database configuration.
func New(cfg DatabaseConfig) (Discoverer, error) {
	switch cfg.Type {
	case "postgres":
		return newPostgres(cfg)
	case "snowflake":
		return newSnowflake(cfg)
	default:
		return nil, fmt.Errorf("unsupported database type %q", cfg.Type)
	}
}

// NewDatabaseLister creates a DatabaseLister for the given database
// configuration. Only Snowflake is supported at this time.
func NewDatabaseLister(cfg DatabaseConfig) (DatabaseLister, error) {
	switch cfg.Type {
	case "snowflake":
		return newSnowflakeDatabaseLister(cfg)
	default:
		return nil, fmt.Errorf("update-databases is not supported for connection type %q at this time (only snowflake validated)", cfg.Type)
	}
}

// openDB is a small helper that opens and pings a database connection.
func openDB(driverName, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s connection: %w", driverName, err)
	}
	return db, nil
}
