// Package discovery provides database schema and table introspection
// for supported database types (Postgres, Redshift, Snowflake, MySQL, BigQuery, SQLite).
package discovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
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

// ColumnInfo holds metadata about a single column in a table.
type ColumnInfo struct {
	Name            string
	DataType        string
	IsNullable      string // "YES" or "NO"
	OrdinalPosition int
	ColumnDefault   string
}

// EnrichedColumnInfo holds detailed profiling metadata about a column.
// It extends basic information_schema metadata with table-level statistics
// and compact sample values suitable for YAML context files.
type EnrichedColumnInfo struct {
	Name            string
	DataType        string
	IsNullable      string
	OrdinalPosition int
	ColumnDefault   string
	AIDescription   string
	DBDescription   string

	TotalRows             int64
	NullCount             int64
	NonNullCount          int64
	DistinctNonNullCount  int64
	DistinctOfNonNullPct  float64
	NullOfTotalRowsPct    float64
	NonNullOfTotalRowsPct float64
	SampleValues          []string
}

// SampleResult holds the column headers and row data from a sample query.
type SampleResult struct {
	Columns []string
	Rows    [][]string
}

// Discoverer retrieves schema and table metadata from a database.
type Discoverer interface {
	// Discover returns all non-system schemas with their tables.
	Discover(ctx context.Context) ([]SchemaInfo, error)
	// Close releases the underlying database connection.
	Close() error
}

// TableDetailDiscoverer extends Discoverer with column and sample data
// retrieval for individual tables.
type TableDetailDiscoverer interface {
	Discoverer
	// GetColumns returns column metadata for the given schema and table.
	GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error)
	// GetColumnEnrichment returns detailed profile stats for a single column.
	GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error)
	// GetSampleRows returns a random sample of rows from the given table.
	GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error)
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
	TLS      string

	// Snowflake
	Account       string
	Role          string
	Warehouse     string
	Schema        string
	Authenticator string

	// BigQuery
	ProjectID       string
	CredentialsFile string

	// SQLite
	// Database is the SQLite file path.
}

// New creates a Discoverer for the given database configuration.
func New(cfg DatabaseConfig) (Discoverer, error) {
	switch cfg.Type {
	case "postgres":
		return newPostgres(cfg)
	case "redshift":
		return newRedshift(cfg)
	case "snowflake":
		return newSnowflake(cfg)
	case "mysql":
		return newMySQL(cfg)
	case "bigquery":
		return newBigQuery(cfg)
	case "sqlite":
		return newSQLite(cfg)
	default:
		return nil, fmt.Errorf("unsupported database type %q", cfg.Type)
	}
}

// NewTableDetailDiscoverer creates a TableDetailDiscoverer for the given
// database configuration. It provides schema discovery plus column metadata
// and sample data retrieval.
func NewTableDetailDiscoverer(cfg DatabaseConfig) (TableDetailDiscoverer, error) {
	switch cfg.Type {
	case "postgres":
		return newPostgres(cfg)
	case "redshift":
		return newRedshift(cfg)
	case "snowflake":
		return newSnowflake(cfg)
	case "mysql":
		return newMySQL(cfg)
	case "bigquery":
		return newBigQuery(cfg)
	case "sqlite":
		return newSQLite(cfg)
	default:
		return nil, fmt.Errorf("unsupported database type %q for table detail discovery", cfg.Type)
	}
}

// NewDatabaseLister creates a DatabaseLister for the given database
// configuration.
func NewDatabaseLister(cfg DatabaseConfig) (DatabaseLister, error) {
	switch cfg.Type {
	case "postgres":
		return newPostgresDatabaseLister(cfg)
	case "redshift":
		return newRedshiftDatabaseLister(cfg)
	case "snowflake":
		return newSnowflakeDatabaseLister(cfg)
	case "mysql":
		return newMySQLDatabaseLister(cfg)
	case "bigquery":
		return newBigQueryDatabaseLister(cfg)
	case "sqlite":
		return newSQLiteDatabaseLister(cfg)
	default:
		return nil, fmt.Errorf("databases discovery is not supported for connection type %q", cfg.Type)
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

// scanSampleRows reads all rows from a *sql.Rows result set and returns
// the column names and string-formatted cell values. NULL values are
// represented as empty strings.
func scanSampleRows(rows *sql.Rows) (*SampleResult, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get column names: %w", err)
	}

	result := &SampleResult{Columns: cols}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan sample row: %w", err)
		}

		row := make([]string, len(cols))
		for i, v := range values {
			row[i] = formatValue(v)
		}
		result.Rows = append(result.Rows, row)
	}

	return result, rows.Err()
}

// formatValue converts any database value to a string representation.
// It handles the full range of types that database/sql drivers may return,
// including time.Time, bool, numeric types, []byte (binary/JSON), and
// sql.Null* wrappers, so that sample data from any column type is captured.
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case bool:
		return strconv.FormatBool(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case time.Time:
		if val.Hour() == 0 && val.Minute() == 0 && val.Second() == 0 && val.Nanosecond() == 0 {
			return val.Format("2006-01-02")
		}
		return val.Format(time.RFC3339)
	case json.RawMessage:
		return string(val)
	case sql.NullString:
		if val.Valid {
			return val.String
		}
		return ""
	case sql.NullInt64:
		if val.Valid {
			return strconv.FormatInt(val.Int64, 10)
		}
		return ""
	case sql.NullFloat64:
		if val.Valid {
			return strconv.FormatFloat(val.Float64, 'f', -1, 64)
		}
		return ""
	case sql.NullBool:
		if val.Valid {
			return strconv.FormatBool(val.Bool)
		}
		return ""
	case sql.NullTime:
		if val.Valid {
			return formatValue(val.Time)
		}
		return ""
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}
