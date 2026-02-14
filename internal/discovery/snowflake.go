package discovery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/snowflakedb/gosnowflake"
)

type snowflakeDiscoverer struct {
	db       *sql.DB
	database string
}

type snowflakeDatabaseLister struct {
	db *sql.DB
}

func newSnowflake(cfg DatabaseConfig) (*snowflakeDiscoverer, error) {
	sfConfig := &gosnowflake.Config{
		Account:   cfg.Account,
		User:      cfg.User,
		Password:  cfg.Password,
		Role:      cfg.Role,
		Warehouse: cfg.Warehouse,
		Database:  cfg.Database,
		Schema:    cfg.Schema,
	}

	switch cfg.Authenticator {
	case "externalbrowser":
		sfConfig.Authenticator = gosnowflake.AuthTypeExternalBrowser
	default:
		sfConfig.Authenticator = gosnowflake.AuthTypeSnowflake
	}

	dsn, err := gosnowflake.DSN(sfConfig)
	if err != nil {
		return nil, fmt.Errorf("build snowflake DSN: %w", err)
	}

	db, err := openDB("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	return &snowflakeDiscoverer{db: db, database: cfg.Database}, nil
}

func newSnowflakeDatabaseLister(cfg DatabaseConfig) (*snowflakeDatabaseLister, error) {
	sfConfig := &gosnowflake.Config{
		Account:   cfg.Account,
		User:      cfg.User,
		Password:  cfg.Password,
		Role:      cfg.Role,
		Warehouse: cfg.Warehouse,
		// Deliberately omit Database and Schema so we connect at the account level.
	}

	switch cfg.Authenticator {
	case "externalbrowser":
		sfConfig.Authenticator = gosnowflake.AuthTypeExternalBrowser
	default:
		sfConfig.Authenticator = gosnowflake.AuthTypeSnowflake
	}

	dsn, err := gosnowflake.DSN(sfConfig)
	if err != nil {
		return nil, fmt.Errorf("build snowflake DSN: %w", err)
	}

	db, err := openDB("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	return &snowflakeDatabaseLister{db: db}, nil
}

func (s *snowflakeDatabaseLister) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("query snowflake databases: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get column names: %w", err)
	}

	nameIdx := -1
	for i, col := range cols {
		if strings.EqualFold(col, "name") {
			nameIdx = i
			break
		}
	}
	if nameIdx < 0 {
		return nil, fmt.Errorf("SHOW DATABASES result has no 'name' column")
	}

	var databases []string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan database row: %w", err)
		}

		name, ok := vals[nameIdx].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for database name column")
		}
		databases = append(databases, name)
	}

	return databases, rows.Err()
}

func (s *snowflakeDatabaseLister) Close() error {
	return s.db.Close()
}

func (s *snowflakeDiscoverer) Discover(ctx context.Context) ([]SchemaInfo, error) {
	schemas, err := s.getSchemas(ctx)
	if err != nil {
		return nil, err
	}

	for i := range schemas {
		tables, err := s.getTables(ctx, schemas[i].Name)
		if err != nil {
			return nil, fmt.Errorf("get tables for schema %q: %w", schemas[i].Name, err)
		}
		schemas[i].Tables = tables
	}

	return schemas, nil
}

func (s *snowflakeDiscoverer) getSchemas(ctx context.Context) ([]SchemaInfo, error) {
	// Use INFORMATION_SCHEMA for consistency. Filter out the INFORMATION_SCHEMA itself.
	query := `
		SELECT SCHEMA_NAME
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
		ORDER BY SCHEMA_NAME
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query snowflake schemas: %w", err)
	}
	defer rows.Close()

	var schemas []SchemaInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan schema row: %w", err)
		}
		schemas = append(schemas, SchemaInfo{Name: name})
	}
	return schemas, rows.Err()
}

func (s *snowflakeDiscoverer) getTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `
		SELECT TABLE_NAME, TABLE_TYPE
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`

	rows, err := s.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("query snowflake tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Name, &t.TableType); err != nil {
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		// Normalize Snowflake table types to match our standard format.
		t.TableType = normalizeTableType(t.TableType)
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (s *snowflakeDiscoverer) Close() error {
	return s.db.Close()
}

// normalizeTableType converts Snowflake-specific table type strings to a
// standard format. Snowflake returns "BASE TABLE", "VIEW",
// "MATERIALIZED VIEW", etc. which are already reasonable, but we normalize
// the casing for consistency.
func normalizeTableType(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}
