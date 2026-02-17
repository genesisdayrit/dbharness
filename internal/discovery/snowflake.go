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

func (s *snowflakeDiscoverer) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION, COALESCE(COLUMN_DEFAULT, '')
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := s.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query snowflake columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.Name, &c.DataType, &c.IsNullable, &c.OrdinalPosition, &c.ColumnDefault); err != nil {
			return nil, fmt.Errorf("scan column row: %w", err)
		}
		columns = append(columns, c)
	}
	return columns, rows.Err()
}

func (s *snowflakeDiscoverer) GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error) {
	profile := newEnrichedColumnInfo(column)

	quotedSchema := quoteSnowflakeIdentifier(schema)
	quotedTable := quoteSnowflakeIdentifier(table)
	quotedColumn := quoteSnowflakeIdentifier(column.Name)

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_rows,
			COUNT_IF(%[1]s IS NULL) AS null_count,
			COUNT(%[1]s) AS non_null_count,
			COUNT(DISTINCT IFF(%[1]s IS NULL, NULL, TO_VARCHAR(%[1]s))) AS distinct_non_null_count
		FROM %[2]s.%[3]s
	`, quotedColumn, quotedSchema, quotedTable)

	var totalRowsRaw, nullCountRaw, nonNullCountRaw, distinctCountRaw interface{}
	if err := s.db.QueryRowContext(ctx, statsQuery).Scan(
		&totalRowsRaw,
		&nullCountRaw,
		&nonNullCountRaw,
		&distinctCountRaw,
	); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile snowflake column %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	var err error
	profile.TotalRows, err = int64FromDBValue(totalRowsRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse total_rows for %q on %s.%s: %w", column.Name, schema, table, err)
	}
	profile.NullCount, err = int64FromDBValue(nullCountRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse null_count for %q on %s.%s: %w", column.Name, schema, table, err)
	}
	profile.NonNullCount, err = int64FromDBValue(nonNullCountRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse non_null_count for %q on %s.%s: %w", column.Name, schema, table, err)
	}
	profile.DistinctNonNullCount, err = int64FromDBValue(distinctCountRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse distinct_non_null_count for %q on %s.%s: %w", column.Name, schema, table, err)
	}

	profile.DistinctOfNonNullPct = percentOfTotal(profile.DistinctNonNullCount, profile.NonNullCount)
	profile.NullOfTotalRowsPct = percentOfTotal(profile.NullCount, profile.TotalRows)
	profile.NonNullOfTotalRowsPct = percentOfTotal(profile.NonNullCount, profile.TotalRows)

	if shouldSkipColumnSamples(column.DataType) {
		return profile, nil
	}

	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT LEFT(TO_VARCHAR(%[1]s), %[2]d)
		FROM %[3]s.%[4]s
		WHERE %[1]s IS NOT NULL
		LIMIT %[5]d
	`, quotedColumn, maxColumnSampleValueLength, quotedSchema, quotedTable, columnProfileSampleValueLimit)

	rows, err := s.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"query snowflake sample values for %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}
	defer rows.Close()

	var samples []string
	for rows.Next() {
		var value interface{}
		if err := rows.Scan(&value); err != nil {
			return EnrichedColumnInfo{}, fmt.Errorf(
				"scan snowflake sample value for %q on %s.%s: %w",
				column.Name,
				schema,
				table,
				err,
			)
		}
		samples = append(samples, formatValue(value))
	}
	if err := rows.Err(); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"iterate snowflake sample values for %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	profile.SampleValues = normalizeColumnSampleValues(samples)
	return profile, nil
}

func (s *snowflakeDiscoverer) GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error) {
	query := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" ORDER BY RANDOM() LIMIT %d`,
		schema, table, limit,
	)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query snowflake sample rows: %w", err)
	}
	defer rows.Close()

	return scanSampleRows(rows)
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
