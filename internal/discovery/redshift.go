package discovery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	defaultRedshiftPort    = 5439
	defaultRedshiftSSLMode = "require"
)

type redshiftDiscoverer struct {
	db *sql.DB
}

type redshiftDatabaseLister struct {
	db *sql.DB
}

func newRedshift(cfg DatabaseConfig) (*redshiftDiscoverer, error) {
	db, err := openDB("postgres", buildRedshiftConnString(cfg, cfg.Database))
	if err != nil {
		return nil, err
	}
	return &redshiftDiscoverer{db: db}, nil
}

func newRedshiftDatabaseLister(cfg DatabaseConfig) (*redshiftDatabaseLister, error) {
	// Redshift requires a database in the connection. Use "dev" when one
	// is not configured yet so database listing still works.
	dbName := strings.TrimSpace(cfg.Database)
	if dbName == "" {
		dbName = "dev"
	}

	db, err := openDB("postgres", buildRedshiftConnString(cfg, dbName))
	if err != nil {
		return nil, err
	}
	return &redshiftDatabaseLister{db: db}, nil
}

func buildRedshiftConnString(cfg DatabaseConfig, database string) string {
	port := cfg.Port
	if port <= 0 {
		port = defaultRedshiftPort
	}

	sslMode := strings.TrimSpace(cfg.SSLMode)
	if sslMode == "" {
		sslMode = defaultRedshiftSSLMode
	}

	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		strings.TrimSpace(cfg.Host),
		port,
		strings.TrimSpace(cfg.User),
		cfg.Password,
		strings.TrimSpace(database),
		sslMode,
	)
}

func (r *redshiftDatabaseLister) ListDatabases(ctx context.Context) ([]string, error) {
	query := `
		SELECT datname
		FROM pg_database
		WHERE datallowconn = true
		  AND datistemplate = false
		ORDER BY datname
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query redshift databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan database row: %w", err)
		}
		databases = append(databases, name)
	}
	return databases, rows.Err()
}

func (r *redshiftDatabaseLister) Close() error {
	return r.db.Close()
}

func (r *redshiftDiscoverer) Discover(ctx context.Context) ([]SchemaInfo, error) {
	schemas, err := r.getSchemas(ctx)
	if err != nil {
		return nil, err
	}

	for i := range schemas {
		tables, err := r.getTables(ctx, schemas[i].Name)
		if err != nil {
			return nil, fmt.Errorf("get tables for schema %q: %w", schemas[i].Name, err)
		}
		schemas[i].Tables = tables
	}

	return schemas, nil
}

func (r *redshiftDiscoverer) getSchemas(ctx context.Context) ([]SchemaInfo, error) {
	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
		  AND schema_name NOT LIKE 'pg_temp_%'
		ORDER BY schema_name
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query redshift schemas: %w", err)
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

func (r *redshiftDiscoverer) getTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = $1
		ORDER BY table_name
	`

	rows, err := r.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("query redshift tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Name, &t.TableType); err != nil {
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (r *redshiftDiscoverer) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	query := `
		SELECT column_name, data_type, is_nullable, ordinal_position, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := r.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query redshift columns: %w", err)
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

func (r *redshiftDiscoverer) GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error) {
	profile := newEnrichedColumnInfo(column)

	quotedSchema := quoteRedshiftIdentifier(schema)
	quotedTable := quoteRedshiftIdentifier(table)
	quotedColumn := quoteRedshiftIdentifier(column.Name)

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*)::bigint AS total_rows,
			SUM(CASE WHEN %[1]s IS NULL THEN 1 ELSE 0 END)::bigint AS null_count,
			COUNT(%[1]s)::bigint AS non_null_count,
			COUNT(DISTINCT CASE WHEN %[1]s IS NULL THEN NULL ELSE CAST(%[1]s AS VARCHAR(65535)) END)::bigint AS distinct_non_null_count
		FROM %[2]s.%[3]s
	`, quotedColumn, quotedSchema, quotedTable)

	var totalRowsRaw, nullCountRaw, nonNullCountRaw, distinctCountRaw interface{}
	if err := r.db.QueryRowContext(ctx, statsQuery).Scan(
		&totalRowsRaw,
		&nullCountRaw,
		&nonNullCountRaw,
		&distinctCountRaw,
	); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile redshift column %q on %s.%s: %w",
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
		SELECT DISTINCT LEFT(CAST(%[1]s AS VARCHAR(65535)), %[2]d)
		FROM %[3]s.%[4]s
		WHERE %[1]s IS NOT NULL
		LIMIT %[5]d
	`, quotedColumn, maxColumnSampleValueLength, quotedSchema, quotedTable, columnProfileSampleValueLimit)

	rows, err := r.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"query redshift sample values for %q on %s.%s: %w",
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
				"scan redshift sample value for %q on %s.%s: %w",
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
			"iterate redshift sample values for %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	profile.SampleValues = normalizeColumnSampleValues(samples)
	return profile, nil
}

func (r *redshiftDiscoverer) GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s ORDER BY RANDOM() LIMIT %d",
		quoteRedshiftIdentifier(schema),
		quoteRedshiftIdentifier(table),
		limit,
	)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query redshift sample rows: %w", err)
	}
	defer rows.Close()

	return scanSampleRows(rows)
}

func (r *redshiftDiscoverer) Close() error {
	return r.db.Close()
}
