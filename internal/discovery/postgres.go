package discovery

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type postgresDiscoverer struct {
	db *sql.DB
}

type postgresDatabaseLister struct {
	db *sql.DB
}

func newPostgres(cfg DatabaseConfig) (*postgresDiscoverer, error) {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, sslMode,
	)

	db, err := openDB("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &postgresDiscoverer{db: db}, nil
}

func newPostgresDatabaseLister(cfg DatabaseConfig) (*postgresDatabaseLister, error) {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	// Connect to the "postgres" default database to list all databases.
	dbName := cfg.Database
	if dbName == "" {
		dbName = "postgres"
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, dbName, sslMode,
	)

	db, err := openDB("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &postgresDatabaseLister{db: db}, nil
}

func (p *postgresDatabaseLister) ListDatabases(ctx context.Context) ([]string, error) {
	query := `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false
		ORDER BY datname
	`

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query postgres databases: %w", err)
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

func (p *postgresDatabaseLister) Close() error {
	return p.db.Close()
}

func (p *postgresDiscoverer) Discover(ctx context.Context) ([]SchemaInfo, error) {
	schemas, err := p.getSchemas(ctx)
	if err != nil {
		return nil, err
	}

	for i := range schemas {
		tables, err := p.getTables(ctx, schemas[i].Name)
		if err != nil {
			return nil, fmt.Errorf("get tables for schema %q: %w", schemas[i].Name, err)
		}
		schemas[i].Tables = tables
	}

	return schemas, nil
}

func (p *postgresDiscoverer) getSchemas(ctx context.Context) ([]SchemaInfo, error) {
	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		  AND schema_name NOT LIKE 'pg_temp_%'
		  AND schema_name NOT LIKE 'pg_toast_temp_%'
		ORDER BY schema_name
	`

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query postgres schemas: %w", err)
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

func (p *postgresDiscoverer) getTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = $1
		ORDER BY table_name
	`

	rows, err := p.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("query postgres tables: %w", err)
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

func (p *postgresDiscoverer) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	query := `
		SELECT column_name, data_type, is_nullable, ordinal_position, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := p.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query postgres columns: %w", err)
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

func (p *postgresDiscoverer) GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error) {
	profile := newEnrichedColumnInfo(column)

	quotedSchema := quotePostgresIdentifier(schema)
	quotedTable := quotePostgresIdentifier(table)
	quotedColumn := quotePostgresIdentifier(column.Name)

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*)::bigint AS total_rows,
			COUNT(*) FILTER (WHERE %[1]s IS NULL)::bigint AS null_count,
			COUNT(%[1]s)::bigint AS non_null_count,
			COUNT(DISTINCT %[1]s::text)::bigint AS distinct_non_null_count
		FROM %[2]s.%[3]s
	`, quotedColumn, quotedSchema, quotedTable)

	if err := p.db.QueryRowContext(ctx, statsQuery).Scan(
		&profile.TotalRows,
		&profile.NullCount,
		&profile.NonNullCount,
		&profile.DistinctNonNullCount,
	); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile postgres column %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	profile.DistinctOfNonNullPct = percentOfTotal(profile.DistinctNonNullCount, profile.NonNullCount)
	profile.NullOfTotalRowsPct = percentOfTotal(profile.NullCount, profile.TotalRows)
	profile.NonNullOfTotalRowsPct = percentOfTotal(profile.NonNullCount, profile.TotalRows)

	if shouldSkipColumnSamples(column.DataType) {
		return profile, nil
	}

	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT LEFT(%[1]s::text, %[2]d)
		FROM %[3]s.%[4]s
		WHERE %[1]s IS NOT NULL
		LIMIT %[5]d
	`, quotedColumn, maxColumnSampleValueLength, quotedSchema, quotedTable, columnProfileSampleValueLimit)

	rows, err := p.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"query postgres sample values for %q on %s.%s: %w",
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
				"scan postgres sample value for %q on %s.%s: %w",
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
			"iterate postgres sample values for %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	profile.SampleValues = normalizeColumnSampleValues(samples)
	return profile, nil
}

func (p *postgresDiscoverer) GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error) {
	query := fmt.Sprintf(
		`SELECT * FROM %q.%q ORDER BY RANDOM() LIMIT %d`,
		schema, table, limit,
	)

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query postgres sample rows: %w", err)
	}
	defer rows.Close()

	return scanSampleRows(rows)
}

func (p *postgresDiscoverer) Close() error {
	return p.db.Close()
}
