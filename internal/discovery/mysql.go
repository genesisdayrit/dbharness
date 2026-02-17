package discovery

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

const defaultMySQLPort = 3306

type mysqlDiscoverer struct {
	db       *sql.DB
	database string
}

type mysqlDatabaseLister struct {
	db *sql.DB
}

func newMySQL(cfg DatabaseConfig) (*mysqlDiscoverer, error) {
	dsn := buildMySQLDSN(cfg, cfg.Database)
	db, err := openDB("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &mysqlDiscoverer{
		db:       db,
		database: strings.TrimSpace(cfg.Database),
	}, nil
}

func newMySQLDatabaseLister(cfg DatabaseConfig) (*mysqlDatabaseLister, error) {
	// Connect without selecting a default DB so listing works even when
	// the current config has no database selected yet.
	dsn := buildMySQLDSN(cfg, "")
	db, err := openDB("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &mysqlDatabaseLister{db: db}, nil
}

func buildMySQLDSN(cfg DatabaseConfig, database string) string {
	port := cfg.Port
	if port <= 0 {
		port = defaultMySQLPort
	}

	driverCfg := mysqlDriver.NewConfig()
	driverCfg.User = cfg.User
	driverCfg.Passwd = cfg.Password
	driverCfg.Net = "tcp"
	driverCfg.Addr = net.JoinHostPort(strings.TrimSpace(cfg.Host), strconv.Itoa(port))
	driverCfg.DBName = strings.TrimSpace(database)
	driverCfg.ParseTime = true

	return driverCfg.FormatDSN()
}

func (m *mysqlDatabaseLister) ListDatabases(ctx context.Context) ([]string, error) {
	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY schema_name
	`

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query mysql databases: %w", err)
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

func (m *mysqlDatabaseLister) Close() error {
	return m.db.Close()
}

func (m *mysqlDiscoverer) Discover(ctx context.Context) ([]SchemaInfo, error) {
	schemas, err := m.getSchemas(ctx)
	if err != nil {
		return nil, err
	}

	for i := range schemas {
		tables, err := m.getTables(ctx, schemas[i].Name)
		if err != nil {
			return nil, fmt.Errorf("get tables for schema %q: %w", schemas[i].Name, err)
		}
		schemas[i].Tables = tables
	}

	return schemas, nil
}

func (m *mysqlDiscoverer) getSchemas(ctx context.Context) ([]SchemaInfo, error) {
	baseQuery := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
	`

	args := make([]interface{}, 0, 1)
	if database := strings.TrimSpace(m.database); database != "" {
		baseQuery += " AND schema_name = ?"
		args = append(args, database)
	}
	baseQuery += " ORDER BY schema_name"

	rows, err := m.db.QueryContext(ctx, baseQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("query mysql schemas: %w", err)
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

func (m *mysqlDiscoverer) getTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = ?
		ORDER BY table_name
	`

	rows, err := m.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("query mysql tables: %w", err)
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

func (m *mysqlDiscoverer) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	query := `
		SELECT column_name, data_type, is_nullable, ordinal_position, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`

	rows, err := m.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query mysql columns: %w", err)
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

func (m *mysqlDiscoverer) GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error) {
	profile := newEnrichedColumnInfo(column)

	quotedSchema := quoteMySQLIdentifier(schema)
	quotedTable := quoteMySQLIdentifier(table)
	quotedColumn := quoteMySQLIdentifier(column.Name)

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_rows,
			SUM(CASE WHEN %[1]s IS NULL THEN 1 ELSE 0 END) AS null_count,
			COUNT(%[1]s) AS non_null_count,
			COUNT(DISTINCT CAST(%[1]s AS CHAR)) AS distinct_non_null_count
		FROM %[2]s.%[3]s
	`, quotedColumn, quotedSchema, quotedTable)

	var totalRowsRaw, nullCountRaw, nonNullCountRaw, distinctCountRaw interface{}
	if err := m.db.QueryRowContext(ctx, statsQuery).Scan(
		&totalRowsRaw,
		&nullCountRaw,
		&nonNullCountRaw,
		&distinctCountRaw,
	); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile mysql column %q on %s.%s: %w",
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
		SELECT DISTINCT LEFT(CAST(%[1]s AS CHAR), %[2]d)
		FROM %[3]s.%[4]s
		WHERE %[1]s IS NOT NULL
		LIMIT %[5]d
	`, quotedColumn, maxColumnSampleValueLength, quotedSchema, quotedTable, columnProfileSampleValueLimit)

	rows, err := m.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"query mysql sample values for %q on %s.%s: %w",
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
				"scan mysql sample value for %q on %s.%s: %w",
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
			"iterate mysql sample values for %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	profile.SampleValues = normalizeColumnSampleValues(samples)
	return profile, nil
}

func (m *mysqlDiscoverer) GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s ORDER BY RAND() LIMIT %d",
		quoteMySQLIdentifier(schema),
		quoteMySQLIdentifier(table),
		limit,
	)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query mysql sample rows: %w", err)
	}
	defer rows.Close()

	return scanSampleRows(rows)
}

func (m *mysqlDiscoverer) Close() error {
	return m.db.Close()
}
