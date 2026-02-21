package discovery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

type sqliteDiscoverer struct {
	db *sql.DB
}

type sqliteDatabaseLister struct {
	db *sql.DB
}

func newSQLite(cfg DatabaseConfig) (*sqliteDiscoverer, error) {
	databasePath := strings.TrimSpace(cfg.Database)
	if databasePath == "" {
		return nil, fmt.Errorf("sqlite requires database file path")
	}

	db, err := openDB("sqlite", databasePath)
	if err != nil {
		return nil, err
	}
	return &sqliteDiscoverer{db: db}, nil
}

func newSQLiteDatabaseLister(cfg DatabaseConfig) (*sqliteDatabaseLister, error) {
	databasePath := strings.TrimSpace(cfg.Database)
	if databasePath == "" {
		return nil, fmt.Errorf("sqlite requires database file path")
	}

	db, err := openDB("sqlite", databasePath)
	if err != nil {
		return nil, err
	}
	return &sqliteDatabaseLister{db: db}, nil
}

func (s *sqliteDatabaseLister) ListDatabases(ctx context.Context) ([]string, error) {
	return listSQLiteDatabases(ctx, s.db)
}

func (s *sqliteDatabaseLister) Close() error {
	return s.db.Close()
}

func listSQLiteDatabases(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return nil, fmt.Errorf("query sqlite databases: %w", err)
	}
	defer rows.Close()

	seen := make(map[string]bool)
	var databases []string

	for rows.Next() {
		var (
			seq      int
			name     string
			filePath sql.NullString
		)
		if err := rows.Scan(&seq, &name, &filePath); err != nil {
			return nil, fmt.Errorf("scan sqlite database row: %w", err)
		}

		dbName := strings.TrimSpace(name)
		if dbName == "" || strings.EqualFold(dbName, "temp") || seen[dbName] {
			continue
		}

		seen[dbName] = true
		databases = append(databases, dbName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(databases) == 0 {
		databases = append(databases, "main")
	}

	return databases, nil
}

func (s *sqliteDiscoverer) Discover(ctx context.Context) ([]SchemaInfo, error) {
	databases, err := listSQLiteDatabases(ctx, s.db)
	if err != nil {
		return nil, err
	}

	schemas := make([]SchemaInfo, 0, len(databases))
	for _, dbName := range databases {
		tables, err := s.getTables(ctx, dbName)
		if err != nil {
			return nil, fmt.Errorf("get tables for sqlite database %q: %w", dbName, err)
		}
		schemas = append(schemas, SchemaInfo{
			Name:   dbName,
			Tables: tables,
		})
	}

	return schemas, nil
}

func (s *sqliteDiscoverer) getTables(ctx context.Context, schema string) ([]TableInfo, error) {
	schemaName := normalizeSQLiteSchemaName(schema)
	query := fmt.Sprintf(`
		SELECT name, type
		FROM %s.sqlite_master
		WHERE type IN ('table', 'view')
		  AND name NOT LIKE 'sqlite_%%'
		ORDER BY name
	`, quoteSQLiteIdentifier(schemaName))

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query sqlite tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var (
			name    string
			rawType string
		)
		if err := rows.Scan(&name, &rawType); err != nil {
			return nil, fmt.Errorf("scan sqlite table row: %w", err)
		}
		tables = append(tables, TableInfo{
			Name:      name,
			TableType: normalizeSQLiteTableType(rawType),
		})
	}

	return tables, rows.Err()
}

func normalizeSQLiteTableType(rawType string) string {
	switch strings.ToLower(strings.TrimSpace(rawType)) {
	case "table":
		return "BASE TABLE"
	case "view":
		return "VIEW"
	default:
		normalized := strings.ToUpper(strings.TrimSpace(rawType))
		if normalized == "" {
			return "BASE TABLE"
		}
		return normalized
	}
}

func (s *sqliteDiscoverer) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	schemaName := normalizeSQLiteSchemaName(schema)
	query := fmt.Sprintf(
		"PRAGMA %s.table_info(%s)",
		quoteSQLiteIdentifier(schemaName),
		quoteSQLiteStringLiteral(table),
	)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query sqlite columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var (
			cid          int
			name         string
			dataType     string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan sqlite column row: %w", err)
		}

		isNullable := "YES"
		if notNull != 0 || primaryKey != 0 {
			isNullable = "NO"
		}

		columnDefault := ""
		if defaultValue.Valid {
			columnDefault = defaultValue.String
		}

		columns = append(columns, ColumnInfo{
			Name:            name,
			DataType:        strings.TrimSpace(dataType),
			IsNullable:      isNullable,
			OrdinalPosition: cid + 1,
			ColumnDefault:   columnDefault,
		})
	}

	return columns, rows.Err()
}

func (s *sqliteDiscoverer) GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error) {
	profile := newEnrichedColumnInfo(column)

	schemaName := normalizeSQLiteSchemaName(schema)
	quotedTable := fmt.Sprintf(
		"%s.%s",
		quoteSQLiteIdentifier(schemaName),
		quoteSQLiteIdentifier(table),
	)
	quotedColumn := quoteSQLiteIdentifier(column.Name)

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_rows,
			SUM(CASE WHEN %[1]s IS NULL THEN 1 ELSE 0 END) AS null_count,
			COUNT(%[1]s) AS non_null_count,
			COUNT(DISTINCT CASE WHEN %[1]s IS NULL THEN NULL ELSE CAST(%[1]s AS TEXT) END) AS distinct_non_null_count
		FROM %[2]s
	`, quotedColumn, quotedTable)

	var totalRowsRaw, nullCountRaw, nonNullCountRaw, distinctCountRaw interface{}
	if err := s.db.QueryRowContext(ctx, statsQuery).Scan(
		&totalRowsRaw,
		&nullCountRaw,
		&nonNullCountRaw,
		&distinctCountRaw,
	); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile sqlite column %q on %s.%s: %w",
			column.Name,
			schemaName,
			table,
			err,
		)
	}

	var err error
	profile.TotalRows, err = int64FromDBValue(totalRowsRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse total_rows for %q on %s.%s: %w", column.Name, schemaName, table, err)
	}
	profile.NullCount, err = int64FromDBValue(nullCountRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse null_count for %q on %s.%s: %w", column.Name, schemaName, table, err)
	}
	profile.NonNullCount, err = int64FromDBValue(nonNullCountRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse non_null_count for %q on %s.%s: %w", column.Name, schemaName, table, err)
	}
	profile.DistinctNonNullCount, err = int64FromDBValue(distinctCountRaw)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse distinct_non_null_count for %q on %s.%s: %w", column.Name, schemaName, table, err)
	}

	profile.DistinctOfNonNullPct = percentOfTotal(profile.DistinctNonNullCount, profile.NonNullCount)
	profile.NullOfTotalRowsPct = percentOfTotal(profile.NullCount, profile.TotalRows)
	profile.NonNullOfTotalRowsPct = percentOfTotal(profile.NonNullCount, profile.TotalRows)

	if shouldSkipColumnSamples(column.DataType) {
		return profile, nil
	}

	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT SUBSTR(CAST(%[1]s AS TEXT), 1, %[2]d)
		FROM %[3]s
		WHERE %[1]s IS NOT NULL
		LIMIT %[4]d
	`, quotedColumn, maxColumnSampleValueLength, quotedTable, columnProfileSampleValueLimit)

	rows, err := s.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"query sqlite sample values for %q on %s.%s: %w",
			column.Name,
			schemaName,
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
				"scan sqlite sample value for %q on %s.%s: %w",
				column.Name,
				schemaName,
				table,
				err,
			)
		}
		samples = append(samples, formatValue(value))
	}
	if err := rows.Err(); err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"iterate sqlite sample values for %q on %s.%s: %w",
			column.Name,
			schemaName,
			table,
			err,
		)
	}

	profile.SampleValues = normalizeColumnSampleValues(samples)
	return profile, nil
}

func (s *sqliteDiscoverer) GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error) {
	if limit <= 0 {
		limit = 10
	}

	schemaName := normalizeSQLiteSchemaName(schema)
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s ORDER BY RANDOM() LIMIT %d",
		quoteSQLiteIdentifier(schemaName),
		quoteSQLiteIdentifier(table),
		limit,
	)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query sqlite sample rows: %w", err)
	}
	defer rows.Close()

	return scanSampleRows(rows)
}

func normalizeSQLiteSchemaName(schema string) string {
	name := strings.TrimSpace(schema)
	if name == "" {
		return "main"
	}
	return name
}

func quoteSQLiteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func (s *sqliteDiscoverer) Close() error {
	return s.db.Close()
}
