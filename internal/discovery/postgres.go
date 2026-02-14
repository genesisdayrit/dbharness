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

func (p *postgresDiscoverer) Close() error {
	return p.db.Close()
}
