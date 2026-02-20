# Schemas

`dbh schemas` connects to a database and generates LLM-friendly context files that describe the available schemas and tables. These files are written to `.dbharness/context/` and are designed to be easy for AI coding agents (Claude Code, Cursor, etc.) to discover and navigate.

## Quick start

```bash
# Generate schema context for the primary connection
dbh schemas

# Generate for a specific connection
dbh schemas -s my-connection
```

## What it generates

The command creates a nested directory structure inside `.dbharness/context/connections/<connection-name>/`:

```
.dbharness/context/connections/my-db/databases/
  _databases.yml                  # List of databases in this connection
  myapp/
    schemas/
      _schemas.yml                # Top-level overview of all schemas
      public/
        _tables.yml               # All tables and views in the "public" schema
      analytics/
        _tables.yml               # All tables and views in the "analytics" schema
```

The `_databases.yml` file is documented in detail in
[`docs/guides/databases.md`](./databases.md), including how
`default_database` is chosen and persisted.

### _schemas.yml

The schema-level file lists every schema in the database with table/view
counts and a lightweight table summary. Schemas and tables are sorted
alphabetically for deterministic output:

```yaml
connection: my-db
database: myapp
database_type: postgres
generated_at: "2026-02-12T15:30:00Z"
schemas:
  - name: analytics
    table_count: 5
    view_count: 2
    ai_description: ""
    db_description: ""
    tables:
      - name: events
        type: BASE TABLE
        ai_description: ""
        db_description: ""
      - name: sessions
        type: BASE TABLE
        ai_description: ""
        db_description: ""
  - name: public
    table_count: 12
    view_count: 3
    ai_description: ""
    db_description: ""
    tables:
      - name: orders
        type: BASE TABLE
        ai_description: ""
        db_description: ""
      - name: products
        type: BASE TABLE
        ai_description: ""
        db_description: ""
      - name: users
        type: BASE TABLE
        ai_description: ""
        db_description: ""
```

### _tables.yml (per schema)

Each schema directory contains a `_tables.yml` with a detailed listing:

```yaml
schema: public
connection: my-db
database: myapp
database_type: postgres
generated_at: "2026-02-12T15:30:00Z"
tables:
  - name: users
    type: BASE TABLE
    ai_description: ""
    db_description: ""
  - name: orders
    type: BASE TABLE
    ai_description: ""
    db_description: ""
  - name: daily_summary
    type: VIEW
    ai_description: ""
    db_description: ""
```

## Description fields

Both `_schemas.yml` and `_tables.yml` include two separate description concepts:

- `ai_description`: intended for AI-authored context text.
- `db_description`: intended for database-native descriptions/comments.

Both fields are generated as empty strings when no description data is available.
Keeping these fields separate avoids mixing generated narrative (`ai_description`) with source-of-truth metadata (`db_description`).

> **Note:** The `_databases.yml` file does not have description fields yet. This may be added in a future release.

## Supported databases

### Postgres

Queries `information_schema.schemata` and `information_schema.tables`. System schemas are excluded by default:

- `information_schema`
- `pg_catalog`
- `pg_toast`
- `pg_temp_*`

### Redshift

Queries `information_schema.schemata` and `information_schema.tables` over the
PostgreSQL-compatible protocol. System schemas are excluded by default:

- `information_schema`
- `pg_catalog`
- `pg_internal`
- `pg_temp_*`

### Snowflake

Queries `INFORMATION_SCHEMA.SCHEMATA` and `INFORMATION_SCHEMA.TABLES`. The `INFORMATION_SCHEMA` schema itself is excluded.

### MySQL

Queries `information_schema.schemata` and `information_schema.tables`. System
schemas are excluded (`information_schema`, `mysql`, `performance_schema`,
`sys`).

### BigQuery

Treats datasets as schema equivalents and discovers them from the configured
project (stored in `project_id` / `database`).

## Re-generating

Running `dbh schemas` again overwrites the existing context files with fresh data. This is useful after schema changes (new tables, dropped schemas, etc.).

## Default database behavior

For `postgres`, `redshift`, `snowflake`, `mysql`, and `bigquery`, `dbh schemas`
generates context for only one configured default database (or project for
BigQuery).

Behavior:

1. If a default database is already configured in `.dbharness/config.json`,
   it is used directly.
2. If no default database is configured, dbh discovers databases for the
   connection and prompts you to select one.
3. The selected database is saved back to `.dbharness/config.json` and then
   used for schema generation.
4. If no databases can be discovered, the command exits with an error asking
   you to configure a default database.

Interactive selection example:

```text
$ dbh schemas -s my-db
No default database configured for connection "my-db".
? Select a database for schema generation
  > analytics
    myapp
    reporting
Saved default database "analytics" to /path/to/project/.dbharness/config.json
```

## Connection selection

The command uses the same connection resolution as `test-connection`:

| Flag | Behavior |
|------|----------|
| (none) | Uses the primary connection |
| `-s name` | Uses the connection with the given name |
| `--name name` | Same as `-s` |

## Example output

```
$ dbh schemas -s my-db
Discovering schemas for connection "my-db" (postgres)...
Found 2 schema(s)
  public                         12 table(s)
  analytics                      5 table(s)
Total: 17 table(s) across 2 schema(s)

Schema context files written to /path/to/project/.dbharness/context/connections/my-db/databases/myapp/schemas

Files generated:
  .dbharness/context/connections/my-db/databases/_databases.yml
  .dbharness/context/connections/my-db/databases/myapp/schemas/_schemas.yml
  .dbharness/context/connections/my-db/databases/myapp/schemas/public/_tables.yml
  .dbharness/context/connections/my-db/databases/myapp/schemas/analytics/_tables.yml
```

## For LLMs / AI agents

The generated files are specifically designed for LLM consumption:

1. **YAML format** — easy to parse and understand
2. **Comment headers** — explain the file structure at the top of each file
3. **Nested directories** — allow incremental exploration (start with `_databases.yml`, drill into schemas via `_schemas.yml`, then individual tables via `_tables.yml`)
4. **Consistent naming** — predictable paths make it easy to navigate programmatically
5. **Metadata** — each file includes connection name, database, type, and generation timestamp
