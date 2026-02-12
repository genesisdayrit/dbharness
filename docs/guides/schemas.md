# Schemas

`dbharness schemas` connects to a database and generates LLM-friendly context files that describe the available schemas and tables. These files are written to `.dbharness/context/` and are designed to be easy for AI coding agents (Claude Code, Cursor, etc.) to discover and navigate.

## Quick start

```bash
# Generate schema context for the primary connection
dbharness schemas

# Generate for a specific connection
dbharness schemas -s my-connection
```

## What it generates

The command creates a nested directory structure inside `.dbharness/context/<connection-name>/`:

```
.dbharness/context/my-db/
  schemas.yml                     # Top-level overview of all schemas
  schemas/
    public/
      tables.yml                  # All tables and views in the "public" schema
    analytics/
      tables.yml                  # All tables and views in the "analytics" schema
```

### schemas.yml

The top-level file lists every schema in the database with table counts:

```yaml
connection: my-db
database: myapp
database_type: postgres
generated_at: "2026-02-12T15:30:00Z"
schemas:
  - name: public
    table_count: 12
    view_count: 3
    description: ""
    tables:
      - users
      - orders
      - products
  - name: analytics
    table_count: 5
    view_count: 2
    description: ""
    tables:
      - events
      - sessions
```

### tables.yml (per schema)

Each schema directory contains a `tables.yml` with a detailed listing:

```yaml
schema: public
connection: my-db
database: myapp
database_type: postgres
generated_at: "2026-02-12T15:30:00Z"
tables:
  - name: users
    type: BASE TABLE
    description: ""
  - name: orders
    type: BASE TABLE
    description: ""
  - name: daily_summary
    type: VIEW
    description: ""
```

## Description fields

The `description` fields in both `schemas.yml` and `tables.yml` are generated empty by default. They serve as placeholders that can be populated later with:

- Human-written descriptions
- LLM-generated descriptions (planned for a future release)

Descriptions help LLMs understand what each schema and table contains without having to inspect the actual data.

## Supported databases

### Postgres

Queries `information_schema.schemata` and `information_schema.tables`. System schemas are excluded by default:

- `information_schema`
- `pg_catalog`
- `pg_toast`
- `pg_temp_*`

### Snowflake

Queries `INFORMATION_SCHEMA.SCHEMATA` and `INFORMATION_SCHEMA.TABLES`. The `INFORMATION_SCHEMA` schema itself is excluded.

## Re-generating

Running `dbharness schemas` again overwrites the existing context files with fresh data. This is useful after schema changes (new tables, dropped schemas, etc.).

## Connection selection

The command uses the same connection resolution as `test-connection`:

| Flag | Behavior |
|------|----------|
| (none) | Uses the primary connection |
| `-s name` | Uses the connection with the given name |
| `--name name` | Same as `-s` |

## Example output

```
$ dbharness schemas -s my-db
Discovering schemas for connection "my-db" (postgres)...
Found 2 schema(s)
  public                         12 table(s)
  analytics                      5 table(s)
Total: 17 table(s) across 2 schema(s)

Schema context files written to /path/to/project/.dbharness/context/my-db

Files generated:
  .dbharness/context/my-db/schemas.yml
  .dbharness/context/my-db/schemas/public/tables.yml
  .dbharness/context/my-db/schemas/analytics/tables.yml
```

## For LLMs / AI agents

The generated files are specifically designed for LLM consumption:

1. **YAML format** — easy to parse and understand
2. **Comment headers** — explain the file structure at the top of each file
3. **Nested directories** — allow incremental exploration (start with schemas.yml, drill into specific schemas)
4. **Consistent naming** — predictable paths make it easy to navigate programmatically
5. **Metadata** — each file includes connection name, database, type, and generation timestamp
