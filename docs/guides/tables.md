# Tables

`dbh tables` connects to a database and generates detailed per-table context files including column metadata and sample data. These files are designed for AI coding agents to have readily explorable database context for generating SQL and analyses.

## Quick start

```bash
# Generate table context for the primary connection
dbh tables

# Generate for a specific connection
dbh tables -s my-connection
```

## What it generates

For each selected table, the command creates a directory with two files inside `.dbharness/context/connections/<connection-name>/databases/<database>/schemas/<schema>/`:

```
.dbharness/context/connections/my-db/databases/myapp/schemas/
  public/
    _tables.yml
    users/
      users__columns.yml
      users__sample.xml
    orders/
      orders__columns.yml
      orders__sample.xml
  analytics/
    _tables.yml
    events/
      events__columns.yml
      events__sample.xml
```

### `<table_name>__columns.yml`

Column metadata for the table, queried from `information_schema.columns`:

```yaml
schema: public
table: users
connection: my-db
database: myapp
database_type: postgres
generated_at: "2026-02-16T12:00:00Z"
columns:
  - name: id
    data_type: integer
    is_nullable: "NO"
    ordinal_position: 1
    column_default: "nextval('users_id_seq'::regclass)"
  - name: name
    data_type: character varying
    is_nullable: "YES"
    ordinal_position: 2
  - name: email
    data_type: character varying
    is_nullable: "NO"
    ordinal_position: 3
```

### `<table_name>__sample.xml`

A random sample of up to 10 rows from the table, in XML format for LLM readability:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<table_sample schema="public" table="users" connection="my-db" database="myapp" row_count="3" generated_at="2026-02-16T12:00:00Z">
  <row>
    <field name="id">42</field>
    <field name="name">Alice</field>
    <field name="email">alice@example.com</field>
  </row>
  <row>
    <field name="id">17</field>
    <field name="name">Bob</field>
    <field name="email">bob@example.com</field>
  </row>
</table_sample>
```

The sample uses `ORDER BY RANDOM() LIMIT 10`, so each run produces different rows.

## Workflow

The `dbh tables` command follows an interactive workflow:

### 1. Database selection

- **Default database configured**: You are asked whether to use the default database or select from available databases.
- **No default database**: All available databases are listed with multi-select checkboxes. A "(Select all)" option is available to toggle all databases.

### 2. Schema selection

For each selected database:

- All schemas are discovered and listed with multi-select checkboxes.
- A "(Select all)" option selects all schemas at once.
- Schemas are sorted alphabetically.

### 3. Table processing

For each table in the selected schemas:

1. Column metadata is retrieved from `information_schema.columns`
2. A random sample of 10 rows is queried with `SELECT * ORDER BY RANDOM() LIMIT 10`
3. Files are written to the appropriate table directory

### Error handling

If a table cannot be accessed (e.g., permission denied), the error is logged and processing continues with the remaining tables. A summary of errors is displayed at the end.

## Connection selection

The command uses the same connection resolution as other commands:

| Flag | Behavior |
|------|----------|
| (none) | Uses the primary connection |
| `-s name` | Uses the connection with the given name |
| `--name name` | Same as `-s` |

## Supported databases

### Postgres

- Columns: Queries `information_schema.columns`
- Sample: `SELECT * FROM "schema"."table" ORDER BY RANDOM() LIMIT 10`
- System schemas (`pg_catalog`, `information_schema`, etc.) are excluded

### Snowflake

- Columns: Queries `INFORMATION_SCHEMA.COLUMNS`
- Sample: `SELECT * FROM "SCHEMA"."TABLE" ORDER BY RANDOM() LIMIT 10`
- `INFORMATION_SCHEMA` is excluded

## Re-generating

Running `dbh tables` again overwrites existing column and sample files with fresh data. This is useful after schema changes or when you want new sample data.

## File naming conventions

- **Directory names** are lowercased and sanitized (spaces, dots, slashes become underscores)
- **Column files** use the pattern `<sanitized_table_name>__columns.yml`
- **Sample files** use the pattern `<sanitized_table_name>__sample.xml`
- The double underscore (`__`) separates the table name from the file type

## Example session

```
$ dbh tables
Using connection "my-db" (postgres)

? Database selection (default: myapp)
  > Use default database (myapp)
    Select databases

? Select schemas
  > [x] (Select all)
    [ ] analytics
    [ ] public

--- Database: myapp ---
Discovering schemas...
Found 2 schema(s)

Processing schema "analytics" (3 tables)...
Processing schema "public" (5 tables)...

Generating context files for 8 table(s)...
Table context files written to /path/to/.dbharness/context/connections/my-db/databases/myapp/schemas

Files generated:
  .../analytics/events/events__columns.yml
  .../analytics/events/events__sample.xml
  .../analytics/sessions/sessions__columns.yml
  .../analytics/sessions/sessions__sample.xml
  ...

Processed 8 table(s) across 2 schema(s)
```

## For LLMs / AI agents

The generated files are specifically designed for LLM consumption:

1. **YAML columns** — structured, easy-to-parse column metadata
2. **XML samples** — readable sample data with named fields for quick data understanding
3. **Per-table directories** — allow targeted exploration of specific tables
4. **Comment headers** — explain file contents at the top of each YAML file
5. **Metadata** — each file includes connection, database, schema, and timestamp
6. **Nested structure** — drill from `_databases.yml` → `_schemas.yml` → `_tables.yml` → `<table>/__columns.yml` and `__sample.xml`
