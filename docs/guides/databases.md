# Databases

`dbh databases` discovers databases for a configured connection and
updates `.dbharness/context/connections/<connection-name>/databases/_databases.yml`.

It is useful when you want to refresh the database list without generating full
schema context for each database.

## Quick start

```bash
# Update databases for the primary connection
dbh databases

# Update databases for a specific connection
dbh databases -s my-connection
```

## What it writes

The command updates:

```text
.dbharness/context/connections/<connection-name>/databases/_databases.yml
```

Example:

```yaml
connection: my-db
database_type: postgres
default_database: myapp
generated_at: "2026-02-12T15:30:00Z"
databases:
  - name: analytics
  - name: myapp
  - name: reporting
```

### `default_database` behavior

`default_database` is always present and is resolved in this order:

1. Use the connection's configured default database from `.dbharness/config.json`.
2. If no default is configured and exactly one database is discovered, use that
   single database and save it to `.dbharness/config.json`.
3. If no default is configured and multiple databases are discovered, prompt the
   user to choose one from an alphabetical arrow-key selector and save it to
   `.dbharness/config.json`.
4. If no databases are discovered, use `_default`.

### Interactive selection (multiple databases)

When multiple databases are available and no default is set, dbh prompts:

```text
No default database configured for connection "my-db".
? Select a default database
  > analytics
    myapp
    reporting
```

Use up/down arrows to select, then press Enter.

## Merge behavior

`_databases.yml` preserves existing entries and appends newly discovered
databases. New entries are appended in alphabetical order.

## Connection selection

| Flag | Behavior |
|------|----------|
| (none) | Uses the primary connection |
| `-s name` | Uses the connection with the given name |
| `--name name` | Same as `-s` |

## Supported databases

- `postgres`
- `redshift`
- `snowflake`
- `mysql`
- `bigquery`

## Related guides

- [`schemas.md`](./schemas.md) for `_schemas.yml` and per-schema `_tables.yml`
- [`columns.md`](./columns.md) for enriched `<table>__columns.yml` profiling
- [`init.md`](./init.md) for setting connection defaults during setup
