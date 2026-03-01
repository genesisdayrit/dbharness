[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# dbharness (dbh)

> Turn your Database Context into LLM-Friendly Files

`dbharness` is a CLI tool that syncs your database context to LLM-ready files for use with coding agents. It gives developers and LLMs a shared, always-current schema map so they can answer data questions and generate better code without rediscovering the database from scratch.

## Quick Start

### 1) Install `dbh`

```bash
brew install genesisdayrit/tap/dbh
```

From source:

```bash
go install github.com/genesisdayrit/dbharness/cmd/dbh@latest
```

### 2) Initialize your project

Run `dbh init` in your project directory to create `.dbharness/` (including `AGENTS.md`) and configure a database connection:

```bash
dbh init
```

### 3) Sync your database context

Generate the initial AI-friendly context files with:

```bash
dbh sync
```

This runs the full discovery flow and writes schema context under `.dbharness/context/`.

---

## Command Reference

### `dbh init`

Initializes a `.dbharness/` folder in the current directory and walks you through setting up your first database connection interactively:

```
$ dbh init
Installed .dbharness to /path/to/project/.dbharness

Connection name: my-db
? Database type: postgres
? Environment: local
Host: localhost
Port (press Enter for 5432):
Database: myapp
User: postgres
Password: secret
? SSL Mode: require

Testing connection to my-db...
Connection ok!

Added "my-db" to /path/to/project/.dbharness/config.json
```

Database type, environment, and SSL mode use interactive arrow-key selectors.

Running `dbh init` again will prompt you to add another connection to the existing config.

For full connection setup details for all supported types (Postgres, Redshift,
Snowflake, MySQL, BigQuery, SQLite), see [`docs/guides/connections.md`](./docs/guides/connections.md).

Use `--force` to overwrite an existing `.dbharness/` folder and start fresh:

```bash
dbh init --force
```

When `.dbharness/` already exists, `dbh init --force` creates a full timestamped backup in `.dbharness-snapshots/<yyyymmdd_hhmm_ss>/` before overwriting. The backup includes the entire `.dbharness/` directory, not just `config.json`.

### `dbh sync`

Runs the full discovery workflow in one command:

```bash
# Use the primary connection
dbh sync

# Use a specific connection
dbh sync -s my-db
```

`dbh sync` executes these commands in order:

1. `dbh databases`
2. `dbh schemas`
3. `dbh tables`

Each stage prints progress and status. If a stage fails, dbh continues to the
next stage and prints a summary at the end.

---

### `dbh workspace create`

Scaffolds a named workspace under `.dbharness/context/workspaces/<name>/`:

```bash
# Provide a name flag
dbh workspace create --name q1-revenue

# Or run interactively (prompts for a workspace name)
dbh workspace create
```

The command creates:

```text
.dbharness/context/workspaces/<name>/
  logs/
  MEMORY.md
  _workspace.yml
```

When run without `--name`, dbh prompts whether to set the new workspace as active
by writing `"active_workspace": "<name>"` to `.dbharness/config.json`.
With `--name`, dbh skips this prompt and leaves the active workspace unchanged.

---

### Sub-commands

The following commands can also be run individually for more control over each stage of the discovery workflow.

### `dbh databases`

Connects to a database and discovers all accessible databases, writing a catalog file to `.dbharness/context/connections/<name>/databases/_databases.yml`:

```bash
# Use the primary connection
dbh databases

# Use a specific connection
dbh databases -s my-db
```

The command:

- queries the database server for all accessible databases
- prompts you to select a default database (if multiple are found and none is configured)
- writes the default selection to both `config.json` and `_databases.yml`

Example `_databases.yml` output:

```yaml
connection: my-db
database_type: postgres
default_database: myapp
generated_at: "2026-02-22T14:30:45Z"
databases:
  - name: analytics
  - name: myapp
  - name: staging
```

### `dbh schemas`

Connects to a database and generates LLM-friendly schema context files in `.dbharness/context/connections/`:

```bash
# Use the primary connection
dbh schemas

# Use a specific connection
dbh schemas -s my-db
```

This creates a nested directory structure:

```
.dbharness/context/connections/my-db/databases/
  _databases.yml                  # List of databases in this connection
  myapp/
    schemas/
      _schemas.yml                # Overview of all schemas
      public/
        _tables.yml               # Tables in the "public" schema
      analytics/
        _tables.yml               # Tables in the "analytics" schema
```

The YAML files are designed for AI coding agents (Claude Code, Cursor, etc.) to discover and explore database structures. Re-running the command refreshes the files with the latest schema data.

### `dbh tables`

Runs an interactive workflow to generate per-table detail files (`__columns.yml` + `__sample.xml`).

```bash
# Use the primary connection
dbh tables

# Use a specific connection
dbh tables -s my-db
```

The command:

- lets you select databases and schemas interactively
- fetches column metadata for each selected table
- writes `<table>__columns.yml` and `<table>__sample.xml` files under table directories
- overwrites existing table detail files with fresh data when re-run

For full workflow details and examples, see [`docs/guides/tables.md`](./docs/guides/tables.md).

### `dbh columns`

Runs an interactive workflow to generate enriched `<table>__columns.yml` files with column-level profiling statistics.

```bash
# Use the primary connection
dbh columns

# Use a specific connection
dbh columns -s my-db
```

The command:

- warns that profiling can take minutes
- lets you select databases, schemas, and tables interactively
- computes per-column metrics (null/non-null counts, distinct counts, percentages)
- writes one enriched `<table>__columns.yml` file per selected table

`dbh columns` does not modify existing `__sample.xml` files.

### `dbh test-connection`

Tests a database connection defined in `.dbharness/config.json`:

```bash
dbh test-connection -s my-db
```

If no name is provided, it defaults to `"default"`.

### `dbh ls -c`

Lists configured connections from `.dbharness/config.json`:

```bash
dbh ls -c
```

Output columns:

- Connection name
- Database type
- Host URL (or `-` when unavailable)

### `dbh set-default -c`

Interactively selects a connection and makes it the primary default in `.dbharness/config.json`:

```bash
dbh set-default -c
```

The selected connection is set to `"primary": true`, and the previous primary connection is updated to `"primary": false`.

### `dbh set-default -d`

Interactively selects a default database for the current primary connection using:

```text
.dbharness/context/connections/<primary-connection>/databases/_databases.yml
```

The prompt shows the current default database (if one exists), includes an option to keep it, and writes any new selection to both:

- `.dbharness/config.json` (connection `database` field)
- `.dbharness/context/connections/<primary-connection>/databases/_databases.yml` (`default_database`)

## Guides

For deeper walkthroughs and architecture details, see:

- [`docs/guides/init.md`](./docs/guides/init.md)
- [`docs/guides/connections.md`](./docs/guides/connections.md)
- [`docs/guides/databases.md`](./docs/guides/databases.md)
- [`docs/guides/schemas.md`](./docs/guides/schemas.md)
- [`docs/guides/tables.md`](./docs/guides/tables.md)
- [`docs/guides/columns.md`](./docs/guides/columns.md)
- [`docs/guides/snapshot.md`](./docs/guides/snapshot.md)
- [`docs/guides/architecture.md`](./docs/guides/architecture.md)
