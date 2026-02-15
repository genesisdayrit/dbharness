[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# dbharness (dbh)

## Install

```bash
brew install genesisdayrit/tap/dbh
```

From source:

```bash
go install github.com/genesisdayrit/dbharness/cmd/dbh@latest
```

## Usage

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

Use `--force` to overwrite an existing `.dbharness/` folder and start fresh:

```bash
dbh init --force
```

When `.dbharness/` already exists, `dbh init --force` creates a full timestamped backup in `.dbharness-snapshots/<yyyymmdd_hhmm_ss>/` before overwriting. The backup includes the entire `.dbharness/` directory, not just `config.json`.

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

### `dbh test-connection`

Tests a database connection defined in `.dbharness/config.json`:

```bash
dbh test-connection -s my-db
```

If no name is provided, it defaults to `"default"`.
