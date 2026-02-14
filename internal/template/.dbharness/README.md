# .dbharness

This folder is installed by the dbh CLI.

## Structure

```
.dbharness/
  config.json                                   # Database connection configuration
  context/                                      # LLM-friendly schema context files (generated)
    connections/
      <connection>/
        databases/
          _databases.yml                        # List of databases in this connection
          <database>/
            schemas/
              _schemas.yml                      # All schemas with table counts
              <schema>/
                _tables.yml                     # Tables and views in this schema
```

## Commands

- `dbh init` — Set up this directory and configure database connections
- `dbh test-connection -s <name>` — Test a database connection
- `dbh schemas [-s <name>]` — Generate schema context files for LLM discovery
- `dbh snapshot` — Back up this directory
