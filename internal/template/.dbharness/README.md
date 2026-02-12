# .dbharness

This folder is installed by the dbharness CLI.

## Structure

```
.dbharness/
  config.json         # Database connection configuration
  context/            # LLM-friendly schema context files (generated)
    <connection>/
      schemas.yml     # All schemas with table counts
      schemas/
        <schema>/
          tables.yml  # Tables and views in this schema
```

## Commands

- `dbharness init` — Set up this directory and configure database connections
- `dbharness test-connection -s <name>` — Test a database connection
- `dbharness schemas [-s <name>]` — Generate schema context files for LLM discovery
- `dbharness snapshot` — Back up this directory
