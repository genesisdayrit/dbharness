# Architecture

This document describes the `.dbharness/context/` directory structure — what is currently implemented and the full future vision.

## Current Structure (implemented)

```
.dbharness/
  config.json
  context/
    connections/
      <connection-name>/
        databases/
          _databases.yml
          <database>/
            schemas/
              _schemas.yml
              <schema>/
                _tables.yml
```

### Levels

| Level | Directory | Index File | Description |
|-------|-----------|------------|-------------|
| Connection | `connections/<name>/` | — | One directory per configured connection |
| Database | `databases/<name>/` | `_databases.yml` | One directory per database; index lists all databases |
| Schema | `schemas/<name>/` | `_schemas.yml` | One directory per schema; index lists all schemas with table counts |
| Table | — | `_tables.yml` | Per-schema file listing all tables and views |

### Naming Conventions

- **Underscore-prefixed YAML files** (`_databases.yml`, `_schemas.yml`, `_tables.yml`) are index files that live alongside subdirectories at the same level. The underscore prefix distinguishes index files from subdirectory names.
- **Directory names** are lowercased and sanitized: `/`, `\`, spaces, and `.` are replaced with `_`.
- **Connection names** are used as-is for directory names (they are user-chosen during `dbh init`).

## Future Vision (planned)

```
.dbharness/
  config.json
  context/
    connections/
      <connection-name>/
        databases/
          _databases.yml
          <database>/
            schemas/
              _schemas.yml
              <schema>/
                _tables.yml
                <table>/
                  <table_name>__columns.yml
                  <table_name>__sample.yml
        workspaces/
          projects/
          memories/
          logs/
```

### Planned Additions

- **Table-level detail**: Per-table directories with `__columns.yml` (column metadata) and `__sample.yml` (sample data) files. Double underscore (`__`) separates the table name from the file type.
- **Workspaces**: A `workspaces/` directory alongside `databases/` for project-specific context, memory, and logging.

### What's Not Yet Implemented

- `<table>/` subdirectories and per-table files (`__columns.yml`, `__sample.yml`)
- `workspaces/` directory and its subdirectories (`projects/`, `memories/`, `logs/`)
- Multi-database support within a single connection (currently one database per `schemas` run)
