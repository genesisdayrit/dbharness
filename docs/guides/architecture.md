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
                <table>/
                  <table_name>__columns.yml
                  <table_name>__sample.xml
```

### Levels

| Level | Directory | Index File | Description |
|-------|-----------|------------|-------------|
| Connection | `connections/<name>/` | — | One directory per configured connection |
| Database | `databases/<name>/` | `_databases.yml` | One directory per database; index lists all databases |
| Schema | `schemas/<name>/` | `_schemas.yml` | One directory per schema; index lists all schemas with table counts |
| Table (index) | — | `_tables.yml` | Per-schema file listing all tables and views |
| Table (detail) | `<table>/` | `__columns.yml`, `__sample.xml` | Per-table column metadata and sample data |

### Naming Conventions

- **Underscore-prefixed YAML files** (`_databases.yml`, `_schemas.yml`, `_tables.yml`) are index files that live alongside subdirectories at the same level. The underscore prefix distinguishes index files from subdirectory names.
- **Double-underscore files** (`__columns.yml`, `__sample.xml`) are per-table detail files. The double underscore (`__`) separates the table name from the file type.
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
                  <table_name>__sample.xml
        workspaces/
          projects/
          memories/
          logs/
```

### Planned Additions

- **Workspaces**: A `workspaces/` directory alongside `databases/` for project-specific context, memory, and logging.
- **Detailed column info command**: More detailed column information beyond what `__columns.yml` currently provides.
- **Schema refresh**: Automated detection and refresh of changed schemas and tables.

### What's Not Yet Implemented

- `workspaces/` directory and its subdirectories (`projects/`, `memories/`, `logs/`)
- Handling connection switches for multi-connection workflows
- Automated schema change detection and refresh
