# Architecture

This document describes the `.dbharness/context/` directory structure — what is currently implemented and the full future vision.
For coding-agent navigation guidance, see `.dbharness/AGENTS.md`.

## Current Structure (implemented)

```
.dbharness/
  AGENTS.md
  config.json
  context/
    connections/
      <connection-name>/
        MEMORY.md
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
      default/
        logs/
          YYYY-MM-DD.md
```

### Two-tier memory model

- **Long-term memory (implemented):**
  - Connection-level Markdown file at `context/connections/<name>/MEMORY.md`
  - Stores durable, high-confidence facts (schema quirks, naming conventions, query preferences)
- **Session memory (implemented):**
  - Workspace-level daily notes at `context/workspaces/<workspace>/logs/YYYY-MM-DD.md`
  - `default` workspace is created during `dbh init`, so session logs always have a landing directory

### Levels

| Level | Directory | Index/File | Description |
|-------|-----------|------------|-------------|
| Connection | `connections/<name>/` | `MEMORY.md` | One directory per configured connection with long-term memory and discovered schema context |
| Database | `databases/<name>/` | `_databases.yml` | One directory per database; index lists all databases |
| Schema | `schemas/<name>/` | `_schemas.yml` | One directory per schema; index lists all schemas with table counts |
| Table (index) | — | `_tables.yml` | Per-schema file listing all tables and views |
| Table (detail) | `<table>/` | `__columns.yml`, `__sample.xml` | Per-table column metadata (basic via `dbh tables`, enriched via `dbh columns`) and sample data |
| Workspace | `workspaces/<name>/` | `logs/YYYY-MM-DD.md` | Global workspaces for session-level notes; not scoped to a single connection |

### Naming Conventions

- **Underscore-prefixed YAML files** (`_databases.yml`, `_schemas.yml`, `_tables.yml`) are index files that live alongside subdirectories at the same level. The underscore prefix distinguishes index files from subdirectory names.
- **Double-underscore files** (`__columns.yml`, `__sample.xml`) are per-table detail files. The double underscore (`__`) separates the table name from the file type.
- **Directory names** are lowercased and sanitized: `/`, `\`, spaces, and `.` are replaced with `_`.
- **Connection names** are used as-is for directory names (they are user-chosen during `dbh init`).

## Future Vision (planned)

```
.dbharness/
  AGENTS.md
  config.json
  context/
    connections/
      <connection-name>/
        MEMORY.md
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
      default/
        logs/
          YYYY-MM-DD.md
      <workspace-name>/
        logs/
          YYYY-MM-DD.md
```

### Planned Additions

- **Workspace creation command**: `dbh create-workspace` / `dbh create-w` scaffolding for named workspaces.
- **Active workspace selection**: config/env/flag support for selecting non-default workspace targets.
- **Session transcripts**: searchable per-session transcript files with descriptive slugs.
- **Execution logs**: structured history of executed SQL and related metadata.
- **Schema refresh**: Automated detection and refresh of changed schemas and tables.

### What's Not Yet Implemented

- Named workspace creation commands and active workspace selection
- Session transcript export and execution log capture
- Automated schema change detection and refresh
