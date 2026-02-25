# AGENTS Guide for `.dbharness`

This file is a navigation guide for coding agents (Codex, Claude Code, Cursor, Amp, etc.) working with dbharness context.

## Goal

Use the **fewest files possible** to answer schema questions correctly.

The context hierarchy is designed for targeted drill-down:

```text
.dbharness/
  config.json
  context/
    connections/
      <connection>/
        databases/
          _databases.yml
          <database>/
            schemas/
              _schemas.yml
              <schema>/
                _tables.yml
                <table>/
                  <table>__columns.yml
                  <table>__sample.xml
```

## Recommended traversal order (token-efficient)

1. Read `.dbharness/config.json` to identify the **primary/default connection**.
2. Go to `context/connections/<primary>/databases/_databases.yml` to identify databases.
3. Open `<database>/schemas/_schemas.yml` to see available schemas and table counts.
4. Open `<schema>/_tables.yml` only for schemas relevant to the user request.
5. Open `<table>/<table>__columns.yml` only for candidate tables you actually need.
6. Open `<table>/<table>__sample.xml` only when example values are needed to confirm data shape.

## Multi-connection rule

- In projects with multiple connections, **always start with the primary/default connection**.
- Only explore non-primary connections if the user explicitly asks for them or if the primary connection clearly does not contain the requested data.

## Worked example

User question:

> "Which table has customer email and signup timestamp for lifecycle analysis?"

Efficient path:

1. Read `.dbharness/config.json` and identify primary connection (for example: `analytics`).
2. Read:
   - `.dbharness/context/connections/analytics/databases/_databases.yml`
3. Choose the default/target database from that file, then read:
   - `.dbharness/context/connections/analytics/databases/<database>/schemas/_schemas.yml`
4. Pick likely schemas (for example `public`, `marketing`) and read only:
   - `.../schemas/public/_tables.yml`
   - `.../schemas/marketing/_tables.yml`
5. For likely tables (for example `users`, `customers`, `signups`), read only:
   - `.../public/users/users__columns.yml`
   - `.../marketing/signups/signups__columns.yml`
6. If column names are ambiguous, optionally read:
   - `.../<table>/<table>__sample.xml`

Stop once enough evidence is found. Avoid broad crawling.

## `__sample.xml` usage note

- `__sample.xml` files contain up to **10 sample rows** per table.
- They are useful for validating value formats, enums, and realistic data shape.
- They are often unnecessary for structural questions; skip them unless sample data is required.

## Practical heuristics

- Prefer index files first (`_databases.yml`, `_schemas.yml`, `_tables.yml`) before table-level files.
- Do not read every schema/table preemptively.
- Expand scope gradually: database -> schema -> table -> columns -> sample rows.
- When answering, cite the specific files used.
