# AGENTS Guide for `.dbharness`

This file is a navigation guide for coding agents working with dbharness context.

## Goal

Use the **fewest files possible** to answer schema questions correctly.

The context hierarchy is designed for targeted drill-down:

```text
.dbharness/
  config.json
  context/
    connections/
      <connection>/
        MEMORY.md
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
    workspaces/
      default/
        logs/
          YYYY-MM-DD.md
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

## Memory Writing

When you discover something of durable, repeat value during a session — such as a schema
quirk, a naming convention, a known data quality issue, a preferred query pattern, or a
standing fact about this database — write it to the connection's long-term memory file at:

  .dbharness/context/connections/<name>/MEMORY.md

Only promote facts that meet all three criteria:
- High confidence: you observed it directly, not inferred
- High reuse value: likely to be useful in future sessions, not just this one
- General to the connection: not specific to a single one-off task

Do not over-write. A short, precise entry is better than a long speculative one.
Avoid writing anything you are not confident about — inaccurate long-term memory is
worse than no memory.

For session-level notes — what was explored, what queries were attempted, what decisions
were made — append to:

  .dbharness/context/workspaces/default/logs/YYYY-MM-DD.md

(Replace 'default' with the active workspace name if one has been configured.)

At the start of each session, read MEMORY.md for the active connection to load prior context.

If during a session you discover that a previously promoted fact in MEMORY.md is no longer
accurate — for example, a schema has changed, a column has been renamed, or a prior assumption
has been proven wrong — you have the authority to correct or remove that entry. Stale or
conflicting memory is worse than no memory. Prefer a precise correction over leaving an
outdated entry that could mislead future sessions.
