# .dbharness

Database context for LLM agents and developers. Installed by the `dbh` CLI, this folder contains connection config and auto-generated schema files so agents can discover your database structure without running queries.

## Structure

```
.dbharness/
  AGENTS.md          # Navigation guide for coding agents
  config.json        # Database connection configuration
  context/           # Auto-generated schema context (see below)
    connections/
      <connection>/
        databases/
          _databases.yml              # Accessible databases
          <database>/
            schemas/
              _schemas.yml            # Schemas with table counts
              <schema>/
                _tables.yml           # Tables and views
                <table>/
                  <table>__columns.yml  # Column metadata + profiling stats
                  <table>__sample.xml   # Sample rows (up to 10)
```

## Commands

Discovery commands accept `-s <name>` to target a specific connection (defaults to primary).

| Command | Description |
|---|---|
| `dbh init` | Set up `.dbharness/` and configure connections |
| `dbh sync [-s]` | Run full discovery workflow (databases → schemas → tables) |
| `dbh databases [-s]` | Discover accessible databases |
| `dbh schemas [-s]` | Generate schema-level context files |
| `dbh tables [-s]` | Generate per-table columns + sample data |
| `dbh columns [-s]` | Generate enriched column metadata with profiling |
| `dbh test-connection [-s]` | Test a database connection |
| `dbh ls -c` | List configured connections |
| `dbh set-default -c` | Set primary connection |
| `dbh set-default -d` | Set default database |
