# AGENTS.md

## Cursor Cloud specific instructions

**dbharness (dbh)** is a Go 1.24 CLI tool that syncs database schema context into LLM-friendly YAML/XML files. See `README.md` for full command reference.

### Build / Test / Lint

- **Build:** `go build -o dbh ./cmd/dbh`
- **Test:** `go test ./...` — all tests are self-contained (use in-memory SQLite, no external databases needed)
- **Lint:** `go vet ./...` — no golangci-lint config; `go vet` is the primary static analysis tool
- **Dependencies:** `go mod download` — the VM startup script also runs `go test ./...` to warm the build cache, so first builds/tests after boot are near-instant

### Running the CLI

The CLI (`dbh`) is interactive — most commands (e.g. `dbh init`, `dbh tables`, `dbh columns`) use charmbracelet/huh TUI prompts that require a TTY. For non-interactive testing:

1. Create a temp directory with a `.dbharness/config.json` and a SQLite database
2. Run non-interactive commands: `dbh test-connection -s <name>`, `dbh databases -s <name>`, `dbh schemas -s <name>`, `dbh ls -c`
3. Commands like `dbh tables` and `dbh columns` require interactive schema/table selection and cannot run headlessly

### Notes

- The `dbh` binary is not checked in; build it with `go build -o dbh ./cmd/dbh`
- SQLite is the only database type that works without external services (uses pure-Go driver `modernc.org/sqlite`)
- No Docker, Makefile, or CI setup scripts exist in this repo — only Go modules

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
