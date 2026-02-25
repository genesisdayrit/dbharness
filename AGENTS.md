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
