# dbharness

## Install

```bash
brew install genesisdayrit/tap/dbharness
```

From source:

```bash
go install github.com/genesisdayrit/dbharness/cmd/dbharness@latest
```

## Usage

### `dbharness init`

Initializes a `.dbharness/` folder in the current directory and walks you through setting up your first database connection interactively:

```
$ dbharness init
Installed .dbharness to /path/to/project/.dbharness

Connection name: my-db
Environment [prod, staging, dev, local, testing] (): local
Database type (postgres):
Host: localhost
Port (5432):
Database: myapp
User: postgres
Password: secret
SSL Mode [require, disable]
  (require): disable

Testing connection to my-db...
Connection ok!

Added "my-db" to /path/to/project/.dbharness/config.json
```

Running `dbharness init` again will prompt you to add another connection to the existing config.

Use `--force` to overwrite an existing `.dbharness/` folder and start fresh:

```bash
dbharness init --force
```

### `dbharness test-connection`

Tests a database connection defined in `.dbharness/config.json`:

```bash
dbharness test-connection -s my-db
```

If no name is provided, it defaults to `"default"`.
