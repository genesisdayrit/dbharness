# Init

`dbharness init` sets up a `.dbharness/` folder in the current directory and walks you through configuring your first database connection.

## First-time setup

Running `dbharness init` in a project without an existing `.dbharness/` folder will:

1. Create the `.dbharness/` directory with a `config.json`
2. Prompt you interactively to configure your first connection

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

The connection is tested before saving. If the test fails, nothing is written to the config.

## Adding more connections

Running `dbharness init` again when `.dbharness/` already exists will prompt you to add another connection:

```
$ dbharness init
.dbharness already exists at /path/to/project/.dbharness

Would you like to add a new connection? (y/n): y
Connection name: staging-db
Environment [prod, staging, dev, local, testing] (): staging
...
```

Connection names must be unique. If you enter a name that already exists, you'll be asked to choose another.

## Prompted fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Connection name | Yes | — | Must be unique across connections |
| Environment | No | — | Recommended: `prod`, `staging`, `dev`, `local`, `testing` |
| Database type | No | `postgres` | Only `postgres` is supported for connection testing currently |
| Host | Yes | — | |
| Port | No | `5432` | |
| Database | Yes | — | |
| User | Yes | — | |
| Password | Yes | — | |
| SSL Mode | No | `require` | Options: `require`, `disable` |

## Re-initializing with `--force`

To discard an existing `.dbharness/` folder and start fresh:

```bash
dbharness init --force
```

This deletes the existing `.dbharness/` directory, creates a new one, and prompts for the first connection again.

## Config file

Connections are stored in `.dbharness/config.json`:

```json
{
  "connections": [
    {
      "name": "my-db",
      "environment": "local",
      "type": "postgres",
      "host": "localhost",
      "port": 5432,
      "database": "myapp",
      "user": "postgres",
      "password": "secret",
      "sslmode": "disable"
    }
  ]
}
```

The `environment` field is omitted from the JSON when left blank.
