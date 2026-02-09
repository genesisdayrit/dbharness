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
? Database type: postgres
? Environment: local
Host: localhost
Port (press Enter for 5432):
Database: myapp
User: postgres
Password: secret
? SSL Mode: require

Testing connection to my-db...
Connection ok!

Added "my-db" to /path/to/project/.dbharness/config.json
```

Fields marked with `?` use interactive arrow-key selectors — use up/down arrows to choose, then press Enter to confirm.

The connection is tested before saving. If the test fails, nothing is written to the config.

## Adding more connections

Running `dbharness init` again when `.dbharness/` already exists will prompt you to add another connection:

```
$ dbharness init
.dbharness already exists at /path/to/project/.dbharness

Would you like to add a new connection? (y/n): y
Connection name: staging-db
? Database type: postgres
? Environment: staging
...
```

Connection names must be unique. If you enter a name that already exists, you'll be asked to choose another.

## Prompted fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Connection name | Yes | — | Must be unique across connections |
| Database type | Yes | — | Interactive selector. Only `postgres` is supported currently |
| Environment | No | — | Interactive selector: `production`, `staging`, `development`, `local`, `testing`, or skip |
| Host | Yes | — | |
| Port | No | `5432` | Press Enter to accept default |
| Database | Yes | — | |
| User | Yes | — | |
| Password | Yes | — | |
| SSL Mode | Yes | — | Interactive selector: `require`, `disable` |

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
