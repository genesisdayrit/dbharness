# Init

`dbh init` sets up a `.dbharness/` folder in the current directory and walks you through configuring your first database connection.

For the full connection setup guide (general instructions + all supported
connection types), see [`connections.md`](./connections.md).

## First-time setup

Running `dbh init` in a project without an existing `.dbharness/` folder will:

1. Create the `.dbharness/` directory with a `config.json`
2. Prompt you interactively to configure your first connection

```
$ dbh init
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

Running `dbh init` again when `.dbharness/` already exists will prompt you to add another connection:

```
$ dbh init
.dbharness already exists at /path/to/project/.dbharness

Would you like to add a new connection? (y/n): y
Connection name: staging-db
? Database type: postgres
? Environment: staging
...
```

Connection names must be unique. If you enter a name that already exists, you'll be asked to choose another.

## Prompted fields

The following fields are shared across all database types:

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Connection name | Yes | — | Must be unique across connections |
| Database type | Yes | — | Interactive selector: `postgres`, `redshift`, `snowflake`, `mysql`, `bigquery` |
| Environment | No | — | Interactive selector: `production`, `staging`, `development`, `local`, `testing`, or skip |

### Postgres fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Host | Yes | — | |
| Port | No | `5432` | Press Enter to accept default |
| Database | Yes | — | |
| User | Yes | — | |
| Password | Yes | — | |
| SSL Mode | Yes | — | Interactive selector: `require`, `disable` |

### Redshift fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Host | Yes | — | |
| Port | No | `5439` | Press Enter to accept default |
| Database | Yes | — | |
| User | Yes | — | |
| Password | Yes | — | |
| SSL Mode | Yes | — | Interactive selector: `require`, `verify-ca`, `verify-full`, `disable` |

### Snowflake fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Account | Yes | — | Format: `org-account_name` |
| Authenticator | Yes | — | Interactive selector: `externalbrowser` (SSO), `snowflake username & password` |
| User | Yes | — | |
| Password | Yes (if username & password auth) | — | Only prompted when authenticator is `snowflake username & password` |
| Role | Yes | — | |
| Warehouse | Yes | — | |
| Default database | No | — | Press Enter to skip |
| Default schema | No | — | Press Enter to skip |

### MySQL fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Host | Yes | — | |
| Port | No | `3306` | Press Enter to accept default |
| Default database | No | — | Press Enter to skip |
| Default schema | No | — | Press Enter to skip |
| User | Yes | — | |
| Password | Yes | — | |
| TLS Mode | Yes | — | Interactive selector: `true`, `preferred`, `skip-verify`, `false` |

### BigQuery fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| Project ID | Yes | — | Saved to `project_id` (and mirrored to `database` for compatibility) |
| Default dataset | No | — | Saved to `schema`; press Enter to skip |
| Service account JSON file path | No | — | Saved to `credentials_file`; leave blank to use ADC |

## Snowflake example

### SSO (External Browser)

```
$ dbh init
Installed .dbharness to /path/to/project/.dbharness

Connection name: analytics
? Database type: snowflake
? Environment: production
Account (e.g. org-account_name): myorg-myaccount
? Authenticator: externalbrowser
User: jsmith@company.com
Role: ANALYST
Warehouse: COMPUTE_WH
Default database (optional, press Enter to skip): ANALYTICS
Default schema (optional, press Enter to skip): PUBLIC

Testing connection to analytics...
Opening browser for SSO authentication...
Connection ok!

Added "analytics" to /path/to/project/.dbharness/config.json
```

### Username & Password

```
$ dbh init
Installed .dbharness to /path/to/project/.dbharness

Connection name: snowflake-svc
? Database type: snowflake
? Environment: development
Account (e.g. org-account_name): myorg-myaccount
? Authenticator: snowflake username & password
User: SVC_ACCOUNT
Password: secret123
Role: TRANSFORMER
Warehouse: COMPUTE_WH
Default database (optional, press Enter to skip): TRANSFORMATIONS
Default schema (optional, press Enter to skip): dbt_dev

Testing connection to snowflake-svc...
Connection ok!

Added "snowflake-svc" to /path/to/project/.dbharness/config.json
```

## Re-initializing with `--force`

To discard an existing `.dbharness/` folder and start fresh:

```bash
dbh init --force
```

When `.dbharness/` already exists, this first creates a full timestamped backup in `.dbharness-snapshots/<yyyymmdd_hhmm_ss>/`, then deletes `.dbharness/`, creates a new one, and prompts for the first connection again.

## Config file

Connections are stored in `.dbharness/config.json`:

Postgres example:

```json
{
  "connections": [
    {
      "name": "my-db",
      "environment": "local",
      "type": "postgres",
      "primary": true,
      "database": "myapp",
      "user": "postgres",
      "host": "localhost",
      "port": 5432,
      "password": "secret",
      "sslmode": "disable"
    }
  ]
}
```

Snowflake example (SSO):

```json
{
  "connections": [
    {
      "name": "analytics",
      "environment": "production",
      "type": "snowflake",
      "primary": true,
      "database": "ANALYTICS",
      "user": "jsmith@company.com",
      "account": "myorg-myaccount",
      "role": "ANALYST",
      "warehouse": "COMPUTE_WH",
      "schema": "PUBLIC",
      "authenticator": "externalbrowser"
    }
  ]
}
```

Snowflake example (username & password):

```json
{
  "connections": [
    {
      "name": "snowflake-svc",
      "type": "snowflake",
      "primary": false,
      "database": "TRANSFORMATIONS",
      "user": "SVC_ACCOUNT",
      "password": "secret123",
      "account": "myorg-myaccount",
      "role": "TRANSFORMER",
      "warehouse": "COMPUTE_WH",
      "schema": "dbt_dev",
      "authenticator": "snowflake"
    }
  ]
}
```

The `environment` field is omitted from the JSON when left blank. Type-specific
fields are omitted when not applicable (e.g. `host`, `port`, `sslmode` for
Postgres; `account`, `role`, `warehouse`, `schema`, `authenticator` for
Snowflake; `host`, `port`, `tls` for MySQL; `project_id`, `credentials_file`
for BigQuery. Redshift uses the same `host`, `port`, `database`, `user`,
`password`, and `sslmode` fields as Postgres, but defaults to port `5439` and
typically uses `sslmode: require`.)
