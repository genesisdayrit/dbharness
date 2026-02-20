# Connections Setup Guide

This guide covers how to configure dbh connections end-to-end, including all
currently supported connection types:

- `postgres`
- `snowflake`
- `mysql`
- `bigquery`

## General setup instructions

Use `dbh init` to create `.dbharness/config.json` and add your first connection:

```bash
dbh init
```

If `.dbharness/` already exists, run `dbh init` again to add more connections.

During setup:

1. Enter a unique connection name.
2. Select a database type.
3. Optionally set an environment (`production`, `staging`, `development`,
   `local`, `testing`, or skip).
4. Enter type-specific connection fields.
5. dbh tests the connection before saving.

If the connection test fails, nothing is written to `config.json`.

### Primary connection behavior

- On first setup, the new connection becomes `primary: true`.
- For additional connections, dbh asks whether to make the new one primary.
- Commands like `dbh schemas`, `dbh tables`, and `dbh databases` use the
  primary connection when `-s/--name` is not provided.

### Config storage

Connections are stored in:

```text
.dbharness/config.json
```

Only relevant fields are written for each connection type (`omitempty` behavior
in JSON).

## Supported connection types

| Type | Main required fields | Auth model |
|------|----------------------|-----------|
| `postgres` | `host`, `port`, `database`, `user`, `password`, `sslmode` | Username/password |
| `snowflake` | `account`, `user`, `role`, `warehouse` (+ optional `database`, `schema`) | External browser SSO or username/password |
| `mysql` | `host`, `port`, `user`, `password`, optional default database/schema, `tls` | Username/password |
| `bigquery` | `project_id`, optional default dataset (`schema`) | ADC or service account JSON file |

---

## Postgres connection setup

### Prompts

- Host (required)
- Port (default `5432`)
- Database (required)
- User (required)
- Password (required)
- SSL Mode (`require` or `disable`)

### Example config

```json
{
  "name": "app-postgres",
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
```

---

## Snowflake connection setup

### Authenticator options

- `externalbrowser` (SSO in browser)
- `snowflake` (username/password mode)

### Prompts

- Account (required)
- Authenticator (required)
- User (required)
- Password (required only for `snowflake` authenticator)
- Role (required)
- Warehouse (required)
- Default database (optional)
- Default schema (optional)

### Example config (external browser SSO)

```json
{
  "name": "analytics-snowflake",
  "environment": "production",
  "type": "snowflake",
  "primary": false,
  "database": "ANALYTICS",
  "user": "jsmith@company.com",
  "account": "myorg-myaccount",
  "role": "ANALYST",
  "warehouse": "COMPUTE_WH",
  "schema": "PUBLIC",
  "authenticator": "externalbrowser"
}
```

### Example config (username/password)

```json
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
```

---

## MySQL connection setup

### Prompts

- Host (required)
- Port (default `3306`)
- Default database (optional)
- Default schema (optional)
- User (required)
- Password (required)
- TLS mode (`true`, `preferred`, `skip-verify`, `false`)

### Example config

```json
{
  "name": "app-mysql",
  "environment": "staging",
  "type": "mysql",
  "primary": false,
  "database": "app_db",
  "schema": "public",
  "user": "app_user",
  "host": "mysql.internal",
  "port": 3306,
  "password": "secret",
  "tls": "preferred"
}
```

---

## BigQuery connection setup

### Prompts

- Project ID (required)
- Default dataset (optional; saved in `schema`)
- Service account JSON file path (optional; leave blank to use ADC)

### Authentication options

BigQuery supports:

1. **Application Default Credentials (ADC)**  
   Leave `credentials_file` blank.
2. **Service account JSON key file**  
   Set `credentials_file` to the key path.

### BigQuery config notes

- `project_id` is the main BigQuery project identifier.
- dbh also stores the same project value in `database` for compatibility with
  existing database selection workflows.
- No `host`, `port`, `user`, or `password` is required for BigQuery.

### Example config

```json
{
  "name": "warehouse-bigquery",
  "environment": "production",
  "type": "bigquery",
  "primary": false,
  "project_id": "my-gcp-project",
  "database": "my-gcp-project",
  "schema": "analytics",
  "credentials_file": "/secrets/bigquery-sa.json"
}
```

## Related commands

- `dbh test-connection -s <name>`: validate connectivity for a configured
  connection.
- `dbh ls -c`: list configured connections.
- `dbh set-default -c`: change primary connection.
