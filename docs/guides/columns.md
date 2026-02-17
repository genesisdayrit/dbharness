# Columns (Enriched)

`dbh columns` generates enriched `<table_name>__columns.yml` files with per-column profiling statistics.

## Quick start

```bash
# Generate enriched columns for the primary connection
dbh columns

# Generate for a specific connection
dbh columns -s my-connection
```

## Workflow

The command uses an interactive flow:

1. Warns that profiling may take minutes, then asks whether to continue.
2. Lets you select one or more databases.
3. Lets you select schemas for each selected database.
4. Lets you select tables per selected schema (including a "Select all" option).
5. Profiles each selected column and writes enriched YAML files.

## Output

For each selected table:

- file path: `.dbharness/context/connections/<connection>/databases/<database>/schemas/<schema>/<table>/<table>__columns.yml`
- file behavior: overwritten only after all columns for that table are successfully profiled
- existing `__sample.xml` files are not modified

## Enriched metrics

Each column includes:

- base metadata (`name`, `data_type`, `is_nullable`, `ordinal_position`, `column_default`)
- `ai_description` (blank placeholder for future AI-generated text)
- `db_description` (database-native description/comment when available; blank otherwise)
- `total_rows`
- `null_count`
- `non_null_count`
- `distinct_non_null_count`
- `distinct_of_non_null_pct`
- `null_of_total_rows_pct`
- `non_null_of_total_rows_pct`
- `sample_values` (up to 5 values, truncated for large payloads)

Vector-like data types skip sample values in this YAML output.
