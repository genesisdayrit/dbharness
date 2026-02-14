# Snapshot

`dbh snapshot` creates a timestamped backup of your `.dbharness/` directory. Snapshots are saved to a `.dbharness-snapshots/` folder in the current directory.

## Snapshot everything

Running `dbh snapshot` copies the entire `.dbharness/` directory:

```
$ dbh snapshot
Snapshot saved to /path/to/project/.dbharness-snapshots/20250209_1430_22
```

This creates a full copy of all files in `.dbharness/` including `config.json`, `README.md`, and `.gitignore`.

## Snapshot config only

Running `dbh snapshot config` copies only the `config.json` file:

```
$ dbh snapshot config
Snapshot saved to /path/to/project/.dbharness-snapshots/20250209_1430_55/config.json
```

This is useful when you only need to preserve your connection configuration before making changes.

## Snapshot directory structure

Snapshots are organized by timestamp in `yyyymmdd_hhmm_ss` format:

```
.dbharness-snapshots/
  20250209_1430_22/
    config.json
    README.md
    .gitignore
  20250209_1430_55/
    config.json
```

## Notes

- The `.dbharness-snapshots/` directory is created automatically on the first snapshot.
- `.dbharness-snapshots/` is included in the project `.gitignore` to prevent committing database credentials.
- A `.dbharness/` directory must exist before snapshotting. Run `dbh init` first if you haven't already.
