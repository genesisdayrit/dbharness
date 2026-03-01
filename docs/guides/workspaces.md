# Workspaces

`dbh` workspaces let you separate session logs and workspace-scoped memory by project, team, task, or domain.

## Overview

Workspace files live under:

```text
.dbharness/context/workspaces/
  default/
    logs/
    MEMORY.md
    _workspace.yml
  <workspace-name>/
    logs/
    MEMORY.md
    _workspace.yml
```

`default` is reserved and cannot be created with `dbh workspace create`.

## Create a workspace

Command:

```bash
dbh workspace create [--name <name>]
```

### Flow A: create with `--name`

```bash
dbh workspace create --name q1-revenue
```

Behavior:

1. Validate name.
2. Ensure `.dbharness/` exists.
3. Ensure workspace does not already exist.
4. Scaffold workspace files/directories.
5. Print success summary.
6. **Skip active-workspace prompt** and keep active workspace unchanged.

### Flow B: interactive create (no `--name`)

```bash
dbh workspace create
```

Behavior:

1. Prompt for workspace name: `Workspace name:`
2. Validate name (re-prompts if invalid).
3. Scaffold workspace files/directories.
4. Prompt for activation:
   - `Set "<name>" as your active workspace? (y/N):`
   - Enter defaults to **No**
5. If user answers yes, `.dbharness/config.json` is updated with:

```json
{
  "active_workspace": "<name>"
}
```

If user answers no (or presses Enter), active workspace is unchanged.

## Name validation rules

| Rule | Detail |
|---|---|
| Allowed characters | Letters (`a-z`, `A-Z`), digits (`0-9`), hyphen (`-`), underscore (`_`) |
| Max length | 64 characters |
| Reserved names | `default` |
| Uniqueness | Must not already exist at `.dbharness/context/workspaces/<name>/` |

Invalid names return:

```text
Workspace name '<name>' is invalid. Use only letters, numbers, hyphens, and underscores (max 64 characters).
```

Reserved `default` returns:

```text
'default' is a reserved workspace name. Please choose a different name.
```

## Scaffolded files

For workspace `<name>`, dbh creates:

```text
.dbharness/context/workspaces/<name>/
  logs/
  MEMORY.md
  _workspace.yml
```

`_workspace.yml` example:

```yaml
name: q1-revenue
description: ""
created_at: "2026-03-01T12:34:56Z"
```

`MEMORY.md` template:

```md
# Workspace Memory — q1-revenue

Session notes, decisions, and context specific to this workspace.
Written and maintained automatically by coding agents following the criteria in AGENTS.md.
```

## Set active workspace

Command:

```bash
dbh set-default -w
```

Interactively selects a workspace and sets it as the active workspace in `.dbharness/config.json`. The workspace list is sourced from top-level directory names under `.dbharness/context/workspaces/`.

### Behavior

| Scenario | Behavior |
|---|---|
| Multiple workspaces exist | Shows interactive menu with all workspaces; includes "Keep current" option if an active workspace is already set |
| Only one workspace exists | Prints message and tip to create more; exits without showing a menu |
| Selected workspace is already active | No write to `config.json`; prints confirmation |
| No active workspace in config | Menu shown without "Keep current" option; success message says "set to" instead of "switched from" |

### Output examples

```text
Current active workspace: "default"
```

```text
Active workspace switched from "default" to "q1-revenue" in /path/to/.dbharness/config.json
```

```text
Only one workspace exists. "default" is already the active workspace.
Tip: Create a new workspace with: dbh workspace create
```

## Error messages

| Scenario | Message |
|---|---|
| Missing `.dbharness/` | `No .dbharness directory found. Run 'dbh init' first.` |
| Duplicate workspace | `Workspace '<name>' already exists at .dbharness/context/workspaces/<name>/.` |
| Reserved workspace | `'default' is a reserved workspace name. Please choose a different name.` |
| Invalid characters/length | `Workspace name '<name>' is invalid. Use only letters, numbers, hyphens, and underscores (max 64 characters).` |
| No workspaces directory | `No workspaces directory found. Run 'dbh init' to set up the default workspace.` |

## Current scope

Implemented now:

- `dbh workspace create [--name <name>]`
- `dbh set-default -w` (interactive active workspace selection)
- Workspace scaffolding (`logs/`, `MEMORY.md`, `_workspace.yml`)
- Optional active workspace update in interactive flow

Not yet implemented:

- `dbh workspace list`
