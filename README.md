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

```bash
dbharness init
```

To overwrite an existing folder:

```bash
dbharness init --force
```

Test a database connection (uses `.dbharness/config.json`):

```bash
dbharness test-connection -s default
```
