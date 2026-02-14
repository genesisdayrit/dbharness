# Testing & Releases

## Testing locally

### Build from source

```bash
go build -o dbh ./cmd/dbh
```

This produces a `dbh` binary in the current directory. Run it directly:

```bash
./dbh init
./dbh test-connection -s default
```

### Install locally via `go install`

From the repo root, this builds and places the binary in `~/go/bin/`:

```bash
go install ./cmd/dbh
```

### Homebrew vs local build

If you also have dbh installed via Homebrew, both binaries exist on your
system and can collide. Homebrew typically installs to `/opt/homebrew/bin/`
(Apple Silicon) or `/usr/local/bin/` (Intel), while `go install` places it in
`~/go/bin/`. Whichever directory appears first in your `$PATH` wins when you
run `dbh` without a full path.

Check which version is active:

```bash
which -a dbh
# example output:
#   /opt/homebrew/bin/dbh    <-- Homebrew (released version)
#   /Users/you/go/bin/dbh   <-- local build (dev version)
```

To test your local build without affecting the Homebrew install, use the full path:

```bash
~/go/bin/dbh init
```

To temporarily unlink the Homebrew version so `dbh` resolves to your
local build:

```bash
brew unlink dbh
# now "dbh" runs ~/go/bin/dbh
dbh init

# when done, restore the Homebrew version
brew link dbh
```

### Test in an isolated directory

To avoid touching your real project configs:

```bash
mkdir /tmp/dbh-test && cd /tmp/dbh-test
~/go/bin/dbh init
# run init again to test the add-database flow
~/go/bin/dbh init
# clean up
rm -rf /tmp/dbh-test
```

## Testing a release before tagging

### Dry-run with GoReleaser

GoReleaser has a `--snapshot` flag that builds everything without publishing or requiring a git tag:

```bash
goreleaser release --snapshot --clean
```

This will:
- Build binaries for all platforms (darwin/linux, amd64/arm64)
- Create archives in the `dist/` directory
- Skip publishing to GitHub Releases and Homebrew tap

Check the output:

```bash
ls dist/
# You'll see:
#   dbh_<version>-SNAPSHOT_darwin_arm64.tar.gz
#   dbh_<version>-SNAPSHOT_darwin_amd64.tar.gz
#   dbh_<version>-SNAPSHOT_linux_arm64.tar.gz
#   dbh_<version>-SNAPSHOT_linux_amd64.tar.gz
```

You can extract and test a specific build:

```bash
tar -xzf dist/dbh_*_darwin_arm64.tar.gz -C /tmp
/tmp/dbh init
```

### Install GoReleaser (if needed)

```bash
brew install goreleaser
```

## Publishing a release

Once you're confident everything works:

1. Merge your PR to `main`
2. Tag and push:

```bash
git checkout main
git pull origin main
git tag v0.x.x
git push origin v0.x.x
```

3. The GitHub Actions workflow (`.github/workflows/release.yml`) triggers automatically on `v*` tags and:
   - Builds binaries for macOS and Linux (Intel + ARM)
   - Creates a GitHub Release with the archives
   - Publishes the updated Homebrew formula to `genesisdayrit/tap`

4. Users receive the update via:

```bash
brew upgrade dbh
```

### Required secrets

The release workflow needs these GitHub repository secrets configured:
- `GITHUB_TOKEN` -- provided automatically by GitHub Actions
- `HOMEBREW_TAP_GITHUB_TOKEN` -- a PAT with write access to the `genesisdayrit/tap` repo
