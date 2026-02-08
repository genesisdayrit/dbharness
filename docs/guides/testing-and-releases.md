# Testing & Releases

## Testing locally

### Build from source

```bash
go build -o dbharness ./cmd/dbharness
```

This produces a `dbharness` binary in the current directory. Run it directly:

```bash
./dbharness init
./dbharness test-connection -s default
```

### Install locally via `go install`

From the repo root, this builds and places the binary in `~/go/bin/`:

```bash
go install ./cmd/dbharness
```

### Homebrew vs local build

If you also have dbharness installed via Homebrew, both binaries exist on your
system and can collide. Homebrew typically installs to `/opt/homebrew/bin/`
(Apple Silicon) or `/usr/local/bin/` (Intel), while `go install` places it in
`~/go/bin/`. Whichever directory appears first in your `$PATH` wins when you
run `dbharness` without a full path.

Check which version is active:

```bash
which -a dbharness
# example output:
#   /opt/homebrew/bin/dbharness    <-- Homebrew (released version)
#   /Users/you/go/bin/dbharness   <-- local build (dev version)
```

To test your local build without affecting the Homebrew install, use the full path:

```bash
~/go/bin/dbharness init
```

To temporarily unlink the Homebrew version so `dbharness` resolves to your
local build:

```bash
brew unlink dbharness
# now "dbharness" runs ~/go/bin/dbharness
dbharness init

# when done, restore the Homebrew version
brew link dbharness
```

### Test in an isolated directory

To avoid touching your real project configs:

```bash
mkdir /tmp/dbharness-test && cd /tmp/dbharness-test
~/go/bin/dbharness init
# run init again to test the add-database flow
~/go/bin/dbharness init
# clean up
rm -rf /tmp/dbharness-test
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
#   dbharness_<version>-SNAPSHOT_darwin_arm64.tar.gz
#   dbharness_<version>-SNAPSHOT_darwin_amd64.tar.gz
#   dbharness_<version>-SNAPSHOT_linux_arm64.tar.gz
#   dbharness_<version>-SNAPSHOT_linux_amd64.tar.gz
```

You can extract and test a specific build:

```bash
tar -xzf dist/dbharness_*_darwin_arm64.tar.gz -C /tmp
/tmp/dbharness init
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
brew upgrade dbharness
```

### Required secrets

The release workflow needs these GitHub repository secrets configured:
- `GITHUB_TOKEN` -- provided automatically by GitHub Actions
- `HOMEBREW_TAP_GITHUB_TOKEN` -- a PAT with write access to the `genesisdayrit/tap` repo
