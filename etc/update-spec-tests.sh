#!/usr/bin/env bash

# Cherry-picks the test changes from commits in the drivers specifications repo into spec/.
#
# Usage: etc/update-spec-tests.sh <commit>
#
# Applies the given commit's changes (like `git cherry-pick`) as a three-way merge, so conflicts
# with local modifications show up as ordinary conflict markers in the working tree.
#
# Only the paths listed in the MAPPINGS table below are considered; everything else in the
# specifications repo is ignored.  Must be run from the root of the repository, with no
# uncommitted changes under spec/.

set -o errexit
set -o nounset
set -o pipefail

UPSTREAM_REMOTE="specs-upstream"
UPSTREAM_URL="${SPECS_UPSTREAM_URL:-https://github.com/mongodb/specifications.git}"

# "<upstream path>:<local path>", one per line.  Test directories for each spec are derived from
# the contents of spec/; extra mappings are listed explicitly.
mappings() {
    for dir in spec/*/; do
        name="$(basename "$dir")"
        [ "$name" = "testdata" ] && continue
        echo "source/$name/tests:spec/$name"
    done
    for sub in data corpus external limits; do
        upstream="etc/data"
        [ "$sub" != "data" ] && upstream="$sub"
        echo "source/client-side-encryption/$upstream:spec/testdata/client-side-encryption/$sub"
    done
}

# Upstream paths that are never synced.
EXCLUDES=(
    "source/client-side-encryption/tests/legacy"
)

if [ ! -d ".git" ]; then
    echo "$0: This script must be run from the root of the repository" >&2
    exit 1
fi

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <commit>" >&2
    exit 1
fi

if ! git diff --quiet HEAD -- spec; then
    echo "$0: spec/ has uncommitted changes; commit or stash them first" >&2
    exit 1
fi

if ! git remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1; then
    git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
    git config "remote.$UPSTREAM_REMOTE.partialclonefilter" blob:none
    git config "remote.$UPSTREAM_REMOTE.promisor" true
fi
git fetch --quiet --no-tags "$UPSTREAM_REMOTE" master
tip="$(git rev-parse "$1")"
base="$(git rev-parse "$tip^")"

exclude_args=()
for path in "${EXCLUDES[@]}"; do
    exclude_args+=(":(exclude)$path")
done

patch="$(mktemp)"
trap 'rm -f "$patch"' EXIT

applied=0
failed=()
skipped=()
# Loop over mappings:
while IFS=: read -r upstream local_path; do
    # For each changed file in the upstream half of the mapping
    while IFS=$'\t' read -r status rel; do
        [ -z "$status" ] && continue
        dest="$local_path/$rel"
        # Ignore deleted files not present locally
        if [ "$status" = "D" ] && [ ! -e "$dest" ]; then
            continue
        fi
        # Skip but warn about changed files we don't have locally
        if [ "$status" != "A" ] && [ ! -e "$dest" ]; then
            skipped+=("$dest")
            continue
        fi
        # Record the upstream patch and apply it locally
        git diff --full-index --no-renames --relative="$upstream" \
            --src-prefix="a/$local_path/" --dst-prefix="b/$local_path/" \
            "$base" "$tip" -- "$upstream/$rel" >"$patch"
        if git apply -p1 --3way --quiet "$patch" 2>/dev/null; then
            applied=$((applied + 1))
        else
            failed+=("$dest")
        fi
    done < <(git diff --name-status --no-renames --relative="$upstream" "$base" "$tip" \
                 -- "$upstream" "${exclude_args[@]}")
done < <(mappings)

echo "Upstream: $(git log -1 --format='mongodb/specifications@%h %s' "$tip")"
echo "Applied cleanly: $applied files"
if [ "${#skipped[@]}" -gt 0 ]; then
    echo "Skipped (changed upstream but not present locally): ${#skipped[@]}"
    printf '  %s\n' "${skipped[@]}"
fi
if [ "${#failed[@]}" -gt 0 ]; then
    echo "Conflicts or failures: ${#failed[@]} (resolve conflict markers, see 'git status')" >&2
    printf '  %s\n' "${failed[@]}" >&2
    exit 1
fi
