#!/usr/bin/env bash

# This script is used to fetch the JSON/YML tests from the drivers specifications repo.
# The first argument is the specification whose tests should be fetched. It is required.
# The second argument is the branch/commit hash that the tests should be synced to. If it
# is omitted, it will default to "master".
#
# This script puts the tests in the directory $reporoot/src/test/spec/json/$specname. It
# must be run from the root of the repository.

set -o errexit
set -o nounset

if [ ! -d ".git" ]; then
    echo "$0: This script must be run from the root of the repository" >&2
    exit 1
fi

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <specification> [branch/commit hash]"
    exit 1
fi

REF="${2:-master}"

tmpdir=`perl -MFile::Temp=tempdir -wle 'print tempdir(TMPDIR => 1, CLEANUP => 0)'`
curl -sL "https://github.com/mongodb/specifications/archive/$REF.zip" -o "$tmpdir/specs.zip"
unzip -q -d "$tmpdir" "$tmpdir/specs.zip"
mkdir -p "src/test/spec/json/$1"
rsync -ah "$tmpdir/specifications-$REF"*"/source/$1/tests/" "src/test/spec/json/$1" --delete

if [ "$1" = "client-side-encryption" ]; then
    mkdir -p "src/test/spec/json/testdata/$1/data"
    rsync -ah "$tmpdir/specifications-$REF"*"/source/$1/etc/data/" "src/test/spec/json/testdata/$1/data" --delete
    mkdir -p "src/test/spec/json/testdata/$1/corpus"
    rsync -ah "$tmpdir/specifications-$REF"*"/source/$1/corpus/" "src/test/spec/json/testdata/$1/corpus" --delete
    mkdir -p "src/test/spec/json/testdata/$1/external"
    rsync -ah "$tmpdir/specifications-$REF"*"/source/$1/external/" "src/test/spec/json/testdata/$1/external" --delete
    mkdir -p "src/test/spec/json/testdata/$1/limits"
    rsync -ah "$tmpdir/specifications-$REF"*"/source/$1/limits/" "src/test/spec/json/testdata/$1/limits" --delete
fi

rm -rf "$tmpdir"
