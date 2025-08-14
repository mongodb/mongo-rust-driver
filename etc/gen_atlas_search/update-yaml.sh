#!/usr/bin/env bash

set -o errexit
set -x

basedir=$(dirname $(realpath $0))
tmpdir=$(mktemp -d)

git clone --sparse --depth 1 "https://github.com/mongodb/mongo-php-library.git" "${tmpdir}" 
cd "${tmpdir}"
git sparse-checkout add generator/config/search

cd "${basedir}"
mkdir -p "yaml/search"
rsync -ah "${tmpdir}/generator/config/search/" "yaml/search" --delete

#rm -rf "${tmpdir}"