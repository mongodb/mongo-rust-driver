#!/usr/bin/env bash

if [ "$1" = "" ]; then
    echo "crypt_shared library directory required"
    exit 1
fi

crypt_shared_glob=("$1"/*)

if [ "${#crypt_shared_glob[@]}" != "1" ]; then
    echo "Wrong number of files found: ${crypt_shared_glob[@]}"
    exit 1
fi

echo ${crypt_shared_glob[0]}