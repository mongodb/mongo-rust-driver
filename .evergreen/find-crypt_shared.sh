#!/usr/bin/env bash

if [[ "Windows_NT" == "$OS" ]]; then
    cygpath ${PROJECT_DIRECTORY}/crypt_shared/bin/mongo_crypt_v1.dll --windows
else
    CS_PATH=${PROJECT_DIRECTORY}/crypt_shared/lib
    crypt_shared_glob=("$CS_PATH"/*)

    if [ "${#crypt_shared_glob[@]}" != "1" ]; then
        echo "Wrong number of files found: ${crypt_shared_glob[@]}"
        exit 1
    fi

    echo ${crypt_shared_glob[0]}
fi