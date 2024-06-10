#!/bin/bash

set +x
echo Checksum for ${1}:
echo ${!1} | shasum