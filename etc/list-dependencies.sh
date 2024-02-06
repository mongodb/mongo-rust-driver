#!/usr/bin/env bash

# This script lightly formats the output of `cargo tree` in a way that's easy to paste into Google Sheets:
# 
# 1. Get the output of this script into the paste buffer (on mac, ./etc/list-dependencies.sh | pbcopy)
# 2. Paste the contents into the sheet
# 3. On the clipboard icon that pops up for the pasted values, choose "Split text to columns"

cargo tree --all-features --prefix none | sort | uniq | grep -v '(*)' | sed -e 's/ /,/'