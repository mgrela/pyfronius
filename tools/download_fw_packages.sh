#!/bin/bash

unset verbose

list_versions() {
    newest_version="3.16.7-1"
    # First, download the newest changelog
    curl http://www3.fronius.com/datalogger_web/dmc/updates/$newest_version/changelog.txt | grep "^Software Version:" \
        | awk '{ if ($3 == "Beta") { print $4; } else { print $3; } }' | sed -e 's/^V//' | tr -d "\r"
}

list_versions | tail -n 23 | while read version; do
    echo "Downloading all packages for version '$version'" >&2

    # First check if pkg0 exists
    pkg0_content=$(curl -s http://www3.fronius.com/datalogger_web/dmc/updates/$version/pkg0.fpk --output -)
    if [ "$pkg0_content" == "not found" ]; then
        echo "pkg0 is empty, version is likely not hosted" >&2
        continue
    fi

    max_pkgnum=100
    ( for i in `seq 0 $max_pkgnum`; do
        url="http://www3.fronius.com/datalogger_web/dmc/updates/$version/pkg$i.fpk"
        echo "$url" # For wget
        [ -n "$verbose" ] && echo $url >&2 # For debug
    done ) | wget --progress=dot --show-progress --no-verbose -m -i -
done


