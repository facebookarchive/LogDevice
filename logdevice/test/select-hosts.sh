#!/bin/bash

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Selects logdevice hosts matching some criteria.
# This allows for instance finding hosts on the same rack.

RACK=
TIER=
LIST_RACKS=false
QUIET=false
PROGNAME=$(basename "$0")
KEEP_TEMP=false
TEMP_FILES=

log_msg() {
    $QUIET || echo "[$(date +'%F %T')] [$PROGNAME]: $*" >&2
}

usage() {
    echo "usage: $PROGNAME -t <tier> [-h] [-k] [-l] [-q] [-r <rack>] [-v]"
}

help() {
    usage
    echo -e "-h       \tDisplay help."
    echo -e "-k       \tKeep temporary files."
    echo -e "-l       \tList racks the hosts belong to."
    echo -e "-q       \tQuiet mode. turns off message logging."
    echo -e "-t <tier>\tSpecifies which SMC logdevice tier to work on. This option is mandatory."
    echo -e "-r <rack>\tSpecifies a rack that hosts belong to. This is used to find hosts sharing the same rack."
    echo -e ""
}

cleanup() {
    if ! $KEEP_TEMP && [ -n "$TEMP_FILES" ]; then
        log_msg "Cleaning up temporary files ($TEMP_FILES)"
        for f in $TEMP_FILES; do
            [ -f "$f" ] && rm "$f"
        done
    fi
}

trap cleanup EXIT

create_temp_file() {
    local return_var=$1
    local tempval=$(mktemp)
    TEMP_FILES="$tempval $TEMP_FILES"
    eval "$return_var"="$tempval"
}

while getopts "a:dhklqr:t:v" opt; do
    case $opt in
        r)
            RACK=$OPTARG
            ;;
        t)
            TIER=$OPTARG
            ;;
        h)
            help
            exit 1
            ;;
        k)
            KEEP_TEMP=true
            ;;
        l)
            LIST_RACKS=true
            ;;
        q)
            QUIET=true
            ;;
        \?)
            echo "$0: invalid option -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

if [ -z "$TIER" ]; then
    usage
    exit 1
fi

if $QUIET; then
    exec 3>/dev/null
else
    exec 3>&2
fi

# sets pipefail so that the exit status of a pipe is the status of the last command that
# returned non-zero.
set -o pipefail

create_temp_file HOSTS
log_msg "Saving list of hosts from SMC tier $TIER into $HOSTS"
if ! smcc list-hosts --ipv6 "$TIER" 2>&3 | tr -d "[]"> "$HOSTS"; then
    log_msg "unable to fetch the list of hosts from SMC"
    exit 1
fi

# TODO: perhaps in the future:
# - validate config
# - allow selecting hosts by logdevice node ID
#

if $LIST_RACKS; then
    log_msg "Listing racks of this tier."

    # prints the list of racks
    # queries all hosts in serfcli matching eth0.ipv6 == "address we collected earlier from SMC" and outputs their rack_path,
    # sort them and print uniq values
    if ! sed s/^/eth0.ipv6==/ "$HOSTS" | serfcli get -S --fields=rack_path --csv --no-headers 2>&3 | sort | uniq; then
        log_msg "unable to get list of racks"
        exit 1
    fi
else
    # prints the list of hosts
    # queries all hosts in serfcli matching eth0.ipv6 == "address we collected earlier from SMC" and outputs their name,
    # optionally if the -r option is specified, modify the query to filter on rack_path
    # sort them and print uniq values
    EXPR="eth0.ipv6=="
    if [ -n "$RACK" ]; then
        EXPR="$RACK,$EXPR"
        log_msg "Filtering hosts of tier $TIER on rack $RACK"
    else
        log_msg "Listing all hosts of tier $TIER"
    fi

    if ! sed "s/^/$EXPR/" "$HOSTS" | serfcli get -S --fields=name --csv --no-headers 2>&3; then
        log_msg "unable to list hosts matching this query"
        exit 1
    fi
fi

exit 0
