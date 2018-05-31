#!/bin/sh

set -eu

cd "$(dirname "$(readlink -f "$0")")"

export PYTHONPATH="$PWD/src:${PYTHONPATH:-}"

exec python3 -m unittest discover -s "$PWD/test"
