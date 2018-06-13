#!/bin/sh

cd "$(dirname "$(readlink -f "$0")")"

exec mypy --strict-optional --ignore-missing-imports src/*.py
