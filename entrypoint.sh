#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  echo "entrypoint.sh: no command provided" >&2
  exit 64
fi

exec "$@"
