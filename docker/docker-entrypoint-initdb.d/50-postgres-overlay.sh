#!/usr/bin/env sh

set -ea

NODE_ENV=production

UPPER_HOST=/var/run/postgresql
UPPER_USER=${POSTGRES_USER:-postgres}
UPPER_DATABASE=${POSTGRES_DB:-${UPPER_USER}}
UPPER_PASSWORD=${POSTGRES_PASSWORD:-${UPPER_USER}}

node /usr/src/app/cli.js
