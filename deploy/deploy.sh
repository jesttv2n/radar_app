#!/usr/bin/env bash
# Lokal deploy: bygger pysteps-wheel hvis den mangler, sender den og kører remote-deploy.sh på serveren.
set -euo pipefail
cd "$(dirname "$0")/.."
HOST=root@46.225.104.143
APP=/var/www/services/radar
ls deploy/wheels/pysteps-*.whl >/dev/null 2>&1 || bash deploy/build-wheels.sh
ssh "$HOST" "mkdir -p $APP/deploy/wheels"
scp -q deploy/wheels/*.whl "$HOST:$APP/deploy/wheels/"
ssh "$HOST" 'bash -s' < deploy/remote-deploy.sh
