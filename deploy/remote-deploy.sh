#!/usr/bin/env bash
# Kører på serveren (via deploy.sh). Forudsætter at nginx-blokken og git clone allerede findes.
set -euo pipefail
APP=/var/www/services/radar
cd "$APP"

old_req=$(sha256sum requirements.txt | cut -d' ' -f1)
git pull --ff-only
new_req=$(sha256sum requirements.txt | cut -d' ' -f1)

if [ ! -x venv/bin/python ]; then
    python3.12 -m venv venv
    venv/bin/pip install -q --upgrade pip
    old_req=none
fi
if [ "$old_req" != "$new_req" ] || [ -n "${FORCE_PIP:-}" ]; then
    venv/bin/pip install -q --find-links deploy/wheels -r requirements.txt
fi

for unit in radar-worker.service radar-worker.timer; do
    install -m 644 "deploy/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload
systemctl enable -q --now radar-worker.timer
systemctl start radar-worker.service
journalctl -u radar-worker -n 5 --no-pager -o cat

# Health-check: manifestet skal være offentligt; vis tiden for nyeste observation
generated=$(curl -sf https://services.tv2nord.dk/radar/data/manifest.json | venv/bin/python -c \
    'import json,sys; print(next(f["time"] for f in json.load(sys.stdin)["frames"] if f["lead"] == 0))')
echo "nyeste observation i manifest: $generated"
