#!/usr/bin/env bash
# pysteps har ingen færdige wheels; byg den i en Linux-container (glibc 2.36 <= serverens 2.39).
set -euo pipefail
cd "$(dirname "$0")/.."
version=$(grep -E '^pysteps==' requirements.txt | cut -d= -f3)
mkdir -p deploy/wheels
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd):/w" python:3.12-slim sh -c \
  "apt-get update -qq && apt-get install -y -qq build-essential >/dev/null && pip wheel -q --no-deps -w /w/deploy/wheels pysteps==$version"
ls deploy/wheels
