#!/usr/bin/env bash
# Push main to GitHub as the BUas account that owns the repository, whichever account the
# gh CLI has active. The token goes from gh to git through a credential helper, never onto a
# command line or into a file.
set -euo pipefail
cd "$(dirname "$0")/.."
ACCOUNT=MohammadaliJaberi244437
git -c credential.helper= \
    -c "credential.helper=!f() { echo username=$ACCOUNT; echo \"password=\$(gh auth token --user $ACCOUNT)\"; }; f" \
    push -q origin main
echo "pushed to origin/main as $ACCOUNT"
