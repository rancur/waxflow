#!/bin/bash
# Usage: ./scripts/bump-version.sh 1.2.1
#
# Single source of truth is the VERSION file. Everything that needs a version
# either reads it at runtime (sync-api/main.py reads /app/VERSION, baked in by the
# Dockerfile) or is updated here. Previously only VERSION was bumped, so main.py
# reported "2.1.0" while VERSION said 2.10.1, and the MusicBrainz user agent was
# stuck at 2.9.
set -euo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>" >&2
    exit 1
fi
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "REFUSE: '$VERSION' is not semver (X.Y.Z)" >&2
    exit 1
fi

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

if ! grep -q "^## $VERSION" CHANGELOG.md 2>/dev/null; then
    echo "WARNING: CHANGELOG.md has no '## $VERSION' section yet." >&2
    printf "Continue anyway? [y/N] "
    read -r reply
    case "$reply" in [yY]*) ;; *) echo "aborted"; exit 1;; esac
fi

echo "$VERSION" > VERSION

# MusicBrainz asks that the user agent identify the app version (it is how they
# rate-limit and contact you); keep the seeded default in step with the release.
MAJOR_MINOR="$(echo "$VERSION" | cut -d. -f1,2)"
sed -i.bak -E "s|('musicbrainz_user_agent', 'WaxFlow/)[0-9]+\.[0-9]+|\1$MAJOR_MINOR|" sync-api/init_db.py
rm -f sync-api/init_db.py.bak

echo "--- files changed ---"
git --no-pager diff --stat VERSION sync-api/init_db.py

git add VERSION sync-api/init_db.py
git commit -m "Bump version to $VERSION"
git tag "v$VERSION"
git push && git push --tags
echo "Version bumped to $VERSION"
