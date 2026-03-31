#!/bin/bash
# Usage: ./scripts/bump-version.sh 1.2.1
VERSION=$1
if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>"
    exit 1
fi
echo "$VERSION" > VERSION
git add VERSION
git commit -m "Bump version to $VERSION"
git tag "v$VERSION"
git push && git push --tags
echo "Version bumped to $VERSION"
