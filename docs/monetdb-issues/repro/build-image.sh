#!/usr/bin/env bash
# Build a linux/arm64 MonetDB image from a local MonetDB checkout.
#
# usage: build-image.sh MONETDB_CHECKOUT IMAGE_TAG [GIT_REF] [APPLY_PATCH]
#
#   MONETDB_CHECKOUT  path to a clone of https://github.com/MonetDB/MonetDB
#   IMAGE_TAG         e.g. monetdb-tip:local
#   GIT_REF           commit/branch/tag to build (default: origin/master)
#   APPLY_PATCH       1 to apply apply-patch.py, 0 to skip (default: 0)
#
# The source is taken with `git archive`, so the checkout's working tree and
# current branch are left untouched.
set -euo pipefail

checkout="${1:?usage: build-image.sh MONETDB_CHECKOUT IMAGE_TAG [GIT_REF] [APPLY_PATCH]}"
tag="${2:?usage: build-image.sh MONETDB_CHECKOUT IMAGE_TAG [GIT_REF] [APPLY_PATCH]}"
ref="${3:-origin/master}"
apply_patch="${4:-0}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT

cp "$here/Dockerfile" "$here/entrypoint.sh" "$here/apply-patch.py" "$ctx/"

revision="$(git -C "$checkout" rev-parse --short=10 "$ref")"
echo "building $tag from $ref ($revision)"
git -C "$checkout" archive --format=tar --prefix=source/ "$ref" \
    | gzip -1 > "$ctx/MonetDB-src.tar.gz"

docker buildx build --platform linux/arm64 --load \
    --build-arg "APPLY_PATCH=$apply_patch" \
    --tag "$tag" "$ctx"

echo "built $tag from $revision"
