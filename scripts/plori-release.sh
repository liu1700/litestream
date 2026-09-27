#!/usr/bin/env bash
set -euo pipefail

tag=${1:?usage: plori-release.sh TAG OUTPUT_DIRECTORY}
output=${2:?usage: plori-release.sh TAG OUTPUT_DIRECTORY}
if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-plori\.[0-9]+$ ]]; then
  echo "invalid Plori release tag: $tag" >&2
  exit 1
fi

version=${tag#v}
commit=$(git rev-parse HEAD)
mkdir -p "$output"
output=$(cd "$output" && pwd)
stage=$(mktemp -d "$output/.build.XXXXXX")
trap 'rm -rf "$stage"' EXIT

for arch in amd64 arm64; do
  case "$arch" in
    amd64) archive_arch=x86_64 ;;
    arm64) archive_arch=arm64 ;;
  esac

  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build \
    -trimpath -ldflags "-s -w -X main.Version=$tag" \
    -o "$stage/litestream" ./cmd/litestream
  {
    printf 'version: %s\ncommit: %s\n' "$tag" "$commit"
    go version
    go version -m "$stage/litestream"
  } > "$stage/build-info.txt"
  tar -czf "$output/litestream-$version-linux-$archive_arch.tar.gz" \
    -C "$stage" litestream build-info.txt
done

cd "$output"
sha256sum "litestream-$version-linux-x86_64.tar.gz" \
  "litestream-$version-linux-arm64.tar.gz" > checksums.txt
