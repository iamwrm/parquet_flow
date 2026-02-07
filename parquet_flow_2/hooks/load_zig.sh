#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_DIR="$PROJECT_DIR/local_data/zig"

# Skip if already installed
if [ -x "$INSTALL_DIR/zig" ]; then
    echo "Zig already installed: $("$INSTALL_DIR/zig" version)"
    export PATH="$INSTALL_DIR:$PATH"
    exit 0
fi

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    aarch64) PLATFORM="aarch64-linux" ;;
    x86_64)  PLATFORM="x86_64-linux" ;;
    *)       echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

echo "Detected platform: $PLATFORM"

# Fetch index.json and extract master tarball URL
INDEX_URL="https://ziglang.org/download/index.json"
echo "Fetching Zig build index..."
INDEX_JSON=$(curl -sSf "$INDEX_URL")

TARBALL_URL=$(python3 -c "
import json, sys
data = json.loads(sys.stdin.read())
print(data['master']['${PLATFORM}']['tarball'])
" <<< "$INDEX_JSON")

echo "Downloading: $TARBALL_URL"

# Download and extract
mkdir -p "$INSTALL_DIR"
TMPFILE=$(mktemp /tmp/zig-download-XXXXXX.tar.xz)
trap 'rm -f "$TMPFILE"' EXIT

curl -#fL "$TARBALL_URL" -o "$TMPFILE"
tar -xJf "$TMPFILE" -C "$INSTALL_DIR" --strip-components=1

export PATH="$INSTALL_DIR:$PATH"
echo "Zig installed: $(zig version)"
echo ""
echo "To use: export PATH=\"$INSTALL_DIR:\$PATH\""
