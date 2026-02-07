#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ZIG_DIR="$PROJECT_DIR/local_data/zig"

# Check if zig is already installed and working
if [ -x "$ZIG_DIR/zig" ]; then
    VERSION=$("$ZIG_DIR/zig" version 2>/dev/null || true)
    if [ -n "$VERSION" ]; then
        echo "Zig is already installed: $VERSION"
        echo "Location: $ZIG_DIR/zig"
        echo ""
        echo "To use this Zig, add it to your PATH:"
        echo "  export PATH=\"$ZIG_DIR:\$PATH\""
        exit 0
    fi
fi

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)  ZIG_ARCH="x86_64" ;;
    aarch64) ZIG_ARCH="aarch64" ;;
    arm64)   ZIG_ARCH="aarch64" ;;  # macOS reports arm64
    *)
        echo "Error: Unsupported architecture: $ARCH" >&2
        exit 1
        ;;
esac

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Linux)  ZIG_OS="linux" ;;
    Darwin) ZIG_OS="macos" ;;
    *)
        echo "Error: Unsupported OS: $OS" >&2
        exit 1
        ;;
esac

PLATFORM="${ZIG_ARCH}-${ZIG_OS}"
echo "Detected platform: $PLATFORM"

# Fetch the download index
echo "Fetching Zig download index..."
INDEX_JSON=$(curl -sS --fail https://ziglang.org/download/index.json)

# Extract the master build tarball URL for our platform
TARBALL_URL=$(echo "$INDEX_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
master = data.get('master', {})
platform = master.get('${PLATFORM}', {})
url = platform.get('tarball', '')
if not url:
    print('ERROR: No master build found for ${PLATFORM}', file=sys.stderr)
    sys.exit(1)
print(url)
")

echo "Download URL: $TARBALL_URL"

# Prepare directories
mkdir -p "$ZIG_DIR"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# Download
TARBALL_NAME=$(basename "$TARBALL_URL")
echo "Downloading $TARBALL_NAME ..."
curl -sS --fail -o "$TMP_DIR/$TARBALL_NAME" "$TARBALL_URL"

# Extract - remove old contents first
rm -rf "$ZIG_DIR"/*
echo "Extracting to $ZIG_DIR ..."

if [[ "$TARBALL_NAME" == *.tar.xz ]]; then
    tar -xJf "$TMP_DIR/$TARBALL_NAME" -C "$TMP_DIR"
elif [[ "$TARBALL_NAME" == *.tar.gz ]]; then
    tar -xzf "$TMP_DIR/$TARBALL_NAME" -C "$TMP_DIR"
else
    echo "Error: Unknown archive format: $TARBALL_NAME" >&2
    exit 1
fi

# Move extracted contents (strip the top-level directory)
EXTRACTED_DIR=$(find "$TMP_DIR" -mindepth 1 -maxdepth 1 -type d | head -1)
if [ -z "$EXTRACTED_DIR" ]; then
    echo "Error: Could not find extracted directory" >&2
    exit 1
fi
mv "$EXTRACTED_DIR"/* "$ZIG_DIR"/

# Verify installation
VERSION=$("$ZIG_DIR/zig" version)
echo ""
echo "Zig $VERSION installed successfully!"
echo "Location: $ZIG_DIR/zig"
echo ""
echo "To use this Zig, add it to your PATH:"
echo "  export PATH=\"$ZIG_DIR:\$PATH\""
