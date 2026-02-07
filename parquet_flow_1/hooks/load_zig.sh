#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
install_root="$repo_root/local_data/zig"

machine_arch="$(uname -m)"
os_name="$(uname -s | tr '[:upper:]' '[:lower:]')"

case "$machine_arch" in
  x86_64) zig_arch="x86_64" ;;
  aarch64|arm64) zig_arch="aarch64" ;;
  *)
    echo "Unsupported architecture: $machine_arch" >&2
    exit 1
    ;;
esac

case "$os_name" in
  linux|darwin) zig_os="$os_name" ;;
  *)
    echo "Unsupported operating system: $os_name" >&2
    exit 1
    ;;
esac

: "${ZIG_URL:=https://ziglang.org/builds/zig-${zig_arch}-${zig_os}-0.16.0-dev.2490+fce7878a9.tar.xz}"

mkdir -p "$install_root"

archive_name="$(basename "$ZIG_URL")"
archive_path="$install_root/$archive_name"

if [[ ! -f "$archive_path" ]]; then
  echo "Downloading Zig from $ZIG_URL"
  curl -fL "$ZIG_URL" -o "$archive_path"
else
  echo "Using cached archive $archive_path"
fi

root_entry="$(tar -tf "$archive_path" | sed -n '1p')"
root_dir="${root_entry%%/*}"
if [[ -z "$root_dir" ]]; then
  echo "Unable to detect archive root directory" >&2
  exit 1
fi

if [[ ! -d "$install_root/$root_dir" ]]; then
  echo "Extracting $archive_name into $install_root"
  tar -xf "$archive_path" -C "$install_root"
fi

ln -sfn "$install_root/$root_dir" "$install_root/current"

export PATH="$install_root/current:$PATH"

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "Zig installed at: $install_root/current"
  echo "PATH updated for this process only. To persist in your shell:"
  echo "  source hooks/load_zig.sh"
fi
