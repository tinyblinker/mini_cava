#!/usr/bin/env bash
#
# install_deps.sh - Install dependencies for `cava_plus_plus` on Arch Linux.
#
# Native libraries required (not fetched by Cargo):
#   pkgconf    - pkg-config, used by *-sys crates to locate libraries
#   pipewire   - libpipewire-0.3 / libspa-0.2 (cpal "pipewire" backend)
#   alsa-lib   - libasound (cpal ALSA backend)
#   clang      - libclang, required by bindgen for pipewire-sys / libspa-sys

set -euo pipefail

info() { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()   { printf '\033[1;32m[OK]\033[0m    %s\n' "$*"; }

info "Installing native dependencies..."
sudo pacman -Syu --needed --noconfirm pkgconf pipewire alsa-lib clang

info "Fetching and building Rust dependencies..."
cargo fetch
cargo build

ok "Done. Run it with: cargo run"
