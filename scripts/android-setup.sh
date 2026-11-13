#!/usr/bin/env bash
set -euo pipefail

sdk_root="${ANDROID_SDK_ROOT:-${ANDROID_HOME:-${XDG_DATA_HOME:-$HOME/.local/share}/gesang/android-sdk}}"
downloads="${GESANG_ANDROID_DOWNLOADS:-${XDG_DATA_HOME:-$HOME/.local/share}/gesang/downloads}"
script_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)
        tools_archive="commandlinetools-mac_arm64-16111833_latest.zip"
        tools_digest="ad03dc49bfacfd52c110b14104ea548b8a07e830"
        tools_size="155384151"
        image_abi="arm64-v8a" ;;
    Darwin-x86_64)
        tools_archive="commandlinetools-mac_x86_64-16111833_latest.zip"
        tools_digest="112cf9618794a997ff273537d55bee02c22abffe"
        tools_size="155582504"
        image_abi="x86_64" ;;
    Linux-x86_64)
        tools_archive="commandlinetools-linux-16111833_latest.zip"
        tools_digest="e025545c62a8e64c7559119566a569fb1dec5f60"
        tools_size="181052239"
        image_abi="x86_64" ;;
    *) printf '%s\n' 'This setup requires a supported emulator host.' >&2; exit 1 ;;
esac

mkdir -p "$downloads" "$sdk_root/cmdline-tools"
if [[ ! -x "$sdk_root/cmdline-tools/23.0/bin/sdkmanager" ]]; then
    archive="$downloads/$tools_archive"
    python3 "$script_root/android-fetch.py" "https://dl.google.com/android/repository/$tools_archive" "$tools_size" "$tools_digest" "$archive"
    staging="$downloads/commandline-tools-23.0-extracted"
    mkdir -p "$staging"
    unzip -q -n "$archive" -d "$staging"
    if [[ -e "$sdk_root/cmdline-tools/23.0" ]]; then
        printf '%s\n' 'An incomplete tools directory already exists. Select another SDK directory.' >&2
        exit 1
    fi
    mv "$staging/cmdline-tools" "$sdk_root/cmdline-tools/23.0"
fi

manager="$sdk_root/cmdline-tools/23.0/bin/sdkmanager"
"$manager" --sdk_root="$sdk_root" --licenses
for package in 'platform-tools' 'platforms;android-37.2' 'build-tools;37.0.0' 'ndk;30.0.16248370' 'emulator' "system-images;android-37.2;google_apis_ps16k;$image_abi"; do
    "$sdk_root/cmdline-tools/23.0/bin/android" --no-metrics --sdk="$sdk_root" sdk install "$package"
done
rustup target add aarch64-linux-android x86_64-linux-android
printf 'SDK installed at %s\n' "$sdk_root"
