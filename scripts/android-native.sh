#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
sdk_root="${ANDROID_SDK_ROOT:-${ANDROID_HOME:-${XDG_DATA_HOME:-$HOME/.local/share}/gesang/android-sdk}}"
ndk_root="${ANDROID_NDK_HOME:-$sdk_root/ndk/30.0.16248370}"
profile="${GESANG_NATIVE_PROFILE:-release}"
profile_output="$profile"
if [[ "$profile" == "dev" ]]; then profile_output="debug"; fi
generated_root="${GESANG_NATIVE_OUTPUT:-$repository_root/apps/android/client/build/generated}"

case "$(uname -s)" in
    Darwin) host_tag="darwin-x86_64"; host_library="libgesang_client.dylib" ;;
    Linux) host_tag="linux-x86_64"; host_library="libgesang_client.so" ;;
    *) printf '%s\n' 'Native Android builds require a supported desktop host.' >&2; exit 1 ;;
esac

toolchain="$ndk_root/toolchains/llvm/prebuilt/$host_tag/bin"
if [[ ! -x "$toolchain/clang" ]]; then
    printf '%s\n' 'The pinned native toolchain is missing. Run scripts/android-setup.sh.' >&2
    exit 1
fi

cd "$repository_root"
cargo build --locked -p gesang-client
cargo run --locked -p gesang-client --features bindgen --bin gesang-bindgen -- \
    generate --library "$repository_root/target/debug/$host_library" \
    --language kotlin --no-format \
    --out-dir "$generated_root/kotlin"

for abi in ${GESANG_ANDROID_ABIS:-arm64-v8a x86_64}; do
    case "$abi" in
        arm64-v8a) target="aarch64-linux-android"; compiler="aarch64-linux-android23-clang" ;;
        x86_64) target="x86_64-linux-android"; compiler="x86_64-linux-android23-clang" ;;
        *) printf 'Unsupported ABI: %s\n' "$abi" >&2; exit 1 ;;
    esac
    linker_key="$(printf '%s' "$target" | tr '[:lower:]-' '[:upper:]_')"
    compiler_key="$(printf '%s' "$target" | tr '-' '_')"
    env "CARGO_TARGET_${linker_key}_LINKER=$toolchain/$compiler" \
        "CC_${compiler_key}=$toolchain/$compiler" \
        "AR_${compiler_key}=$toolchain/llvm-ar" \
        cargo build --locked -p gesang-client --target "$target" --profile "$profile"
    mkdir -p "$generated_root/jniLibs/$abi"
    cp "$repository_root/target/$target/$profile_output/libgesang_client.so" "$generated_root/jniLibs/$abi/libgesang_client.so"
done
