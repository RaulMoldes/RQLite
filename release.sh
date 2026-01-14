#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR" && pwd)"

BINARIES=("axmos-server" "axmos-client")
DEFAULT_TARGETS=(
    "x86_64-unknown-linux-gnu"
)

VERSION=""
TARGETS=()
OUTPUT_DIR=""
CREATE_TAG=false
DRY_RUN=false
FEATURES=""

print_usage() {
    cat << EOF
AxmosDB Release Script

Usage: $(basename "$0") [OPTIONS]

Options:
    -v, --version <VERSION>    Release version (required, e.g., 0.1.0)
    -t, --target <TARGET>      Target triple (can be specified multiple times)
                               Default: linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64
    -o, --output <DIR>         Output directory (default: ./releases)
    -g, --git-tag              Create and push git tag
    -n, --dry-run              Show what would be done without executing
    -f, --features <FEATURES>  Cargo features to enable
    --no-features              Build without any features
    -h, --help                 Show this help message

Examples:
    $(basename "$0") -v 0.1.0
    $(basename "$0") -v 0.2.0 -t x86_64-unknown-linux-gnu -g
    $(basename "$0") -v 1.0.0 --dry-run

EOF
}

log_info() {
    echo "[INFO] $*"
}

log_warn() {
    echo "[WARN] $*" >&2
}

log_error() {
    echo "[ERROR] $*" >&2
}

check_dependencies() {
    local missing=()

    if ! command -v cargo &> /dev/null; then
        missing+=("cargo")
    fi

    if ! command -v sha256sum &> /dev/null && ! command -v shasum &> /dev/null; then
        missing+=("sha256sum or shasum")
    fi

    if ! command -v tar &> /dev/null; then
        missing+=("tar")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing required dependencies: ${missing[*]}"
        exit 1
    fi
}

compute_sha256() {
    local file="$1"
    if command -v sha256sum &> /dev/null; then
        sha256sum "$file" | awk '{print $1}'
    else
        shasum -a 256 "$file" | awk '{print $1}'
    fi
}

target_to_friendly_name() {
    local target="$1"
    case "$target" in
        x86_64-unknown-linux-gnu)   echo "linux-x86_64" ;;
        x86_64-pc-windows-msvc)     echo "windows-x86_64" ;;
        *)                          echo "$target" ;;
    esac
}

install_target() {
    local target="$1"
    if ! rustup target list --installed | grep -q "^$target$"; then
        log_info "Installing target: $target"
        if [[ "$DRY_RUN" == "true" ]]; then
            echo "  [DRY-RUN] rustup target add $target"
        else
            rustup target add "$target" || {
                log_warn "Failed to install target $target, skipping..."
                return 1
            }
        fi
    fi
    return 0
}

build_binary() {
    local binary="$1"
    local target="$2"
    local features="$3"

    log_info "Building $binary for $target..."

    local cargo_args=(
        "build"
        "--release"
        "--target" "$target"
        "--bin" "$binary"
    )

    if [[ -n "$features" ]]; then
        cargo_args+=("--features" "$features")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] cargo ${cargo_args[*]}"
        return 0
    fi

    cd "$PROJECT_ROOT"
    if ! cargo "${cargo_args[@]}"; then
        log_error "Failed to build $binary for $target"
        return 1
    fi

    return 0
}

package_release() {
    local binary="$1"
    local target="$2"
    local version="$3"
    local output_dir="$4"

    local friendly_name
    friendly_name=$(target_to_friendly_name "$target")
    local archive_name="${binary}-v${version}-${friendly_name}"
    local archive_path="${output_dir}/${archive_name}.tar.gz"

    local binary_path="${PROJECT_ROOT}/target/${target}/release/${binary}"

    if [[ ! -f "$binary_path" ]] && [[ "$DRY_RUN" != "true" ]]; then
        log_error "Binary not found: $binary_path"
        return 1
    fi

    log_info "Packaging $archive_name..."

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Creating $archive_path"
        return 0
    fi

    local temp_dir
    temp_dir=$(mktemp -d)
    local package_dir="${temp_dir}/${archive_name}"
    mkdir -p "$package_dir"

    cp "$binary_path" "$package_dir/"
    chmod +x "$package_dir/$binary"

    if [[ -f "${PROJECT_ROOT}/README.md" ]]; then
        cp "${PROJECT_ROOT}/README.md" "$package_dir/"
    fi

    if [[ -f "${PROJECT_ROOT}/LICENSE" ]]; then
        cp "${PROJECT_ROOT}/LICENSE" "$package_dir/"
    fi

    tar -czf "$archive_path" -C "$temp_dir" "$archive_name"

    local checksum
    checksum=$(compute_sha256 "$archive_path")
    echo "$checksum  $(basename "$archive_path")" >> "${output_dir}/checksums.txt"

    rm -rf "$temp_dir"

    log_info "Created: $archive_path"
    return 0
}

create_git_tag() {
    local version="$1"
    local tag="v${version}"

    log_info "Creating git tag: $tag"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] git tag -a $tag -m 'Release $tag'"
        echo "  [DRY-RUN] git push origin $tag"
        return 0
    fi

    if git rev-parse "$tag" &> /dev/null; then
        log_warn "Tag $tag already exists"
        return 0
    fi

    git tag -a "$tag" -m "Release $tag"
    git push origin "$tag"

    log_info "Tag $tag created and pushed"
}

main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            -t|--target)
                TARGETS+=("$2")
                shift 2
                ;;
            -o|--output)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            -g|--git-tag)
                CREATE_TAG=true
                shift
                ;;
            -n|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -f|--features)
                FEATURES="$2"
                shift 2
                ;;
            --no-features)
                FEATURES=""
                shift
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done

    if [[ -z "$VERSION" ]]; then
        log_error "Version is required. Use -v or --version."
        print_usage
        exit 1
    fi

    if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
        log_error "Invalid version format: $VERSION (expected: X.Y.Z or X.Y.Z-suffix)"
        exit 1
    fi

    if [[ ${#TARGETS[@]} -eq 0 ]]; then
        TARGETS=("${DEFAULT_TARGETS[@]}")
    fi

    if [[ -z "$OUTPUT_DIR" ]]; then
        OUTPUT_DIR="${PROJECT_ROOT}/releases/v${VERSION}"
    fi

    check_dependencies

    log_info "AxmosDB Release Script"
    log_info "Version: $VERSION"
    log_info "Targets: ${TARGETS[*]}"
    log_info "Output:  $OUTPUT_DIR"
    log_info "Features: ${FEATURES:-none}"
    [[ "$DRY_RUN" == "true" ]] && log_info "Mode: DRY-RUN"
    echo

    if [[ "$DRY_RUN" != "true" ]]; then
        mkdir -p "$OUTPUT_DIR"
        : > "${OUTPUT_DIR}/checksums.txt"
    fi

    local successful_builds=0
    local failed_builds=0

    for target in "${TARGETS[@]}"; do
        if ! install_target "$target"; then
            ((failed_builds++))
            continue
        fi

        for binary in "${BINARIES[@]}"; do
            echo "Building binary" "$binary"
            if build_binary "$binary" "$target" "$FEATURES"; then
                if package_release "$binary" "$target" "$VERSION" "$OUTPUT_DIR"; then
                    successful_builds=$((successful_builds + 1))
                else
                    failed_builds=$((failed_builds + 1))
                fi
            else
                successful_builds=$((successful_builds + 1))
            fi
        done
    done

    echo
    log_info "Build Summary:"
    log_info "  Successful: $successful_builds"
    log_info "  Failed:     $failed_builds"

    if [[ "$successful_builds" -gt 0 ]] && [[ "$DRY_RUN" != "true" ]]; then
        log_info "Checksums written to: ${OUTPUT_DIR}/checksums.txt"
        echo
        cat "${OUTPUT_DIR}/checksums.txt"
    fi

    if [[ "$CREATE_TAG" == "true" ]] && [[ "$successful_builds" -gt 0 ]]; then
        echo
        create_git_tag "$VERSION"
    fi

    if [[ "$failed_builds" -gt 0 ]]; then
        exit 1
    fi
}

main "$@"
