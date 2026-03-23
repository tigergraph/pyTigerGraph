#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -b, --build       Build the package only
  -u, --upload      Upload the existing dist/ to PyPI only
  -a, --all         Build then upload (default when no option is given)
  -h, --help        Show this help message

Examples:
  $(basename "$0")            # build + upload
  $(basename "$0") --build    # build only
  $(basename "$0") --upload   # upload only
  $(basename "$0") --all      # build + upload (explicit)
EOF
}

DO_BUILD=false
DO_UPLOAD=false

if [[ $# -eq 0 ]]; then
    DO_BUILD=true
    DO_UPLOAD=true
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--build)  DO_BUILD=true ;;
        -u|--upload) DO_UPLOAD=true ;;
        -a|--all)    DO_BUILD=true; DO_UPLOAD=true ;;
        -h|--help)   usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

if $DO_BUILD; then
    echo "---- Removing old dist ----"
    rm -rf dist

    echo "---- Building new package ----"
    python3 -m build
fi

if $DO_UPLOAD; then
    if [[ ! -d dist ]] || [[ -z "$(ls dist/)" ]]; then
        echo "Error: dist/ is empty or missing. Run with --build first." >&2
        exit 1
    fi

    echo "---- Uploading to PyPI ----"
    python3 -m twine upload dist/*
fi
