#!/usr/bin/env bash
set -euo pipefail

RECIPE_DIR="$(cd "$(dirname "$0")/pytigergraph-recipe/recipe" && pwd)"
PYPI_PACKAGE="pytigergraph"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -b,   --build           Build the PyPI package only
  -u,   --upload          Upload the existing dist/ to PyPI only
  -a,   --all             Build then upload to PyPI (default when no option is given)
  -cb,  --conda-build     Build the conda package locally (validates the recipe)
  -cu,  --conda-upload    Upload the built conda package to anaconda.org
  -ca,  --conda-all       conda-build then conda-upload
  -h,   --help            Show this help message

Examples:
  $(basename "$0")                # PyPI build + upload (default)
  $(basename "$0") --build        # PyPI build only
  $(basename "$0") --upload       # PyPI upload only
  $(basename "$0") --conda-build  # validate conda recipe; auto-publishes to PyPI if needed
  $(basename "$0") --conda-all    # conda build + upload (auto-publishes to PyPI if needed)
EOF
}

DO_BUILD=false
DO_UPLOAD=false
DO_CONDA_BUILD=false
DO_CONDA_UPLOAD=false

if [[ $# -eq 0 ]]; then
    DO_BUILD=true
    DO_UPLOAD=true
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--build)         DO_BUILD=true ;;
        -u|--upload)        DO_UPLOAD=true ;;
        -cb|--conda-build)  DO_CONDA_BUILD=true ;;
        -cu|--conda-upload) DO_CONDA_UPLOAD=true ;;
        -ca|--conda-all)    DO_CONDA_BUILD=true; DO_CONDA_UPLOAD=true ;;
        -a|--all)           DO_BUILD=true; DO_UPLOAD=true ;;
        -h|--help)          usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

# ── PyPI ────────────────────────────────────────────────────────────────────

if $DO_BUILD; then
    echo "---- Removing old dist ----"
    rm -rf dist

    echo "---- Building PyPI package ----"
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

# ── Conda ───────────────────────────────────────────────────────────────────

if $DO_CONDA_BUILD; then
    if ! command -v conda-build &>/dev/null; then
        echo "Error: conda-build not found. Run: conda install conda-build" >&2
        exit 1
    fi

    # Verify the required version is already published on PyPI before proceeding.
    RECIPE_VERSION=$(grep "^  version:" "$RECIPE_DIR/meta.yaml" | awk '{print $2}' | tr -d '"')
    echo "---- Checking PyPI for $PYPI_PACKAGE==$RECIPE_VERSION ----"
    PYPI_VERSIONS=$(curl -sf "https://pypi.org/pypi/$PYPI_PACKAGE/json" | python3 -c "import sys,json; print('\n'.join(json.load(sys.stdin)['releases'].keys()))" 2>/dev/null || true)
    if ! echo "$PYPI_VERSIONS" | grep -qx "$RECIPE_VERSION"; then
        echo "  $PYPI_PACKAGE==$RECIPE_VERSION not found on PyPI. Running --all to build and publish first..."
        rm -rf dist
        python3 -m build
        python3 -m twine upload dist/*
        echo "  ✓ Published $PYPI_PACKAGE==$RECIPE_VERSION to PyPI"
    else
        echo "  ✓ Found $PYPI_PACKAGE==$RECIPE_VERSION on PyPI"
    fi

    # Compute sha256 of the tarball declared in the recipe and verify it matches.
    TARBALL_URL=$(grep "url:" "$RECIPE_DIR/meta.yaml" | awk '{print $2}')
    echo "---- Computing sha256 for $TARBALL_URL ----"
    COMPUTED_SHA=$(curl -sL "$TARBALL_URL" | sha256sum | awk '{print $1}')
    RECIPE_SHA=$(grep "sha256:" "$RECIPE_DIR/meta.yaml" | awk '{print $2}' || true)
    if [[ -n "$RECIPE_SHA" && "$COMPUTED_SHA" != "$RECIPE_SHA" ]]; then
        echo "Error: sha256 mismatch!" >&2
        echo "  recipe : $RECIPE_SHA" >&2
        echo "  actual : $COMPUTED_SHA" >&2
        exit 1
    fi
    if [[ -z "$RECIPE_SHA" ]]; then
        echo "Warning: no sha256 in recipe. For conda-forge submission add:"
        echo "  sha256: $COMPUTED_SHA"
    fi

    echo "---- Building conda package ----"
    conda build "$RECIPE_DIR"
fi

if $DO_CONDA_UPLOAD; then
    if ! command -v anaconda &>/dev/null; then
        echo "Error: anaconda-client not found. Run: conda install anaconda-client" >&2
        exit 1
    fi

    CONDA_PKG=$(conda build "$RECIPE_DIR" --output)
    if [[ ! -f "$CONDA_PKG" ]]; then
        echo "Error: conda package not found at $CONDA_PKG. Run --conda-build first." >&2
        exit 1
    fi

    echo "---- Uploading conda package to anaconda.org ----"
    anaconda upload --user tigergraph "$CONDA_PKG"
fi
