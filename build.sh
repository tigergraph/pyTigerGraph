#!/usr/bin/env bash
set -euo pipefail

RECIPE_DIR="$(cd "$(dirname "$0")/pytigergraph-recipe/recipe" && pwd)"
PYPI_PACKAGE="pytigergraph"
CONDA_FORGE_PKG="pytigergraph"
STAGED_RECIPES_DIR="${STAGED_RECIPES_DIR:-$(cd "$(dirname "$0")/../staged-recipes" 2>/dev/null && pwd || echo "")}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -b,   --build           Build the PyPI package only
  -u,   --upload          Upload the existing dist/ to PyPI only
  -a,   --all             Build then upload to PyPI (default when no option is given)
  -cb,  --conda-build       Build the conda package locally (validates the recipe)
  -cu,  --conda-upload      Upload the built conda package to anaconda.org
  -ca,  --conda-all         conda-build then conda-upload
  -cft, --conda-forge-test  Full conda-forge CI simulation via staged-recipes/build-locally.py
  -h,   --help              Show this help message

Environment variables:
  STAGED_RECIPES_DIR  Path to your conda-forge/staged-recipes clone (default: ../staged-recipes)

Examples:
  $(basename "$0")                     # PyPI build + upload (default)
  $(basename "$0") --build             # PyPI build only
  $(basename "$0") --upload            # PyPI upload only
  $(basename "$0") --conda-build       # validate conda recipe locally
  $(basename "$0") --conda-forge-test  # simulate full conda-forge CI build
EOF
}

DO_BUILD=false
DO_UPLOAD=false
DO_CONDA_BUILD=false
DO_CONDA_UPLOAD=false
DO_CONDA_FORGE_TEST=false

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
        -cft|--conda-forge-test) DO_CONDA_FORGE_TEST=true ;;
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

    # Update conda recipe meta.yaml with the new version and sha256
    PKG_VERSION=$(grep "^version" pyproject.toml | awk -F'"' '{print $2}')
    TARBALL_URL="https://pypi.org/packages/source/p/$PYPI_PACKAGE/$PYPI_PACKAGE-$PKG_VERSION.tar.gz"

    echo "---- Updating conda recipe to $PKG_VERSION ----"
    # Wait briefly for PyPI to make the tarball available
    for i in $(seq 1 30); do
        HTTP_CODE=$(curl -sL -o /dev/null -w "%{http_code}" "$TARBALL_URL")
        if [[ "$HTTP_CODE" == "200" ]]; then
            break
        fi
        echo "  Waiting for PyPI tarball to become available... ($i/30)"
        sleep 5
    done
    if [[ "$HTTP_CODE" != "200" ]]; then
        echo "Warning: could not fetch tarball from PyPI. Update meta.yaml manually." >&2
    else
        NEW_SHA=$(curl -sL "$TARBALL_URL" | sha256sum | awk '{print $1}')
        sed -i.bak "s|^  version:.*|  version: \"$PKG_VERSION\"|" "$RECIPE_DIR/meta.yaml"
        sed -i.bak "s|^  url:.*|  url: $TARBALL_URL|" "$RECIPE_DIR/meta.yaml"
        sed -i.bak "s|^  sha256:.*|  sha256: $NEW_SHA|" "$RECIPE_DIR/meta.yaml"
        sed -i.bak "s|^  # sha256:.*|  sha256: $NEW_SHA|" "$RECIPE_DIR/meta.yaml"
        rm -f "$RECIPE_DIR/meta.yaml.bak"
        echo "  ✓ Updated meta.yaml: version=$PKG_VERSION sha256=$NEW_SHA"
    fi
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
    conda build -c conda-forge "$RECIPE_DIR"
fi

if $DO_CONDA_UPLOAD; then
    if ! command -v anaconda &>/dev/null; then
        echo "Error: anaconda-client not found. Run: conda install anaconda-client" >&2
        exit 1
    fi

    CONDA_PKG=$(conda build -c conda-forge "$RECIPE_DIR" --output)
    if [[ ! -f "$CONDA_PKG" ]]; then
        echo "Error: conda package not found at $CONDA_PKG. Run --conda-build first." >&2
        exit 1
    fi

    echo "---- Uploading conda package to anaconda.org ----"
    anaconda upload --user tigergraph "$CONDA_PKG"
fi

if $DO_CONDA_FORGE_TEST; then
    if [[ -z "$STAGED_RECIPES_DIR" || ! -f "$STAGED_RECIPES_DIR/build-locally.py" ]]; then
        echo "Error: staged-recipes not found at '${STAGED_RECIPES_DIR}'." >&2
        echo "Clone it with: git clone https://github.com/conda-forge/staged-recipes.git ../staged-recipes" >&2
        echo "Or set: export STAGED_RECIPES_DIR=/path/to/staged-recipes" >&2
        exit 1
    fi
    if [[ ! -f "$STAGED_RECIPES_DIR/recipes/$CONDA_FORGE_PKG/meta.yaml" ]]; then
        echo "Error: recipe not found at $STAGED_RECIPES_DIR/recipes/$CONDA_FORGE_PKG/meta.yaml" >&2
        echo "Copy your recipe: cp $RECIPE_DIR/meta.yaml $STAGED_RECIPES_DIR/recipes/$CONDA_FORGE_PKG/meta.yaml" >&2
        exit 1
    fi
    # Detect the local platform config for build-locally.py
    _OS="$(uname -s)"
    _ARCH="$(uname -m)"
    if [[ "$_OS" == "Darwin" && "$_ARCH" == "arm64" ]]; then
        _CONFIG="osx_arm64"
    elif [[ "$_OS" == "Darwin" ]]; then
        _CONFIG="osx64"
    elif [[ "$_OS" == "Linux" && "$_ARCH" == "aarch64" ]]; then
        _CONFIG="linux_aarch64"
    else
        _CONFIG="linux64"
    fi
    echo "---- Running conda-forge CI simulation (config: $_CONFIG) ----"
    echo "Note: build-locally.py builds ALL recipes in staged-recipes/recipes/"
    cd "$STAGED_RECIPES_DIR"
    python build-locally.py "$_CONFIG"
fi
