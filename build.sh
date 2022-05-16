#! /bin/bash

echo ---- Removing old dist ----
rm -rf dist

echo ---- Building new package ----
python3 -m build

echo ---- Uploading to pypi ----
python3 -m twine upload dist/*