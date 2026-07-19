#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON_BIN=".venv/bin/python"
uv pip install --python "$PYTHON_BIN" wheel twine
uv run --python "$PYTHON_BIN" python setup.py sdist bdist_wheel
uv run --python "$PYTHON_BIN" twine upload dist/*.whl
