#!/usr/bin/env bash
set -euo pipefail
cd "${CLAUDE_PROJECT_DIR:-.}"

if [ ! -d .venv ]; then
  python3 -m venv .venv || {
    echo "SessionStart: failed to create .venv (python3-venv missing?)" >&2
    exit 1
  }
fi

if ! .venv/bin/pip install -e ".[dev]" -q; then
  echo "SessionStart: 'pip install -e .[dev]' failed — dependencies are NOT installed; tests will not run until this is fixed." >&2
  exit 1
fi

echo "SessionStart: kafka-reliability installed in .venv with [dev] extras."
