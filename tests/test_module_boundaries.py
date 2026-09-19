"""Enforce the module-independence rules of docs/claude/05-architecture.md
("The organising constraint") and the CI mitigation named in
docs/claude/06-decisions.md D10.

These pass vacuously today (nothing violates them yet) and are regression
guards: each is designed to fail the moment a violation is introduced.
"""

from __future__ import annotations

import ast
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

import kafka_reliability
from kafka_reliability.core.errors import MissingExtraError

PKG_ROOT = Path(kafka_reliability.__file__).resolve().parent
ISOLATED = {"outbox", "dedup", "replay"}


def _imports_of(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_outbox_dedup_replay_never_import_one_another():
    violations: list[str] = []
    for pkg in ISOLATED:
        for path in (PKG_ROOT / pkg).rglob("*.py"):
            for imp in _imports_of(path):
                for other in ISOLATED - {pkg}:
                    prefix = f"kafka_reliability.{other}"
                    if imp == prefix or imp.startswith(prefix + "."):
                        violations.append(f"{path.relative_to(PKG_ROOT.parent)}: imports {imp}")
    assert not violations, "Cross-module imports found:\n" + "\n".join(violations)


def test_core_imports_stdlib_only():
    stdlib = sys.stdlib_module_names
    violations: list[str] = []
    for path in (PKG_ROOT / "core").rglob("*.py"):
        for imp in _imports_of(path):
            root = imp.split(".")[0]
            if root != "kafka_reliability" and root not in stdlib:
                violations.append(
                    f"{path.relative_to(PKG_ROOT.parent)}: imports {imp} (not stdlib)"
                )
    assert not violations, "core imports outside stdlib found:\n" + "\n".join(violations)


def test_outbox_writer_import_does_not_pull_in_kafka_client():
    for mod in ("aiokafka", "confluent_kafka", "kafka_reliability.outbox.writer"):
        sys.modules.pop(mod, None)

    importlib.import_module("kafka_reliability.outbox.writer")

    leaked = [m for m in ("aiokafka", "confluent_kafka") if m in sys.modules]
    assert not leaked, f"Kafka client(s) leaked into sys.modules via outbox.writer: {leaked}"


def test_no_kafka_client_after_importing_every_outbox_write_path_module():
    # A fresh interpreter: the in-process check above is polluted by other tests.
    code = (
        "import sys\n"
        "import kafka_reliability.outbox, kafka_reliability.outbox.schema, "
        "kafka_reliability.outbox.writer\n"
        "for m in ('asyncpg', 'psycopg', 'sqlalchemy', 'django'):\n"
        "    try:\n"
        "        __import__(f'kafka_reliability.outbox.backends.{m}')\n"
        "    except ImportError:\n"
        "        pass\n"
        "leaked = [m for m in ('aiokafka', 'confluent_kafka', 'kafka') if m in sys.modules]\n"
        "sys.exit(f'leaked: {leaked}' if leaked else 0)\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout


def test_missing_extra_raises_clear_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "asyncpg", None)
    sys.modules.pop("kafka_reliability.outbox.backends.asyncpg", None)

    with pytest.raises(MissingExtraError, match="outbox-asyncpg"):
        importlib.import_module("kafka_reliability.outbox.backends.asyncpg")

    sys.modules.pop("kafka_reliability.outbox.backends.asyncpg", None)


def test_producer_port_and_memory_import_only_stdlib_and_core():
    stdlib = sys.stdlib_module_names
    violations: list[str] = []
    for name in ("port.py", "memory.py"):
        path = PKG_ROOT / "producers" / name
        for imp in _imports_of(path):
            root = imp.split(".")[0]
            allowed = imp == "kafka_reliability.core" or imp.startswith("kafka_reliability.core.")
            if root not in stdlib and not allowed:
                violations.append(f"{path.relative_to(PKG_ROOT.parent)}: imports {imp}")
    assert not violations, "producers.port/memory import beyond stdlib + core:\n" + "\n".join(
        violations
    )


def test_importing_producers_package_does_not_pull_in_a_kafka_client():
    for mod in ("aiokafka", "confluent_kafka", "kafka_reliability.producers"):
        sys.modules.pop(mod, None)

    importlib.import_module("kafka_reliability.producers")

    leaked = [m for m in ("aiokafka", "confluent_kafka") if m in sys.modules]
    assert not leaked, f"Kafka client(s) leaked into sys.modules via producers: {leaked}"


def test_outbox_and_replay_depend_on_the_protocol_not_a_concrete_adapter():
    adapters = ("kafka_reliability.producers.aiokafka", "kafka_reliability.producers.confluent")
    violations: list[str] = []
    for pkg in ("outbox", "replay"):
        for path in (PKG_ROOT / pkg).rglob("*.py"):
            for imp in _imports_of(path):
                if imp in adapters:
                    violations.append(f"{path.relative_to(PKG_ROOT.parent)}: imports {imp}")
    assert not violations, "Concrete producer adapter imported:\n" + "\n".join(violations)
