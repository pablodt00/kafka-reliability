#!/usr/bin/env python3
"""Ad hoc check of kafka_reliability's module-independence rules
(docs/claude/05-architecture.md, "The organising constraint")."""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4] / "src" / "kafka_reliability"
ISOLATED = {"outbox", "dedup", "replay"}


def imports_of(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def rule_no_cross_imports() -> list[str]:
    violations = []
    for pkg in ISOLATED:
        for path in (ROOT / pkg).rglob("*.py"):
            for imp in imports_of(path):
                for other in ISOLATED - {pkg}:
                    prefix = f"kafka_reliability.{other}"
                    if imp == prefix or imp.startswith(prefix + "."):
                        violations.append(f"{path.relative_to(ROOT.parent)}: imports {imp}")
    return violations


def rule_core_is_stdlib_only() -> list[str]:
    stdlib = sys.stdlib_module_names
    violations = []
    for path in (ROOT / "core").rglob("*.py"):
        for imp in imports_of(path):
            root = imp.split(".")[0]
            if root != "kafka_reliability" and root not in stdlib:
                violations.append(f"{path.relative_to(ROOT.parent)}: imports {imp} (not stdlib)")
    return violations


def rule_outbox_write_path_no_kafka_client() -> list[str]:
    import importlib

    sys.path.insert(0, str(ROOT.parent))
    for mod in ("aiokafka", "confluent_kafka"):
        sys.modules.pop(mod, None)
    importlib.import_module("kafka_reliability.outbox.writer")
    leaked = [m for m in ("aiokafka", "confluent_kafka") if m in sys.modules]
    return [f"kafka_reliability.outbox.writer import pulled in {m}" for m in leaked]


def main() -> int:
    violations = (
        rule_no_cross_imports()
        + rule_core_is_stdlib_only()
        + rule_outbox_write_path_no_kafka_client()
    )
    if violations:
        print("Module-boundary violations:")
        for v in violations:
            print(f"  - {v}")
        return 1
    print("All module-boundary rules hold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
