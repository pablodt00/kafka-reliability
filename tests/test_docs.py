"""Docs guardrails: "exactly once" without the qualification is a bug
(00-overview.md), and the docs must not point at files that do not exist."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PATTERN = re.compile(r"exactly[\s-]once", re.IGNORECASE)
# The claim is qualified if the surrounding text negates it, scopes it, or names the
# honest target instead.
QUALIFIERS = re.compile(
    r"\bnever\b|\bnot\b|n't|\bno\b|effectively|at-least-once|without|\bwithin\b|"
    r"unqualified|\bbug\b|promise|rules? out|only|quot",
    re.IGNORECASE,
)


def _files() -> list[Path]:
    files = [ROOT / "README.md", ROOT / "CHANGELOG.md", ROOT / "CLAUDE.md"]
    files += sorted((ROOT / "docs").rglob("*.md"))
    files += sorted((ROOT / "src").rglob("*.py"))
    return [f for f in files if f.exists()]


def test_no_unqualified_exactly_once_anywhere():
    bad: list[str] = []
    for path in _files():
        lines = path.read_text().splitlines()
        for i, line in enumerate(lines):
            if PATTERN.search(line):
                window = " ".join(lines[max(0, i - 2) : i + 3])
                if not QUALIFIERS.search(window):
                    bad.append(f"{path.relative_to(ROOT)}:{i + 1}: {line.strip()}")
    assert not bad, "unqualified 'exactly once':\n" + "\n".join(bad)


@pytest.mark.parametrize("doc", ["README.md", "docs/quickstart.md"])
def test_relative_links_resolve(doc: str):
    path = ROOT / doc
    for target in re.findall(r"\]\((?!https?:|#)([^)#]+)", path.read_text()):
        assert (path.parent / target).exists(), f"{doc} links to missing {target}"
