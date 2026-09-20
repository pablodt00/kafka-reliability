"""The replay CLI — a thin layer over the Python API (behind the [cli] extra).

Python API first, so replays can be scripted, reviewed in a PR and run from CI;
the CLI adds no behaviour of its own. It is a dry run unless you pass
`--execute`, which shows source, target, count and rate and asks to confirm
(`--yes` for scripts). There is no delete and no transform.

    kafka-reliability-replay --bootstrap-servers kafka:9092 --topic orders.dlq \\
        --to-topic orders.replay-test --from 2026-09-04T00:00Z --to 2026-09-04T06:00Z \\
        --where "x-dlq-error-type == 'TimeoutError'"

Predicate filtering is client-side: the whole selected range is read however few
records match. Fine at DLQ volumes; do not point it at a high-volume primary topic.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kafka_reliability.core.errors import KafkaReliabilityError, require_extra
from kafka_reliability.replay.expr import parse_header_filter
from kafka_reliability.replay.runner import ConfirmationRequired, ReplayOptions, ReplayRunner
from kafka_reliability.replay.selector import ReplaySelector, Selection

try:
    import click
except ImportError as exc:
    require_extra(package="click", extra="cli", cause=exc)


def _timestamp(_ctx: Any, _param: Any, value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise click.BadParameter(f"{value!r} is not an ISO-8601 timestamp") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(frozen=True)
class Factories:
    """How the CLI obtains its Kafka clients. Passed as click's `obj`, so this
    module depends on the `Producer` and `Reader` protocols and never on a
    concrete adapter; `kafka_reliability.contrib.replay_cli` supplies the
    aiokafka-backed defaults."""

    reader: Callable[[str, str | None], Any]
    producer: Callable[[str], Any]


@click.command(
    name="kafka-reliability-replay",
    help="Replay records from one topic to another. Dry run unless --execute.",
)
@click.option("--bootstrap-servers", required=True)
@click.option("--topic", "source", required=True, help="Source topic (e.g. the DLQ).")
@click.option("--to-topic", required=True, help="Target topic. Required; never inferred.")
@click.option("--partitions", default=None, help="Comma-separated partitions (default: all).")
@click.option("--from-offset", type=int, default=None, help="Inclusive; needs one --partitions.")
@click.option("--to-offset", type=int, default=None, help="Exclusive; needs one --partitions.")
@click.option("--from", "from_ts", callback=_timestamp, default=None, help="ISO-8601 start.")
@click.option("--to", "to_ts", callback=_timestamp, default=None, help="ISO-8601 end (exclusive).")
@click.option("--where", "where", default=None, help="Header filter: name == 'value' [and ...].")
@click.option(
    "--rate", type=float, default=100.0, show_default=True, help="Messages/second; 0 = unlimited."
)
@click.option("--max", "max_messages", type=int, default=None, help="Stop after this many.")
@click.option("--max-replay-count", type=int, default=3, show_default=True)
@click.option("--allow-same-topic", is_flag=True)
@click.option("--commit", "commit", is_flag=True, help="Commit source offsets (off by default).")
@click.option("--group-id", default=None, help="Consumer group for --commit.")
@click.option("--audit-log", type=click.Path(path_type=Path), default=None)
@click.option("--execute", is_flag=True, help="Really produce. Without it: dry run.")
@click.option("--yes", is_flag=True, help="Skip the --execute confirmation prompt.")
@click.pass_obj
def main(factories: Factories, /, **o: Any) -> None:
    partitions = [int(p) for p in o["partitions"].split(",")] if o["partitions"] else None
    if (o["from_offset"] is not None or o["to_offset"] is not None) and (
        partitions is None or len(partitions) != 1
    ):
        raise click.UsageError("--from-offset/--to-offset are per partition: give one --partitions")
    part = partitions[0] if partitions else None
    try:
        selection = Selection(
            topic=o["source"],
            partitions=partitions,
            from_offset={part: o["from_offset"]} if o["from_offset"] is not None else None,  # type: ignore[dict-item]
            to_offset={part: o["to_offset"]} if o["to_offset"] is not None else None,  # type: ignore[dict-item]
            from_timestamp=o["from_ts"],
            to_timestamp=o["to_ts"],
            predicate=parse_header_filter(o["where"]) if o["where"] else None,
            max_messages=o["max_messages"],
        )
        options = ReplayOptions(
            target_topic=o["to_topic"],
            rate_per_second=o["rate"] or None,
            max_replay_count=o["max_replay_count"],
            allow_same_topic=o["allow_same_topic"],
            commit_source_offsets=o["commit"],
            audit_path=o["audit_log"],
            assume_yes=o["yes"],
        )
        asyncio.run(_run(factories, o, selection, options))
    except (KafkaReliabilityError, ConfirmationRequired) as exc:
        raise click.ClickException(str(exc)) from exc


async def _run(
    factories: Factories, o: dict[str, Any], selection: Selection, options: ReplayOptions
) -> None:
    reader = factories.reader(o["bootstrap_servers"], o["group_id"])
    producer = factories.producer(o["bootstrap_servers"])
    async with reader, producer:
        runner = ReplayRunner(
            selector=ReplaySelector(reader=reader),
            producer=producer,
            options=options,
            confirm=lambda resolved: click.confirm(
                f"Replay up to {resolved.total_records} messages?", default=False
            ),
        )
        plan = await runner.dry_run(selection)
        click.echo(plan.format())
        if not o["execute"]:
            return
        if not o["yes"]:
            rate = options.rate_per_second or "unlimited"
            click.confirm(
                f"Replay {plan.matched} messages from '{selection.topic}' to "
                f"'{options.target_topic}' at {rate}/s?",
                abort=True,
            )
        result = await runner.execute(selection)
        click.echo(f"produced {result.produced} messages (replay {result.plan.replay_id})")
