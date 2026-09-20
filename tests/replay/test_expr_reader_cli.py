from __future__ import annotations

from types import SimpleNamespace

import pytest

from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.replay.expr import parse_header_filter

from .helpers import make_reader, make_record


def r(**h):
    return make_record(0, 1, headers=[(k.replace("_", "-"), v.encode()) for k, v in h.items()])


def test_header_filter_supports_equality_inequality_and_conjunction():
    f = parse_header_filter("x-dlq-error-type == 'TimeoutError'")
    assert f(r(x_dlq_error_type="TimeoutError")) and not f(r(x_dlq_error_type="ValueError"))
    g = parse_header_filter(
        "x-dlq-consumer-group == \"billing\" and x-dlq-error-type != 'ValueError'"
    )
    assert g(r(x_dlq_consumer_group="billing", x_dlq_error_type="Timeout"))
    assert not g(r(x_dlq_consumer_group="billing", x_dlq_error_type="ValueError"))


def test_a_missing_header_never_equals_anything():
    assert not parse_header_filter("h == 'x'")(r())
    assert parse_header_filter("h != 'x'")(r())


@pytest.mark.parametrize(
    "bad", ["", "h", "h = 'x'", "h == x", "__import__('os')", "h == 'a' or h == 'b'"]
)
def test_arbitrary_code_and_unsupported_syntax_is_rejected(bad):
    with pytest.raises(ConfigurationError):
        parse_header_filter(bad)


# --- AiokafkaReader over a fake AIOKafkaConsumer ----------------------------------------------

aiokafka = pytest.importorskip("aiokafka")


class FakeConsumer:
    def __init__(self, **kwargs):
        self.kwargs, self.assigned, self.pos, self.committed = kwargs, None, 0, None
        self.log = {
            i: SimpleNamespace(
                topic="t",
                partition=0,
                offset=i,
                key=b"k",
                value=b"v",
                headers=[("h", b"1")],
                timestamp=1_767_225_600_000 + i,
            )
            for i in range(10)
        }

    async def start(self): ...
    async def stop(self): ...
    async def topics(self):
        return {"t"}

    def partitions_for_topic(self, topic):
        return {0, 1} if topic == "t" else None

    async def beginning_offsets(self, tps):
        return {tp: 0 for tp in tps}

    async def end_offsets(self, tps):
        return {tp: 10 for tp in tps}

    async def offsets_for_times(self, m):
        return {tp: SimpleNamespace(offset=3) for tp in m}

    def assign(self, tps):
        self.assigned = tps

    def seek(self, tp, offset):
        self.pos = offset

    async def position(self, tp):
        return self.pos

    async def getmany(self, tp, timeout_ms, max_records):
        batch = [self.log[i] for i in range(self.pos, min(self.pos + 4, 10))]
        self.pos += len(batch)
        return {tp: batch} if batch else {}

    async def commit(self, offsets):
        self.committed = offsets  # noqa: E704


async def test_aiokafka_reader_reads_a_bounded_range_and_never_auto_commits():
    from kafka_reliability.replay.reader_aiokafka import AiokafkaReader

    consumers = []

    def factory(**kw):
        consumers.append(FakeConsumer(**kw))
        return consumers[-1]

    async with AiokafkaReader("k:9092", group_id="g", consumer_factory=factory) as reader:
        assert consumers[0].kwargs["enable_auto_commit"] is False
        assert await reader.partitions("t") == [0, 1]
        assert await reader.end_offsets("t", [0]) == {0: 10}
        assert await reader.offsets_for_times("t", {0: 1}) == {0: 3}
        got = [rec.offset async for rec in reader.read("t", 0, 2, 7)]
        assert got == [2, 3, 4, 5, 6]
        await reader.commit("t", 0, 7)
        assert list(consumers[0].committed.values())[0].offset == 7
        with pytest.raises(ConfigurationError):
            await reader.partitions("nope")


def test_aiokafka_reader_refuses_auto_commit():
    from kafka_reliability.replay.reader_aiokafka import AiokafkaReader

    with pytest.raises(ConfigurationError):
        AiokafkaReader("k", enable_auto_commit=True)


# --- CLI -------------------------------------------------------------------------------------

click_testing = pytest.importorskip("click.testing")


@pytest.fixture
def cli(monkeypatch):
    from kafka_reliability.producers import InMemoryProducer
    from kafka_reliability.replay import cli as cli_module

    producer = InMemoryProducer()
    reader = make_reader()

    class R(type(reader)):
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a): ...

    class P(InMemoryProducer):
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a): ...

    reader.__class__ = R
    producer.__class__ = P
    factories = cli_module.Factories(reader=lambda *a: reader, producer=lambda *a: producer)
    runner = click_testing.CliRunner()
    base = ["--bootstrap-servers", "k:9092", "--topic", "orders.dlq"]
    return SimpleNamespace(
        run=lambda *args, **kw: runner.invoke(
            cli_module.main, base + list(args), obj=factories, **kw
        ),
        producer=producer,
        reader=reader,
    )


def test_the_default_is_a_dry_run_that_produces_nothing(cli):
    result = cli.run("--to-topic", "orders", "--rate", "0")
    assert result.exit_code == 0, result.output
    assert (
        "DRY RUN — no messages produced" in result.output
        and "run with --execute to replay" in result.output
    )
    assert cli.producer.sent == ()


def test_execute_prompts_and_declining_aborts(cli):
    result = cli.run("--to-topic", "orders", "--rate", "0", "--execute", input="n\n")
    assert result.exit_code != 0 and cli.producer.sent == ()


def test_execute_yes_replays_and_reports(cli):
    result = cli.run("--to-topic", "orders", "--rate", "0", "--execute", "--yes")
    assert result.exit_code == 0, result.output
    assert len(cli.producer.sent) == 15 and "produced 15 messages" in result.output


def test_the_where_filter_and_max_apply(cli):
    result = cli.run(
        "--to-topic", "orders", "--rate", "0", "--where", "nope == 'x'", "--execute", "--yes"
    )
    assert "produced 0" in result.output
    assert cli.producer.sent == ()


def test_to_topic_is_required_and_same_topic_is_refused(cli):
    assert cli.run().exit_code != 0
    result = cli.run("--to-topic", "orders.dlq")
    assert result.exit_code != 0 and "itself" in result.output


def test_offset_flags_need_exactly_one_partition(cli):
    result = cli.run("--to-topic", "orders", "--from-offset", "5")
    assert result.exit_code != 0 and "one --partitions" in result.output
    ok = cli.run("--to-topic", "orders", "--rate", "0", "--partitions", "0", "--from-offset", "105")
    assert ok.exit_code == 0 and "offsets 105 → 110" in ok.output
