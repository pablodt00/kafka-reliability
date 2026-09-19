"""core.errors — one base class, and only the distinctions callers catch."""

from __future__ import annotations

import pytest

from kafka_reliability.core.errors import (
    ConfigurationError,
    KafkaReliabilityError,
    MissingExtraError,
    ProducerError,
    RelayError,
    StoreUnavailableError,
    require_extra,
)


@pytest.mark.parametrize(
    "error",
    [ConfigurationError, StoreUnavailableError, RelayError, MissingExtraError, ProducerError],
)
def test_every_error_derives_from_the_base(error):
    assert issubclass(error, KafkaReliabilityError)


def test_missing_extra_is_configuration_error_and_import_error():
    assert issubclass(MissingExtraError, ConfigurationError)
    assert issubclass(MissingExtraError, ImportError)


def test_require_extra_names_the_extra_and_chains_the_cause():
    cause = ImportError("no module named foo")
    with pytest.raises(MissingExtraError, match="dedup-redis") as excinfo:
        require_extra(package="redis", extra="dedup-redis", cause=cause)
    assert excinfo.value.__cause__ is cause
