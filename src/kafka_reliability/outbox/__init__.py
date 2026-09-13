"""Transactional outbox: enqueue writers, the relay that publishes rows to
Kafka, DDL/schema helpers, and retention sweeps. Never imports dedup or replay."""
