"""OutboxRelay: poll the outbox table, produce to Kafka, mark rows sent.
Depends on core and the producers.port protocol, plus a DB driver."""
