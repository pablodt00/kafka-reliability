"""Redis-backed DedupStore. Fast, TTL-native; cannot join the caller's
transaction, so supports_transactions is always False. Requires [dedup-redis]."""
