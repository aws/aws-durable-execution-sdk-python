"""Shared internal constants."""

# Maximum checkpoint payload size in bytes (256KB).
# Payloads exceeding this limit trigger ReplayChildren mode in child contexts,
# and overflow-to-file behavior in FileSystemSerDes.
CHECKPOINT_SIZE_LIMIT_BYTES = 256 * 1024

# Maximum serialized size of a distributed map reader's state in bytes (32KB).
# The service rejects a larger state, so the initial state and every next state
# are checked against this.
READER_STATE_LIMIT_BYTES = 32 * 1024
