"""Shared internal constants."""

# Maximum checkpoint payload size in bytes (256KB).
# Payloads exceeding this limit trigger ReplayChildren mode in child contexts,
# and overflow-to-file behavior in FileSystemSerDes.
CHECKPOINT_SIZE_LIMIT_BYTES = 256 * 1024
