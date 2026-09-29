"""Redis online-store writer used from Spark executors.

Writes are guarded by a Lua script: a hash is only updated when the
incoming record is at least as new as what is stored, so late or replayed
micro-batches can never overwrite fresher feature values.
"""

from __future__ import annotations

from collections.abc import Iterable

import redis

from eddy.config import user_key

# KEYS[1] = hash key; ARGV[1] = guard field, ARGV[2] = incoming guard value,
# ARGV[3] = ttl seconds, ARGV[4..] = field/value pairs.
GUARDED_HSET = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current and tonumber(current) > tonumber(ARGV[2]) then
  return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 4))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
"""


def to_mapping(row: dict, suffix: str | None, exclude: tuple[str, ...] = ("user_id",)) -> dict[str, str]:
    """Flatten a feature row to Redis hash fields, suffixing names by window."""
    mapping = {}
    for name, value in row.items():
        if name in exclude or value is None:
            continue
        field = f"{name}_{suffix}" if suffix else name
        mapping[field] = repr(round(value, 6)) if isinstance(value, float) else str(value)
    return mapping


def _flatten(mapping: dict[str, str]) -> list[str]:
    out: list[str] = []
    for k, v in mapping.items():
        out.extend((k, v))
    return out


class OnlineStoreWriter:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: str = "",
        prefix: str = "eddy",
        ttl_seconds: int = 86400,
        flush_every: int = 500,
        client: redis.Redis | None = None,
    ):
        self.client = client or redis.Redis(host=host, port=port, password=password or None)
        self.prefix = prefix
        self.ttl = ttl_seconds
        self.flush_every = flush_every
        self.script = self.client.register_script(GUARDED_HSET)

    def write(self, rows: Iterable[dict], suffix: str | None, guard_field: str) -> tuple[int, int]:
        """Write rows; returns (applied, skipped_as_stale)."""
        applied = skipped = 0
        pipe = self.client.pipeline(transaction=False)
        pending = 0
        guard_name = f"{guard_field}_{suffix}" if suffix else guard_field

        def flush():
            nonlocal applied, skipped, pending
            if pending:
                results = pipe.execute()
                applied += sum(1 for r in results if r == 1)
                skipped += sum(1 for r in results if r == 0)
                pending = 0

        for row in rows:
            mapping = to_mapping(row, suffix)
            if guard_name not in mapping:
                continue
            self.script(
                keys=[user_key(self.prefix, row["user_id"])],
                args=[guard_name, mapping[guard_name], self.ttl, *_flatten(mapping)],
                client=pipe,
            )
            pending += 1
            if pending >= self.flush_every:
                flush()
        flush()
        return applied, skipped
