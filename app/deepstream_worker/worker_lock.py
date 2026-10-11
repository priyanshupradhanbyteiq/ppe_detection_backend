"""Keep one command consumer active for the deployment's shared camera queue."""

from contextlib import asynccontextmanager
from hashlib import sha256

from sqlalchemy import text


# PostgreSQL advisory locks accept a signed 64-bit key. Use a stable key across
# processes; Python's built-in hash() is randomized at process startup.
LOCK_KEY = int.from_bytes(
    sha256(b"deepstream.camera.commands").digest()[:8], "big", signed=True
)


@asynccontextmanager
async def worker_lock(engine):
    """Hold a session-level lock on a dedicated connection for the worker's life."""
    async with engine.connect() as connection:
        acquired = False
        try:
            acquired = (await connection.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": LOCK_KEY}
            )).scalar_one()
            await connection.commit()
            if not acquired:
                raise RuntimeError("Another DeepStream worker is already running for this database")
            yield
        finally:
            if acquired:
                try:
                    await connection.rollback()
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": LOCK_KEY}
                    )
                    await connection.commit()
                except BaseException:
                    # Never return a connection that might still hold the lock
                    # to the pool; closing it lets PostgreSQL release the lock.
                    await connection.invalidate()
                    raise
