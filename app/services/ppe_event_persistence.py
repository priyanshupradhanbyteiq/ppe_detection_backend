"""Thread-safe durable admission, ordered PostgreSQL writes, separate snapshots.

SQLite is an outbox, not event history. A job is removed only after its database
transaction commits. It also retains staged snapshot jobs across worker restarts.
"""

import asyncio
import json
import logging
import os
import queue
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError

from app.models import Camera, PPEEvent
from app.repositories.ppe_event_repository import PPEEventRepository
from app.services.ppe_snapshot_service import PPESnapshotService

logger = logging.getLogger(__name__)
TIME_FIELDS = ("first_observed_at", "confirmed_at", "last_observed_at", "resolved_at")


class PPEEventPersistence:
    def __init__(self, session_factory=None, outbox_path="data/ppe_outbox.sqlite3",
                 snapshot_service=None, update_interval=5.0, shutdown_timeout=30.0):
        if session_factory is None:
            from app.core.database import AsyncSessionLocal
            session_factory = AsyncSessionLocal
        self.session_factory = session_factory
        self.repository = PPEEventRepository()
        self.snapshots = snapshot_service if snapshot_service is not None else PPESnapshotService()
        self.update_interval = update_interval
        self.shutdown_timeout = shutdown_timeout
        self.last_update = {}
        self.path = Path(outbox_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir = self.path.parent / "ppe_snapshots"
        self.lock = threading.Lock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS actions (seq INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS snapshots (event_id TEXT PRIMARY KEY, camera_id TEXT NOT NULL, path TEXT NOT NULL, captured_at TEXT NOT NULL)")
        self.db.commit()
        self.snapshot_queue = queue.Queue(maxsize=32)
        self.tasks = []
        self.closing = False
        self.closed = False

    def submit(self, action):
        """Called from the decision thread. Lifecycle transitions are never dropped."""
        event_id = str(action["event"]["id"])
        with self.lock:
            if self.closed or self.closing:
                raise RuntimeError("PPE persistence is stopping")
            now = time.monotonic()
            if action["action"] == "UPDATED" and now - self.last_update.get(event_id, float("-inf")) < self.update_interval:
                return
            payload = json.dumps(action, default=lambda value: value.isoformat(), allow_nan=False)
            self.db.execute("INSERT INTO actions(payload) VALUES (?)", (payload,))
            self.db.commit()
            if action["action"] == "RESOLVED":
                self.last_update.pop(event_id, None)
            else:
                self.last_update[event_id] = now

    def submit_snapshot(self, request, frame, captured_at):
        if not self.snapshots.enabled or self.closing:
            return False
        try:
            self.snapshot_queue.put_nowait((request, frame, captured_at))
            return True
        except queue.Full:
            # The processor retains the request and retries on the next frame.
            return False

    def _next_action(self):
        with self.lock:
            return self.db.execute("SELECT seq, payload FROM actions ORDER BY seq LIMIT 1").fetchone()

    def _ack_action(self, seq):
        with self.lock:
            self.db.execute("DELETE FROM actions WHERE seq = ?", (seq,))
            self.db.commit()

    async def persist_action(self, action):
        operation = {"CREATED": self.repository.create, "UPDATED": self.repository.update,
                     "RESOLVED": self.repository.resolve}.get(action["action"])
        if operation is None:
            raise ValueError(f"Unknown PPE action: {action['action']}")
        async with self.session_factory() as db, db.begin():
            await operation(db, action["event"])

    async def persist_actions(self, actions):
        for action in actions:
            await self.persist_action(action)

    async def start(self, worker_id):
        self.closing = False
        try:
            # Fail startup before restoring cameras when the schema is outdated.
            try:
                async with self.session_factory() as db:
                    await db.execute(select(PPEEvent.id, PPEEvent.snapshot_captured_at).limit(0))
            except ProgrammingError as error:
                if getattr(error.orig, "sqlstate", None) not in {"42P01", "42703"}:
                    raise
                raise RuntimeError(
                    "PPE database schema is outdated. From camera_recognization_backend, "
                    "run 'python3 -m alembic upgrade n7p8q9' before starting the worker."
                ) from error
            self.tasks = [asyncio.create_task(self._event_loop(), name="ppe-postgres-writer")]
            # Replay before recovery: an old queued CREATE must not reopen a
            # violation belonging to the worker's abandoned camera sessions.
            await asyncio.wait_for(self._wait_for_actions(), self.shutdown_timeout)
            async with self.session_factory() as db, db.begin():
                await self.repository.recover_worker(db, worker_id)
            if self.snapshots.enabled:
                self.tasks.append(asyncio.create_task(self._snapshot_loop(), name="ppe-snapshot-writer"))
            else:
                logger.info("Cloudinary credentials absent; PPE snapshots disabled")
        except BaseException:
            await self.stop()
            raise

    async def _wait_for_actions(self):
        while self._next_action() is not None:
            await asyncio.sleep(0.05)

    async def _event_loop(self):
        failures = 0
        while True:
            item = self._next_action()
            if item is None:
                if self.closing:
                    return
                await asyncio.sleep(0.05)
                continue
            seq, payload = item
            try:
                action = json.loads(payload)
                for field in TIME_FIELDS:
                    if action["event"].get(field) is not None:
                        action["event"][field] = datetime.fromisoformat(action["event"][field])
                await self.persist_action(action)
                self._ack_action(seq)
                failures = 0
            except Exception:
                failures += 1
                if failures == 1 or failures % 5 == 0:
                    logger.exception("PPE write failed; outbox job %s retained (attempt %s)", seq, failures)
                # Do not allow later updates/resolutions to overtake this job.
                await asyncio.sleep(min(5.0, 0.1 * 2 ** min(failures, 6)))

    def _write_snapshot(self, request, frame):
        # A cancelled to_thread call can keep running during shutdown. Keep
        # SQLite access on the consumer so that thread cannot use a closed DB.
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        event_id = str(UUID(str(request["event_id"])))
        path = self.snapshot_dir / f"{event_id}.jpg"
        temporary = path.with_suffix(".tmp")
        image = self.snapshots.encode(frame)
        with temporary.open("wb") as file:
            file.write(image)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(path)
        return event_id, path

    async def _stage_snapshot(self, request, frame, captured_at):
        event_id, path = await asyncio.to_thread(self._write_snapshot, request, frame)
        with self.lock:
            self.db.execute("INSERT OR IGNORE INTO snapshots VALUES (?, ?, ?, ?)",
                            (event_id, request["camera_id"], str(path.resolve()), captured_at.isoformat()))
            self.db.commit()

    async def _snapshot_loop(self):
        failures = 0
        while True:
            # Stage CPU images before potentially slow network uploads.
            while True:
                try:
                    request, frame, captured_at = self.snapshot_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    stage_failures = 0
                    while True:
                        try:
                            await self._stage_snapshot(request, frame, captured_at)
                            break
                        except Exception:
                            stage_failures += 1
                            if stage_failures == 1 or stage_failures % 5 == 0:
                                logger.exception("PPE snapshot staging failed; CPU image retained")
                            await asyncio.sleep(min(5.0, 0.1 * 2 ** min(stage_failures, 6)))
                finally:
                    self.snapshot_queue.task_done()
            with self.lock:
                row = self.db.execute("SELECT event_id, camera_id, path, captured_at FROM snapshots ORDER BY rowid LIMIT 1").fetchone()
            if row is None:
                if self.closing:
                    return
                await asyncio.sleep(0.05)
                continue
            event_id, camera_id, path, captured_at = row
            try:
                async with self.session_factory() as db:
                    record = await db.get(PPEEvent, UUID(event_id))
                    camera = await db.get(Camera, UUID(camera_id))
                if record is None and camera is not None:
                    # CREATE has not committed yet. Snapshot uploads wait for it.
                    await asyncio.sleep(0.1)
                    continue
                if record is not None:
                    snapshot = await self.snapshots.upload_file(path, camera_id, event_id)
                    async with self.session_factory() as db, db.begin():
                        await self.repository.save_snapshot(db, event_id, **snapshot,
                                                            captured_at=datetime.fromisoformat(captured_at))
                with self.lock:
                    self.db.execute("DELETE FROM snapshots WHERE event_id = ?", (event_id,))
                    self.db.commit()
                Path(path).unlink(missing_ok=True)
                failures = 0
            except Exception:
                failures += 1
                if failures == 1 or failures % 5 == 0:
                    logger.exception("PPE snapshot %s failed; staged image retained", event_id)
                await asyncio.sleep(min(5.0, 0.1 * 2 ** min(failures, 6)))

    async def stop(self):
        if self.closed:
            return
        self.closing = True
        try:
            if self.tasks:
                await asyncio.wait_for(asyncio.gather(*self.tasks), self.shutdown_timeout)
        except asyncio.TimeoutError as error:
            raise RuntimeError("PPE persistence drain timed out; pending jobs remain in the outbox") from error
        finally:
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            with self.lock:
                self.db.close()
                self.closed = True
