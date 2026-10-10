"""Committed worker events -> PostgreSQL LISTEN -> browser WebSocket queues."""

import asyncio
import json
import logging
from uuid import UUID

import asyncpg
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.repositories.ppe_event_repository import PPEEventRepository
from app.services.ppe_event_serialization import serialize_event

logger = logging.getLogger(__name__)
CHANNEL = "ppe_events"


async def notify_event(db, event_id, action):
    """NOTIFY is inside the event transaction: rollback never sends an alert."""
    # SQLite adapters support local persistence tests, without a PostgreSQL bus.
    session = getattr(db, "session", db)
    if session.get_bind().dialect.name != "postgresql":
        return
    payload = json.dumps({"event_id": str(event_id), "action": action})
    await db.execute(select(func.pg_notify(CHANNEL, payload)))


class PPENotificationHub:
    def __init__(self, session_factory=None, connect=None):
        self.session_factory = session_factory or AsyncSessionLocal
        self.connect = connect or asyncpg.connect
        self.clients = set()
        self.pending = asyncio.Queue(maxsize=1000)
        self.connected = asyncio.Event()
        self.listener_task = None
        self.dispatch_task = None

    async def start(self):
        if self.listener_task is None or self.listener_task.done():
            self.listener_task = asyncio.create_task(self._listen(), name="ppe-notification-listener")
            self.dispatch_task = asyncio.create_task(self._dispatch(), name="ppe-notification-dispatch")

    async def subscribe(self):
        await self.start()
        await asyncio.wait_for(self.connected.wait(), 6)
        queue = asyncio.Queue(maxsize=200)
        self.clients.add(queue)
        return queue

    def unsubscribe(self, queue):
        self.clients.discard(queue)

    def broadcast(self, message):
        for queue in list(self.clients):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait({"type": "resync"})

    def _received(self, connection, pid, channel, payload):
        try:
            message = json.loads(payload)
            UUID(message["event_id"])
            if message["action"] not in {"CREATED", "UPDATED", "RESOLVED"}:
                return
            self.pending.put_nowait(message)
        except asyncio.QueueFull:
            self.broadcast({"type": "resync"})
        except (ValueError, KeyError, TypeError):
            return

    async def _listen(self):
        # Render internally only; never log a credential-bearing database URL.
        dsn = make_url(settings.DATABASE_URL).set(drivername="postgresql").render_as_string(hide_password=False)
        failures = 0
        while True:
            connection = None
            try:
                connection = await self.connect(dsn, timeout=5, command_timeout=5)
                await connection.add_listener(CHANNEL, self._received)
                self.connected.set()
                self.broadcast({"type": "resync"})
                failures = 0
                while not connection.is_closed():
                    await asyncio.sleep(5)
                    await connection.execute("SELECT 1")
            except asyncio.CancelledError:
                raise
            except Exception:
                failures += 1
                if failures == 1 or failures % 10 == 0:
                    logger.warning("PPE notification listener unavailable; reconnecting")
            finally:
                self.connected.clear()
                if connection is not None and not connection.is_closed():
                    try:
                        await connection.close(timeout=2)
                    except Exception:
                        connection.terminate()
            self.broadcast({"type": "reconnecting"})
            await asyncio.sleep(min(10, failures + 1))

    async def _dispatch(self):
        while True:
            message = await self.pending.get()
            try:
                if not self.clients:
                    continue
                async with self.session_factory() as db:
                    event = await PPEEventRepository.get_by_id(db, UUID(message["event_id"]))
                    if event is None:
                        continue  # Camera deletion cascaded the event.
                    payload = serialize_event(event).model_dump(mode="json")
                self.broadcast({"type": "ppe_event", "action": message["action"], "event": payload})
            except Exception:
                logger.warning("PPE notification dispatch failed; clients will resync")
                self.broadcast({"type": "resync"})
            finally:
                self.pending.task_done()

    async def stop(self):
        tasks = [task for task in (self.listener_task, self.dispatch_task) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.listener_task = self.dispatch_task = None
        self.connected.clear()
        self.clients.clear()


ppe_notification_hub = PPENotificationHub()
