"""Opt-in real PostgreSQL commit/rollback semantics; no table writes or migrations.

Run with PPE_TEST_POSTGRES=1 and the application's DATABASE_URL. Each test uses
its own notification channel, so it cannot send popups to application users.
"""
import asyncio
import json
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import asyncpg
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.services import ppe_notifications


@unittest.skipUnless(os.environ.get("PPE_TEST_POSTGRES") == "1", "Opt-in PostgreSQL notification test")
class PostgreSQLNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.channel = "ppe_test_" + uuid4().hex
        self.patch = patch.object(ppe_notifications, "CHANNEL", self.channel)
        self.patch.start()
        self.messages = asyncio.Queue()
        dsn = make_url(settings.DATABASE_URL).set(drivername="postgresql").render_as_string(hide_password=False)
        self.listener = await asyncpg.connect(dsn, timeout=5)
        await self.listener.add_listener(self.channel, lambda connection, pid, channel, payload: self.messages.put_nowait(json.loads(payload)))
        self.engine = create_async_engine(settings.DATABASE_URL)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def asyncTearDown(self):
        await self.listener.close()
        await self.engine.dispose()
        self.patch.stop()

    async def test_event_notification_is_delivered_only_after_commit(self):
        event_id = uuid4()
        async with self.factory() as db, db.begin():
            await ppe_notifications.notify_event(db, event_id, "CREATED")
            await asyncio.sleep(.05)
            self.assertTrue(self.messages.empty())
        message = await asyncio.wait_for(self.messages.get(), 2)
        self.assertEqual(message, {"event_id": str(event_id), "action": "CREATED"})

    async def test_rolled_back_notification_is_not_delivered(self):
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            async with self.factory() as db, db.begin():
                await ppe_notifications.notify_event(db, uuid4(), "CREATED")
                raise RuntimeError("rollback")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.messages.get(), .15)


if __name__ == "__main__":
    unittest.main()
