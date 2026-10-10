"""Notification transitions, bounded fanout, and authenticated WebSocket delivery."""
import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.websockets import WebSocketDisconnect

from app.api.v1 import event_notifications
from app.deepstream_worker.ppe_decision_engine import PPEEventEngine
from app.models import Base, Camera, PPEEvent
from app.services.ppe_event_persistence import PPEEventPersistence
from app.services.ppe_notifications import PPENotificationHub
from test_camera_lifecycle import AsyncSQLiteSession


class PPENotificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)
        self.factory = lambda: AsyncSQLiteSession(self.engine)
        self.tmp = tempfile.TemporaryDirectory()
        self.persistence = PPEEventPersistence(self.factory, Path(self.tmp.name) / "outbox.sqlite3", Mock(enabled=False))
        self.camera_id = uuid4()
        with Session(self.engine) as db, db.begin():
            db.add(Camera(id=self.camera_id, name="Loading Bay", location_name="North", rtsp_url="rtsp://test", source_type="RTSP_STREAM"))

    async def asyncTearDown(self):
        await self.persistence.stop()
        self.engine.dispose()
        self.tmp.cleanup()

    def payload(self):
        now = datetime.now(timezone.utc)
        return dict(id=str(uuid4()), camera_id=str(self.camera_id), session_id=str(uuid4()),
                    person_track_id=7, ppe_type="helmet", violation_type="NO_HARDHAT", event_type="PPE_VIOLATION",
                    severity="HIGH", status="OPEN", confidence=.9, violation_ratio=1., missing_count=5,
                    present_count=0, unknown_count=0, first_observed_at=now, confirmed_at=now,
                    last_observed_at=now, resolved_at=None, resolution_reason=None)

    async def test_replayed_create_and_resolve_do_not_publish_twice(self):
        event = self.payload()
        with patch("app.services.ppe_event_persistence.notify_event", new_callable=AsyncMock) as notify:
            await self.persistence.persist_action({"action": "CREATED", "event": event})
            await self.persistence.persist_action({"action": "CREATED", "event": event})
            await self.persistence.persist_action({"action": "UPDATED", "event": event})
            resolved = dict(event, status="RESOLVED", resolved_at=event["confirmed_at"], resolution_reason="PERSON_LEFT_CAMERA")
            await self.persistence.persist_action({"action": "RESOLVED", "event": resolved})
            await self.persistence.persist_action({"action": "RESOLVED", "event": resolved})
            await self.persistence.persist_action({"action": "UPDATED", "event": event})
            self.assertEqual([call.args[2] for call in notify.await_args_list], ["CREATED", "UPDATED", "RESOLVED"])

    async def test_notification_failure_rolls_back_event_for_outbox_retry(self):
        event = self.payload()
        with patch("app.services.ppe_event_persistence.notify_event", new_callable=AsyncMock) as notify:
            notify.side_effect = RuntimeError("notification transaction failure")
            with self.assertRaises(RuntimeError):
                await self.persistence.persist_action({"action": "CREATED", "event": event})
            with Session(self.engine) as db:
                self.assertEqual(db.query(PPEEvent).count(), 0)
            notify.side_effect = None
            await self.persistence.persist_action({"action": "CREATED", "event": event})
            with Session(self.engine) as db:
                self.assertEqual(db.query(PPEEvent).count(), 1)

    async def test_engine_marks_only_lifecycle_transitions_for_notification(self):
        engine = PPEEventEngine(min_observations=1, min_evidence_seconds=0, recovery_threshold=1, recovery_window_seconds=.1)
        observation = {"camera_id": str(self.camera_id), "track_id": 7, "timestamp": 1,
                       "ppe": {"helmet": {"state": "MISSING", "confidence": .9}}}
        action = engine.process(observation)[0]
        self.assertEqual(action["notification_type"], "ppe.created")
        updated = engine.process(dict(observation, timestamp=1.1))[0]
        self.assertNotIn("notification_type", updated)
        resolved = engine.close_session(2)[0]
        self.assertEqual(resolved["notification_type"], "ppe.resolved")
        self.assertEqual(resolved["event"]["id"], action["event"]["id"])

    async def test_hub_serializes_saved_event_with_camera_details(self):
        event = self.payload()
        await self.persistence.persist_action({"action": "CREATED", "event": event})
        hub = PPENotificationHub(session_factory=self.factory)
        queue = asyncio.Queue(maxsize=200)
        hub.clients.add(queue)
        task = asyncio.create_task(hub._dispatch())
        try:
            hub._received(None, 0, "ppe_events", '{"event_id":"not-a-uuid"}')
            self.assertTrue(hub.pending.empty())
            hub._received(None, 0, "ppe_events", '{"event_id":"%s","action":"CREATED"}' % event["id"])
            delivered = await asyncio.wait_for(queue.get(), 2)
            self.assertEqual(delivered["type"], "ppe_event")
            self.assertEqual(delivered["event"]["id"], event["id"])
            self.assertEqual(delivered["event"]["camera_name"], "Loading Bay")
            self.assertEqual(delivered["event"]["location_name"], "North")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_slow_client_gets_resync_instead_of_an_unbounded_queue(self):
        hub = PPENotificationHub()
        queue = asyncio.Queue(maxsize=2)
        hub.clients.add(queue)
        hub.broadcast({"type": "first"})
        hub.broadcast({"type": "second"})
        hub.broadcast({"type": "third"})
        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(await queue.get(), {"type": "resync"})
        hub.unsubscribe(queue)
        self.assertEqual(hub.clients, set())


class FakeHub:
    async def subscribe(self):
        self.loop = asyncio.get_running_loop()
        self.queue = asyncio.Queue()
        self.subscribed = True
        return self.queue
    def unsubscribe(self, queue):
        self.subscribed = False
    def emit(self, event):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, event)


class NotificationWebSocketTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(event_notifications.router, prefix="/api/v1/events")
        self.hub = FakeHub()
        self.hub.subscribed = False
        self.hub_patch = patch.object(event_notifications, "get_notification_hub", return_value=self.hub)
        self.hub_patch.start()
        self.auth_patch = patch.object(event_notifications, "authenticate_event_socket", new_callable=AsyncMock)
        self.auth = self.auth_patch.start()
        self.auth.return_value = {"exp": time.time() + 120}
        self.client = TestClient(self.app)

    def tearDown(self):
        self.client.close()
        self.auth_patch.stop()
        self.hub_patch.stop()

    def test_authenticated_socket_receives_event_and_disconnect_unsubscribes(self):
        with self.client.websocket_connect("/api/v1/events/ws", headers={"origin": "http://localhost:5173"}) as socket:
            socket.send_json({"type": "auth", "token": "test-access-token"})
            self.assertEqual(socket.receive_json(), {"type": "ready"})
            self.auth.assert_awaited_once_with("test-access-token")
            payload = {"type": "ppe_event", "action": "CREATED", "event": {"id": str(uuid4())}}
            self.hub.emit(payload)
            self.assertEqual(socket.receive_json(), payload)
            socket.send_json({"type": "ping"})
            self.assertEqual(socket.receive_json(), {"type": "pong"})
        self.assertFalse(self.hub.subscribed)

    def test_missing_authentication_is_rejected_before_subscribing(self):
        with self.client.websocket_connect("/api/v1/events/ws") as socket:
            socket.send_json({"type": "ping"})
            with self.assertRaises(WebSocketDisconnect) as rejected:
                socket.receive_json()
            self.assertEqual(rejected.exception.code, 4401)
        self.assertFalse(self.hub.subscribed)

    def test_invalid_token_is_rejected_before_subscribing(self):
        self.auth.side_effect = ValueError("invalid")
        with self.client.websocket_connect("/api/v1/events/ws") as socket:
            socket.send_json({"type": "auth", "token": "invalid"})
            with self.assertRaises(WebSocketDisconnect) as rejected:
                socket.receive_json()
            self.assertEqual(rejected.exception.code, 4401)
        self.assertFalse(self.hub.subscribed)

    def test_untrusted_origin_is_rejected(self):
        with self.assertRaises(WebSocketDisconnect) as rejected:
            self.client.websocket_connect("/api/v1/events/ws", headers={"origin": "https://untrusted.example"}).__enter__()
        self.assertEqual(rejected.exception.code, 4403)
        self.auth.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
