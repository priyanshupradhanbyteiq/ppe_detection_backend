"""Lifecycle tests with real SQL transactions and a fake streaming pipeline.

A synchronous SQLite adapter exercises the async repository without an external
DB or extra test dependencies. PostgreSQL DDL is checked separately with Alembic.
"""
import asyncio
import os
import threading
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from pydantic import ValidationError
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.deepstream_worker.camera_manager import CameraManager
from app.deepstream_worker.messaging.camera_consumer import CameraCommandConsumer
from app.models import Base, Camera, CameraRuntimeLog, CameraSession, CameraStatusLog, DeepStreamWorker
from app.repositories.camera_repository import CameraRepository
from app.repositories.camera_runtime_repository import CameraRuntimeRepository
from app.schemas.camera import CameraCreateRequest, CameraUpdateRequest
from app.services.camera_service import CameraService


@compiles(JSONB, "sqlite")
def sqlite_jsonb(type_, compiler, **kwargs):
    return "JSON"


class AsyncTransaction:
    def __init__(self, transaction):
        self.transaction = transaction
    async def __aenter__(self):
        self.transaction.__enter__()
        return self
    async def __aexit__(self, *args):
        return self.transaction.__exit__(*args)


class AsyncSQLiteSession:
    def __init__(self, engine):
        self.session = Session(engine, expire_on_commit=False)
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        self.session.close()
    def begin(self):
        return AsyncTransaction(self.session.begin())
    def add(self, value):
        self.session.add(value)
    async def execute(self, statement):
        return self.session.execute(statement)
    async def get(self, model, key):
        return self.session.get(model, key)
    async def flush(self):
        self.session.flush()
    async def commit(self):
        self.session.commit()
    async def refresh(self, value):
        self.session.refresh(value)
    async def delete(self, value):
        self.session.delete(value)


class FakePipeline:
    MAX_CAMERAS = 5
    pipeline_id = "default-pipeline"
    def __init__(self):
        self.sources = {}
        self.removed = []
        self.on_error = None
    def add_camera(self, camera_id, rtsp_url, session_id, on_frame):
        if len(self.sources) >= self.MAX_CAMERAS:
            raise RuntimeError("Pipeline capacity reached")
        index = min(set(range(self.MAX_CAMERAS)) - {s["index"] for s in self.sources.values()})
        self.sources[camera_id] = dict(index=index, session_id=session_id, on_frame=on_frame)
        return index
    def remove_camera(self, camera_id):
        self.removed.append(camera_id)
        return self.sources.pop(camera_id, None) is not None
    def shutdown(self):
        self.sources.clear()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://")
        @event.listens_for(self.engine, "connect")
        def foreign_keys(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")
        Base.metadata.create_all(self.engine)
        self.factory = lambda: AsyncSQLiteSession(self.engine)
        self.repository = CameraRuntimeRepository(self.factory)
        self.worker_id = await self.repository.initialize_worker("test-worker", "localhost", 5)
        self.pipeline = FakePipeline()
        self.manager = CameraManager(self.repository, self.pipeline, self.worker_id, retry_delay=0)
        self.db = self.factory()
        self.service = CameraService(CameraRepository(self.db))
        self.service.rabbitmq = SimpleNamespace(publish=AsyncMock())
        self.consumer = CameraCommandConsumer("unused", self.manager)

    async def asyncTearDown(self):
        self.db.session.close()
        self.engine.dispose()

    async def create_camera(self, desired_state="STOPPED"):
        return await self.service.create_camera(CameraCreateRequest(
            name="Test camera", rtsp_url="rtsp://localhost/test", desired_state=desired_state,
        ), verify_stream_first=False)

    def read(self, model):
        with Session(self.engine) as db:
            return db.scalars(select(model)).all()

    async def test_create_start_frame_stop_roundtrip(self):
        camera = await self.create_camera()
        await self.consumer.dispatch("camera.created", str(camera.id))
        self.assertEqual(camera.desired_state, "STOPPED")
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.read(CameraSession), [])
        await self.service.start_camera(camera.id)
        await self.consumer.dispatch("camera.start", str(camera.id))
        runtime = self.manager.get_runtime(camera.id)
        token = runtime.session_id
        self.assertTrue(runtime.running)
        self.assertEqual(self.read(Camera)[0].status, "CONNECTING")
        self.assertEqual(self.read(CameraRuntimeLog)[0].metadata_json, {"source_index": 0})
        await self.manager.mark_frame_received(camera.id, token)
        self.assertEqual(self.read(Camera)[0].status, "ONLINE")
        self.assertEqual(self.read(CameraSession)[0].frames_processed, 1)
        self.assertIsNotNone(self.read(Camera)[0].last_frame_at)
        await self.service.stop_camera(camera.id)
        await self.consumer.dispatch("camera.stop", str(camera.id))
        self.assertFalse(runtime.running)
        self.assertIsNone(runtime.session_id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.read(Camera)[0].status, "OFFLINE")
        self.assertEqual(self.read(CameraSession)[0].status, "STOPPED")
        self.assertIsNotNone(self.read(CameraSession)[0].stopped_at)
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 0)
        self.assertEqual([row.status for row in self.read(CameraStatusLog)],
                         ["OFFLINE", "CONNECTING", "ONLINE", "OFFLINE"])
        self.assertIsNotNone(self.read(CameraStatusLog)[0].ended_at)
        await self.manager.mark_frame_received(camera.id, token)
        self.assertEqual(runtime.status, "OFFLINE")

    async def test_concurrent_start_is_idempotent(self):
        camera = await self.create_camera("RUNNING")
        await asyncio.gather(*(self.manager.start_camera(camera.id) for _ in range(5)))
        self.assertEqual(len(self.read(CameraSession)), 1)
        self.assertEqual(len(self.pipeline.sources), 1)

    async def test_stale_commands_follow_latest_db_state(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        await self.service.stop_camera(camera.id)
        await self.consumer.dispatch("camera.start", str(camera.id))
        self.assertFalse(self.manager.get_runtime(camera.id).running)
        await self.service.start_camera(camera.id)
        await self.consumer.dispatch("camera.stop", str(camera.id))
        self.assertTrue(self.manager.get_runtime(camera.id).running)

    async def test_broker_failure_recovers_and_repeated_start_publishes(self):
        camera = await self.create_camera()
        self.service.rabbitmq.publish.side_effect = RuntimeError("broker unavailable")
        with self.assertLogs("app.services.camera_service", level="ERROR"):
            await self.service.start_camera(camera.id)
        self.assertEqual(self.read(Camera)[0].desired_state, "RUNNING")
        await self.manager.reconcile_all()
        self.assertTrue(self.manager.get_runtime(camera.id).running)
        self.service.rabbitmq.publish.side_effect = None
        await self.service.start_camera(camera.id)
        self.assertEqual(self.service.rabbitmq.publish.call_args.kwargs["routing_key"], "camera.start")

    async def test_failed_session_write_removes_source_and_rolls_back(self):
        camera = await self.create_camera("RUNNING")
        original = self.repository._transition
        async def transition(db, camera, worker, session, status, event, message=None):
            if event == "CAMERA_SOURCE_ASSIGNED":
                raise RuntimeError("injected transaction failure")
            return await original(db, camera, worker, session, status, event, message)
        with patch.object(self.repository, "_transition", transition):
            with self.assertRaisesRegex(RuntimeError, "transaction failure"):
                await self.manager.start_camera(camera.id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.read(CameraSession), [])
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 0)
        self.assertEqual(self.read(Camera)[0].status, "ERROR")
        await self.manager.reconcile_all()
        self.assertTrue(self.manager.get_runtime(camera.id).running)

    async def test_failed_stop_write_retries_same_session(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        token = self.manager.get_runtime(camera.id).session_id
        await self.service.stop_camera(camera.id)
        with patch.object(self.repository, "finish_session", AsyncMock(side_effect=RuntimeError("db unavailable"))):
            with self.assertRaises(RuntimeError):
                await self.manager.stop_camera(camera.id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.manager.get_runtime(camera.id).session_id, token)
        await self.manager.reconcile_all()
        self.assertIsNone(self.manager.get_runtime(camera.id).session_id)
        self.assertEqual(self.read(CameraSession)[0].status, "STOPPED")

    async def test_restart_closes_stale_session_and_restores_running_camera(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        old_token = self.manager.get_runtime(camera.id).session_id
        self.pipeline.shutdown()  # Simulate a dead process, leaving its DB session open.
        worker = await self.repository.initialize_worker("test-worker", "localhost", 5)
        restarted = CameraManager(self.repository, FakePipeline(), worker, retry_delay=0)
        await restarted.reconcile_all()
        self.assertEqual(self.read(CameraSession)[0].status, "ERROR")
        self.assertIsNotNone(self.read(CameraSession)[0].stopped_at)
        runtime = restarted.get_runtime(camera.id)
        self.assertNotEqual(runtime.session_id, old_token)
        await restarted.mark_frame_received(camera.id, old_token)
        self.assertEqual(runtime.frames_processed, 0)
        self.assertEqual(runtime.status, "CONNECTING")

    async def test_capacity_recovers_after_a_slot_is_freed(self):
        cameras = [await self.create_camera("RUNNING") for _ in range(6)]
        with self.assertLogs("app.deepstream_worker.camera_manager", level="ERROR"):
            await self.manager.reconcile_all()
        self.assertEqual(len(self.pipeline.sources), 5)
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 5)
        waiting = next(c for c in cameras if not self.manager.get_runtime(c.id).running)
        active = next(c for c in cameras if self.manager.get_runtime(c.id).running)
        await self.service.stop_camera(active.id)
        await self.manager.reconcile_all()
        self.assertTrue(self.manager.get_runtime(waiting.id).running)
        self.assertEqual(len(self.pipeline.sources), 5)

    async def test_source_error_timeout_and_shutdown_persist(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        runtime = self.manager.get_runtime(camera.id)
        await self.manager.mark_error(camera.id, runtime.session_id, "RTSP disconnected")
        self.assertEqual(self.read(CameraSession)[0].error_message, "RTSP disconnected")
        self.assertEqual(self.read(Camera)[0].status, "ERROR")
        await self.manager.reconcile_all()
        runtime.started_at -= 60
        await self.manager.flush_frames()
        self.assertFalse(runtime.running)
        await self.manager.reconcile_all()
        await self.manager.shutdown()
        self.assertEqual(self.read(DeepStreamWorker)[0].status, "OFFLINE")
        self.assertEqual(self.read(Camera)[0].desired_state, "RUNNING")
        self.assertTrue(all(s.stopped_at is not None for s in self.read(CameraSession)))

    async def test_delete_and_rtsp_update_reconcile_sources(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        token = self.manager.get_runtime(camera.id).session_id
        await self.service.update_camera(camera.id, CameraUpdateRequest(rtsp_url="rtsp://localhost/new"))
        await self.manager.update_camera(camera.id)
        self.assertNotEqual(self.manager.get_runtime(camera.id).session_id, token)
        await self.service.delete_camera(camera.id)
        await self.manager.remove_camera(camera.id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertIsNone(self.manager.get_runtime(camera.id))
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 0)

    async def test_db_prevents_duplicate_active_sessions(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        with self.assertRaises(IntegrityError):
            await self.repository.create_session(camera.id, self.worker_id, uuid4(), "default-pipeline", 1)
        self.assertEqual(len(self.read(CameraSession)), 1)
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 1)

    def test_schema_rejects_invalid_states_and_runtime_status_edits(self):
        for payload in [{"desired_state": "ACTIVE"}, {"desired_state": None},
                        {"status": "ONLINE"}, {"name": None}, {"ppe_enabled": None}]:
            with self.assertRaises(ValidationError):
                CameraUpdateRequest(**payload)

    async def test_failed_commands_are_preserved(self):
        class MessageContext:
            async def __aenter__(context):
                return context
            async def __aexit__(context, exc_type, exc, tb):
                context.error = exc
                return False
        context = MessageContext()
        message = SimpleNamespace(body=b'{invalid-json', message_id='test-event',
                                  process=lambda **kwargs: context)
        publish = AsyncMock()
        self.consumer.channel = SimpleNamespace(default_exchange=SimpleNamespace(publish=publish))
        with self.assertLogs("app.deepstream_worker.messaging.camera_consumer", level="ERROR"):
            await self.consumer.handle_message(message)
        self.assertIsNone(context.error)
        self.assertEqual(publish.call_args.kwargs['routing_key'], 'deepstream.camera.commands.failed')
        self.assertEqual(publish.call_args.args[0].body, message.body)
        publish.side_effect = RuntimeError('cannot preserve failed command')
        with self.assertLogs("app.deepstream_worker.messaging.camera_consumer", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, 'cannot preserve'):
                await self.consumer.handle_message(message)
        self.assertIsInstance(context.error, RuntimeError)

    async def test_thread_frame_callback_updates_only_its_session(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        token = self.manager.get_runtime(camera.id).session_id
        # Use a streaming-style thread rather than asyncio's executor: no
        # thread-pool shutdown or network wake-up is needed in sandboxed tests.
        thread = threading.Thread(target=self.manager.notify_frame, args=(str(camera.id), token))
        thread.start()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        await asyncio.sleep(0)
        await self.manager.flush_frames()
        self.assertEqual(self.read(CameraSession)[0].frames_processed, 1)
        self.assertEqual(self.read(Camera)[0].status, "ONLINE")

    async def test_stop_persistence_failure_before_new_running_request(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.start_camera(camera.id)
        await self.service.stop_camera(camera.id)
        with patch.object(self.repository, "finish_session", AsyncMock(side_effect=RuntimeError("db unavailable"))):
            with self.assertRaises(RuntimeError):
                await self.manager.stop_camera(camera.id)
        await self.service.start_camera(camera.id)
        await self.manager.reconcile_all()
        sessions = self.read(CameraSession)
        self.assertEqual(len(sessions), 2)
        self.assertIsNotNone(sessions[0].stopped_at)
        self.assertIsNone(sessions[1].stopped_at)

    async def test_lost_commit_acknowledgement_retains_session_for_cleanup(self):
        camera = await self.create_camera("RUNNING")
        original = self.repository.create_session
        async def lost_acknowledgement(**kwargs):
            await original(**kwargs)
            raise RuntimeError("connection lost after commit")
        with patch.object(self.repository, "create_session", lost_acknowledgement), \
             patch.object(self.repository, "finish_session", AsyncMock(side_effect=RuntimeError("DB still unavailable"))):
            with self.assertRaisesRegex(RuntimeError, "DB still unavailable"):
                await self.manager.start_camera(camera.id)
        token = self.manager.get_runtime(camera.id).session_id
        self.assertIsNotNone(token)
        self.assertEqual(self.pipeline.sources, {})
        await self.manager.reconcile_all()
        sessions = self.read(CameraSession)
        self.assertEqual(len(sessions), 2)
        self.assertEqual(str(sessions[0].id), token)
        self.assertIsNotNone(sessions[0].stopped_at)
        self.assertIsNone(sessions[1].stopped_at)

    async def test_consumer_retries_broker_startup_failure(self):
        camera = await self.create_camera("RUNNING")
        await self.manager.reconcile_all()
        stop_event = asyncio.Event()
        attempts = 0
        async def start():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("broker unavailable")
            stop_event.set()
        with patch.object(self.consumer, "start", start):
            with self.assertLogs("app.deepstream_worker.messaging.camera_consumer", level="ERROR"):
                await self.consumer.run(stop_event, retry_seconds=0.001)
        self.assertEqual(attempts, 2)
        self.assertTrue(self.manager.get_runtime(camera.id).running)


if __name__ == "__main__":
    unittest.main()
