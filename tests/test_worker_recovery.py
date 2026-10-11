"""Recover abandoned source slots without disturbing another worker's sessions."""

import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from app.deepstream_worker.camera_manager import CameraManager
from app.deepstream_worker.worker_lock import worker_lock
from app.models import Base, Camera, CameraRuntimeLog, CameraSession, DeepStreamWorker
from app.repositories.camera_runtime_repository import CameraRuntimeRepository
from test_camera_lifecycle import AsyncSQLiteSession, FakePipeline


class WorkerRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://")

        @event.listens_for(self.engine, "connect")
        def foreign_keys(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(self.engine)
        self.repository = CameraRuntimeRepository(lambda: AsyncSQLiteSession(self.engine))
        self.worker_id = await self.repository.initialize_worker("recovery", "localhost", 5)
        self.camera_id = uuid4()
        with Session(self.engine) as db, db.begin():
            db.add(Camera(id=self.camera_id, name="Recovery camera", rtsp_url="rtsp://localhost/test",
                          desired_state="RUNNING", source_type="RTSP_STREAM"))
        self.pipeline = FakePipeline()
        self.manager = CameraManager(self.repository, self.pipeline, self.worker_id)

    async def asyncTearDown(self):
        await self.manager.shutdown()
        self.engine.dispose()

    def read(self, model):
        with Session(self.engine) as db:
            return db.scalars(select(model)).all()

    async def abandon_session(self):
        runtime = await self.manager.start_camera(self.camera_id)
        # Simulate process death: the streaming sources are gone, but no session
        # finalization ran. The next process starts with empty in-memory state.
        self.pipeline.shutdown()
        self.manager.cameras.clear()
        return runtime["session_id"]

    async def test_restart_releases_source_zero_and_preserves_session_history(self):
        old_session_id = await self.abandon_session()
        worker_id = await self.repository.initialize_worker("recovery", "new-host", 5)
        self.assertEqual(worker_id, self.worker_id)
        old_session = self.read(CameraSession)[0]
        self.assertEqual(old_session.status, "ERROR")
        self.assertIsNotNone(old_session.stopped_at)
        self.assertIn("Worker restarted", old_session.error_message)
        self.assertEqual(self.read(Camera)[0].desired_state, "RUNNING")
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 0)
        await self.manager.restore_cameras()
        runtime = self.manager.cameras[str(self.camera_id)]
        self.assertEqual(runtime["source_index"], 0)
        self.assertNotEqual(runtime["session_id"], old_session_id)
        self.assertEqual(len(self.read(CameraSession)), 2)
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 1)
        self.assertIn("WORKER_RECOVERED", [row.event_type for row in self.read(CameraRuntimeLog)])
        # A queued start for the restored camera must not add another session.
        await self.manager.start_camera(self.camera_id)
        self.assertEqual(len(self.read(CameraSession)), 2)

    async def test_recovery_leaves_other_workers_sessions_and_counts_unchanged(self):
        await self.abandon_session()
        other_worker = await self.repository.initialize_worker("other", "other-host", 5)
        other_camera = uuid4()
        with Session(self.engine) as db, db.begin():
            db.add(Camera(id=other_camera, name="Other", rtsp_url="rtsp://localhost/other",
                          desired_state="RUNNING", source_type="RTSP_STREAM"))
        other_session = await self.repository.create_session(other_camera, other_worker, "other-pipeline", 0)
        await self.repository.initialize_worker("recovery", "new-host", 5)
        with Session(self.engine) as db:
            self.assertIsNone(db.get(CameraSession, other_session.id).stopped_at)
            self.assertEqual(db.get(DeepStreamWorker, other_worker).current_cameras, 1)
            self.assertEqual(db.get(Camera, other_camera).status, "CONNECTING")

    async def test_recovery_is_idempotent_and_transactional(self):
        await self.abandon_session()
        with patch.object(self.repository, "_transition", AsyncMock(side_effect=RuntimeError("DB failed"))):
            with self.assertRaisesRegex(RuntimeError, "DB failed"):
                await self.repository.initialize_worker("recovery", "new-host", 5)
        self.assertIsNone(self.read(CameraSession)[0].stopped_at)
        self.assertEqual(self.read(DeepStreamWorker)[0].current_cameras, 1)
        await self.repository.initialize_worker("recovery", "new-host", 5)
        logs = len(self.read(CameraRuntimeLog))
        await self.repository.initialize_worker("recovery", "new-host", 5)
        self.assertEqual(len(self.read(CameraRuntimeLog)), logs)

    async def test_stopped_camera_is_not_restored_or_started_by_old_command(self):
        with Session(self.engine) as db, db.begin():
            db.get(Camera, self.camera_id).desired_state = "STOPPED"
        await self.manager.restore_cameras()
        await self.manager.start_camera(self.camera_id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.read(CameraSession), [])


class WorkerCleanupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        try:
            from app.deepstream_worker import main
        except (ImportError, ValueError) as exc:
            self.skipTest(f"GStreamer bindings unavailable: {exc}")
        self.main = main
        self.pipeline = Mock(MAX_CAMERAS=5)
        self.repository = Mock(initialize_worker=AsyncMock(return_value=uuid4()), heartbeat=AsyncMock())
        self.manager = Mock(cameras={}, restore_cameras=AsyncMock(), shutdown=AsyncMock())
        self.consumer = Mock(connect=AsyncMock(), start=AsyncMock(), close=AsyncMock())
        self.stop_event = asyncio.Event()
        self.stop_event.set()
        self.persistence = Mock(start=AsyncMock(), stop=AsyncMock())

    async def run_worker(self):
        with patch.object(self.main, "PPEEventPersistence", return_value=self.persistence), \
             patch.object(self.main, "DynamicPipeline", return_value=self.pipeline), \
             patch.object(self.main, "CameraManager", return_value=self.manager), \
             patch.object(self.main, "CameraCommandConsumer", return_value=self.consumer):
            await self.main._run_worker(self.repository, self.stop_event)

    async def test_broker_startup_failure_still_cleans_up_pipeline_and_worker(self):
        self.consumer.connect.side_effect = RuntimeError("RabbitMQ unavailable")
        with self.assertRaisesRegex(RuntimeError, "RabbitMQ unavailable"):
            await self.run_worker()
        self.manager.shutdown.assert_awaited_once()
        self.consumer.close.assert_awaited_once()
        self.persistence.stop.assert_awaited_once()
        self.repository.heartbeat.assert_awaited_once_with(
            self.repository.initialize_worker.return_value, current_cameras=0, offline=True
        )

    async def test_consumer_close_failure_still_shuts_down_manager(self):
        self.consumer.close.side_effect = RuntimeError("Close failed")
        with self.assertRaisesRegex(RuntimeError, "Close failed"):
            await self.run_worker()
        self.manager.shutdown.assert_awaited_once()
        self.persistence.stop.assert_awaited_once()
        self.repository.heartbeat.assert_awaited_once()

    async def test_database_initialization_failure_shuts_down_pipeline(self):
        self.repository.initialize_worker.side_effect = RuntimeError("Database unavailable")
        with self.assertRaisesRegex(RuntimeError, "Database unavailable"):
            await self.run_worker()
        self.pipeline.shutdown.assert_called_once()


class WorkerLockTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.connection = AsyncMock()
        self.connection.execute.return_value = Mock(scalar_one=Mock(return_value=True))
        context = AsyncMock()
        context.__aenter__.return_value = self.connection
        self.engine = Mock(connect=Mock(return_value=context))

    async def test_failed_worker_releases_lock_before_connection_returns_to_pool(self):
        with self.assertRaisesRegex(RuntimeError, "Worker failed"):
            async with worker_lock(self.engine):
                raise RuntimeError("Worker failed")
        queries = [str(call.args[0]) for call in self.connection.execute.await_args_list]
        self.assertEqual(queries, ["SELECT pg_try_advisory_lock(:key)", "SELECT pg_advisory_unlock(:key)"])

    async def test_second_worker_is_rejected_without_unlocking_first_workers_lock(self):
        self.connection.execute.return_value.scalar_one.return_value = False
        with self.assertRaisesRegex(RuntimeError, "already running"):
            async with worker_lock(self.engine):
                self.fail("Second worker acquired the lock")
        self.assertEqual(self.connection.execute.await_count, 1)

    async def test_failed_unlock_invalidates_connection(self):
        self.connection.execute.side_effect = [Mock(scalar_one=Mock(return_value=True)), RuntimeError("DB lost")]
        with self.assertRaisesRegex(RuntimeError, "DB lost"):
            async with worker_lock(self.engine):
                pass
        self.connection.invalidate.assert_awaited_once()
