"""PPE persistence contracts with real SQL transactions and mocked uploads."""

import asyncio
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import numpy as np
from sqlalchemy import create_engine, event, select
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import Session

from app.deepstream_worker.ppe_processor import MultiCameraPPEProcessor
from app.models import Base, Camera, CameraSession, DeepStreamWorker, PPEEvent
from app.repositories.ppe_event_repository import PPEEventRepository
from app.services.ppe_event_persistence import PPEEventPersistence
from app.services.ppe_snapshot_service import PPESnapshotService
from test_camera_lifecycle import AsyncSQLiteSession
from test_ppe_processing import detections


class PPEPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://")
        @event.listens_for(self.engine, "connect")
        def foreign_keys(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")
        Base.metadata.create_all(self.engine)
        self.factory = lambda: AsyncSQLiteSession(self.engine)
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "outbox.sqlite3"
        self.worker_id, self.camera_id, self.session_id, self.tracking_id = [uuid4() for _ in range(4)]
        with Session(self.engine) as db, db.begin():
            db.add(DeepStreamWorker(id=self.worker_id, name="ppe-test", hostname="localhost"))
            db.add(Camera(id=self.camera_id, name="PPE camera", rtsp_url="rtsp://test", source_type="RTSP_STREAM"))
            db.flush()
            db.add(CameraSession(id=self.session_id, camera_id=self.camera_id,
                                 worker_id=self.worker_id, pipeline_id="test", source_index=0, status="CONNECTING"))
        self.snapshots = Mock(enabled=False)
        self.persistence = PPEEventPersistence(self.factory, self.path, self.snapshots)

    async def asyncTearDown(self):
        await self.persistence.stop()
        self.engine.dispose()
        self.tmp.cleanup()

    def payload(self, **changes):
        now = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        result = dict(id=str(uuid4()), camera_id=str(self.camera_id),
                      camera_session_id=str(self.session_id), session_id=str(self.tracking_id),
                      person_track_id=42, ppe_type="helmet", violation_type="NO_HARDHAT",
                      event_type="PPE_VIOLATION", severity="HIGH", status="OPEN",
                      confidence=0.9, violation_ratio=1.0, missing_count=5,
                      present_count=0, unknown_count=0, first_observed_at=now,
                      confirmed_at=now + timedelta(seconds=1), last_observed_at=now + timedelta(seconds=1),
                      resolved_at=None, resolution_reason=None)
        result.update(changes)
        return result

    def records(self):
        with Session(self.engine) as db:
            return db.scalars(select(PPEEvent).order_by(PPEEvent.confirmed_at)).all()

    async def test_missing_schema_reports_migration_and_closes_outbox(self):
        for code in ("42P01", "42703"):
            with self.subTest(sqlstate=code):
                session = AsyncMock()
                cause = RuntimeError("missing PPE table or column")
                cause.sqlstate = code
                session.execute.side_effect = ProgrammingError("SELECT", {}, cause)
                session.__aenter__.return_value = session
                persistence = PPEEventPersistence(lambda: session, self.path, self.snapshots)
                with self.assertRaisesRegex(RuntimeError, "alembic upgrade n7p8q9"):
                    await persistence.start(self.worker_id)
                self.assertTrue(persistence.closed)
                self.assertEqual(persistence.tasks, [])

    async def test_unrelated_database_error_is_not_reported_as_missing_schema(self):
        session = AsyncMock()
        cause = RuntimeError("permission denied")
        cause.sqlstate = "42501"
        failure = ProgrammingError("SELECT", {}, cause)
        session.execute.side_effect = failure
        session.__aenter__.return_value = session
        self.persistence.session_factory = lambda: session
        with self.assertRaises(ProgrammingError) as raised:
            await self.persistence.start(self.worker_id)
        self.assertIs(raised.exception, failure)
        self.assertTrue(self.persistence.closed)

    async def test_retry_is_idempotent_and_resolution_keeps_final_statistics(self):
        payload = self.payload()
        await self.persistence.persist_action({"action": "CREATED", "event": payload})
        await self.persistence.persist_action({"action": "CREATED", "event": payload})
        resolved = dict(payload, status="RESOLVED", missing_count=17, confidence=None,
                        last_observed_at=payload["confirmed_at"] + timedelta(seconds=9),
                        resolved_at=payload["confirmed_at"] + timedelta(seconds=10),
                        resolution_reason="STREAM_STOPPED")
        await self.persistence.persist_action({"action": "RESOLVED", "event": resolved})
        await self.persistence.persist_action({"action": "UPDATED", "event": payload})
        records = self.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, "RESOLVED")
        self.assertEqual(records[0].missing_count, 17)
        self.assertIsNone(records[0].confidence)
        self.assertEqual(records[0].session_id, self.session_id)
        self.assertEqual(records[0].tracking_session_id, self.tracking_id)
        self.assertEqual(records[0].first_observed_at.replace(tzinfo=timezone.utc), payload["first_observed_at"])

    async def test_repeat_violation_after_recovery_gets_new_event_uuid(self):
        first = self.payload()
        await self.persistence.persist_action({"action": "CREATED", "event": first})
        with self.assertRaises(IntegrityError):
            await self.persistence.persist_action({"action": "CREATED", "event": self.payload()})
        first.update(status="RESOLVED", resolved_at=first["confirmed_at"],
                     resolution_reason="PPE_COMPLIANCE_RESTORED")
        await self.persistence.persist_action({"action": "RESOLVED", "event": first})
        await self.persistence.persist_action({"action": "CREATED", "event": self.payload()})
        self.assertEqual([record.status for record in self.records()], ["RESOLVED", "OPEN"])

    async def test_recovery_only_closes_this_workers_inactive_sessions(self):
        first = self.payload()
        await self.persistence.persist_action({"action": "CREATED", "event": first})
        other_worker, other_camera, other_session = [uuid4() for _ in range(3)]
        with Session(self.engine) as db, db.begin():
            db.get(CameraSession, self.session_id).stopped_at = datetime.now(timezone.utc)
            db.add(DeepStreamWorker(id=other_worker, name="other", hostname="localhost"))
            db.add(Camera(id=other_camera, name="Other camera", rtsp_url="rtsp://other", source_type="RTSP_STREAM"))
            db.flush()
            db.add(CameraSession(id=other_session, camera_id=other_camera, worker_id=other_worker,
                                 pipeline_id="test", source_index=0, status="CONNECTING"))
        other = self.payload(camera_id=str(other_camera), camera_session_id=str(other_session),
                             session_id=str(uuid4()))
        await self.persistence.persist_action({"action": "CREATED", "event": other})
        async with self.factory() as db, db.begin():
            await PPEEventRepository.recover_worker(db, self.worker_id)
        records = {str(record.id): record for record in self.records()}
        self.assertEqual(records[first["id"]].resolution_reason, "SESSION_INTERRUPTED")
        self.assertEqual(records[other["id"]].status, "OPEN")

    async def test_outbox_survives_restart_and_preserves_lifecycle_order(self):
        payload = self.payload()
        self.persistence.submit({"action": "CREATED", "event": payload})
        payload.update(status="RESOLVED", missing_count=13, resolved_at=payload["confirmed_at"],
                       resolution_reason="PIPELINE_STOPPED")
        self.persistence.submit({"action": "RESOLVED", "event": payload})
        await self.persistence.stop()  # No consumer started: both jobs remain durable.
        self.persistence = PPEEventPersistence(self.factory, self.path, self.snapshots)
        await self.persistence.start(self.worker_id)
        await self.persistence.stop()
        self.assertEqual(len(self.records()), 1)
        self.assertEqual(self.records()[0].missing_count, 13)
        self.assertEqual(self.records()[0].status, "RESOLVED")

    async def test_database_failure_retries_without_resolution_overtaking_create(self):
        await self.persistence.start(self.worker_id)
        operation = self.persistence.persist_action
        seen = []
        async def fail_once(action):
            seen.append(action["action"])
            if len(seen) == 1:
                raise RuntimeError("temporary database outage")
            await operation(action)
        self.persistence.persist_action = fail_once
        payload = self.payload()
        with self.assertLogs("app.services.ppe_event_persistence", level="ERROR"):
            self.persistence.submit({"action": "CREATED", "event": payload})
            resolved = dict(payload, status="RESOLVED", resolved_at=payload["confirmed_at"],
                            resolution_reason="STREAM_STOPPED")
            self.persistence.submit({"action": "RESOLVED", "event": resolved})
            await self.persistence.stop()
        self.assertEqual(seen, ["CREATED", "CREATED", "RESOLVED"])
        self.assertEqual(self.records()[0].status, "RESOLVED")

    async def test_updates_are_throttled_but_resolution_always_writes(self):
        payload = self.payload()
        self.persistence.submit({"action": "CREATED", "event": payload})
        for count in range(20):
            self.persistence.submit({"action": "UPDATED", "event": dict(payload, missing_count=count)})
        self.persistence.submit({"action": "RESOLVED", "event": dict(
            payload, status="RESOLVED", missing_count=30, resolved_at=payload["confirmed_at"],
            resolution_reason="STREAM_STOPPED")})
        self.assertEqual(self.persistence.db.execute("SELECT count(*) FROM actions").fetchone()[0], 2)
        await self.persistence.start(self.worker_id)
        await self.persistence.stop()
        self.assertEqual(self.records()[0].missing_count, 30)

    async def test_snapshot_upload_waits_for_event_commit_and_retries(self):
        self.snapshots.enabled = True
        self.snapshots.encode = PPESnapshotService.encode
        payload = self.payload()
        attempts = []
        async def upload(path, camera_id, event_id):
            self.assertEqual(len(self.records()), 1)
            self.assertTrue(Path(path).read_bytes().startswith(b"\xff\xd8"))
            attempts.append(event_id)
            if len(attempts) == 1:
                raise RuntimeError("temporary Cloudinary outage")
            return dict(snapshot_url="https://example.test/evidence.jpg", snapshot_public_id="ppe_events/test")
        self.snapshots.upload_file = upload
        await self.persistence.start(self.worker_id)
        self.persistence.submit({"action": "CREATED", "event": payload})
        request = {"event_id": payload["id"], "camera_id": str(self.camera_id)}
        self.assertTrue(self.persistence.submit_snapshot(request, np.zeros((8, 8, 3), dtype=np.uint8),
                                                       payload["confirmed_at"]))
        with self.assertLogs("app.services.ppe_event_persistence", level="ERROR"):
            await self.persistence.stop()
        self.assertEqual(attempts, [payload["id"], payload["id"]])
        record = self.records()[0]
        self.assertEqual(record.snapshot_url, "https://example.test/evidence.jpg")
        self.assertEqual(record.snapshot_public_id, "ppe_events/test")
        self.assertEqual(record.snapshot_captured_at.replace(tzinfo=timezone.utc), payload["confirmed_at"])

    async def test_staged_snapshot_survives_worker_restart(self):
        payload = self.payload()
        await self.persistence.persist_action({"action": "CREATED", "event": payload})
        self.snapshots.encode = PPESnapshotService.encode
        await self.persistence._stage_snapshot({"event_id": payload["id"], "camera_id": str(self.camera_id)},
                                               np.zeros((4, 4, 3), dtype=np.uint8), payload["confirmed_at"])
        await self.persistence.stop()
        self.snapshots.enabled = True
        self.snapshots.upload_file = AsyncMock(return_value=dict(
            snapshot_url="https://example.test/replayed.jpg", snapshot_public_id="replayed"))
        self.persistence = PPEEventPersistence(self.factory, self.path, self.snapshots)
        await self.persistence.start(self.worker_id)
        await self.persistence.stop()
        self.snapshots.upload_file.assert_awaited_once()
        self.assertEqual(self.records()[0].snapshot_public_id, "replayed")

    async def test_thread_submission_and_utc_conversion_preserve_creation_times(self):
        await self.persistence.start(self.worker_id)
        processor = MultiCameraPPEProcessor(action_sink=self.persistence.submit)
        processor.register_camera(self.camera_id, str(self.session_id), ready=False)
        engine = processor.sessions[str(self.camera_id)].engine
        processor.submit_frame(self.camera_id, self.session_id, 100, detections())
        processor.activate_camera(self.camera_id, self.session_id)
        first = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        # Drop the frame submitted while the session was not ready.
        processor.queue.get_nowait()
        processor.queue.task_done()
        for index in range(5):
            processor.submit_frame(self.camera_id, self.session_id, 100 + index * .25, detections(),
                                   observed_at=first + timedelta(seconds=index * .25))
        thread = threading.Thread(target=processor.stop)
        thread.start()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        await self.persistence.stop()
        record = self.records()[0]
        self.assertEqual(record.first_observed_at.replace(tzinfo=timezone.utc), first)
        self.assertEqual(record.confirmed_at.replace(tzinfo=timezone.utc), first + timedelta(seconds=1))
        self.assertEqual(record.tracking_session_id, UUID(engine.events[0]["session_id"]))


if __name__ == "__main__":
    unittest.main()
