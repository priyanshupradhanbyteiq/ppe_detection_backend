"""Summary API contracts against real SQL rows, without external services."""

import unittest
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from app.api.v1.cameras import router
from app.core.database import get_db
from app.dependecies.auth import get_current_user
from app.models import Base, Camera, PPEEvent
from test_camera_lifecycle import AsyncSQLiteSession


class CameraSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://")

        @event.listens_for(self.engine, "connect")
        def foreign_keys(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(self.engine)

        async def database():
            async with AsyncSQLiteSession(self.engine) as db:
                yield db

        app = FastAPI()
        app.include_router(router, prefix="/api/v1/cameras")
        app.dependency_overrides[get_db] = database
        app.dependency_overrides[get_current_user] = lambda: object()
        self.client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()
        self.engine.dispose()

    async def test_empty_database_returns_numeric_zero_counts(self):
        response = await self.client.get("/api/v1/cameras/summary")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for field in ("total", "active_count", "inactive_count", "ppe_event_count"):
            self.assertEqual(data[field], 0)
            self.assertIs(type(data[field]), int)
        self.assertEqual(data["cameras"], [])

    async def test_counts_open_and_resolved_occurrences_across_cameras(self):
        camera_ids = [uuid4(), uuid4()]
        tracking_id = uuid4()
        now = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        with Session(self.engine) as db, db.begin():
            db.add_all([
                Camera(id=camera_ids[0], name="Requested camera", rtsp_url="rtsp://test/1",
                       source_type="RTSP_STREAM", status="ERROR", desired_state="RUNNING"),
                Camera(id=camera_ids[1], name="Stopped camera", rtsp_url="rtsp://test/2",
                       source_type="RTSP_STREAM", status="OFFLINE", desired_state="STOPPED"),
            ])
            db.flush()
            for camera_id, status in (
                (camera_ids[0], "RESOLVED"), (camera_ids[0], "OPEN"), (camera_ids[1], "OPEN"),
            ):
                db.add(PPEEvent(
                    camera_id=camera_id, tracking_session_id=tracking_id,
                    person_track_id=42, ppe_type="helmet", violation_type="NO_HARDHAT",
                    status=status, first_observed_at=now, confirmed_at=now,
                    last_observed_at=now, resolved_at=now if status == "RESOLVED" else None,
                    resolution_reason="PPE_COMPLIANCE_RESTORED" if status == "RESOLVED" else None,
                ))

        response = await self.client.get("/api/v1/cameras/summary")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total"], 2)
        self.assertEqual(data["active_count"], 1)
        self.assertEqual(data["inactive_count"], 1)
        self.assertEqual(data["ppe_event_count"], 3)
        self.assertEqual(data["breakdown"]["by_status"], {"ERROR": 1, "OFFLINE": 1})
        self.assertEqual(len(data["cameras"]), 2)


if __name__ == "__main__":
    unittest.main()
