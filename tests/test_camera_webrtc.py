"""Actual local WebRTC transport with deterministic OpenCV capture frames."""
import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import numpy as np
from aiortc import RTCBundlePolicy, RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.v1.camera_webrtc import router, get_preview_service
from app.core.config import settings
from app.core.database import get_db
from app.dependecies.auth import get_current_user
from app.schemas.camera_webrtc import WebRTCOffer
from app.services.camera_webrtc_service import CameraWebRTCService, OpenCVCameraSource, PreviewUnavailable


class SyntheticCapture:
    def __init__(self, url):
        self.released = False
        self.count = 0

    def isOpened(self):
        return True

    def read(self):
        time.sleep(1 / 30)
        self.count += 1
        return True, np.full((120, 160, 3), (self.count * 35) % 200, dtype=np.uint8)

    def release(self):
        self.released = True


class CameraWebRTCTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sources = []
        self.clients = []
        self.camera_id = uuid4()
        self.owner_id = uuid4()

        def source_factory(camera_id, url):
            source = OpenCVCameraSource(camera_id, url, capture_factory=SyntheticCapture)
            self.sources.append(source)
            return source

        self.service = CameraWebRTCService(source_factory=source_factory, ice_servers=[])
        app = FastAPI()
        app.include_router(router, prefix="/api/v1/cameras")
        app.dependency_overrides[get_preview_service] = lambda: self.service
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=self.owner_id)
        app.dependency_overrides[get_db] = lambda: object()
        self.app = app
        self.http = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        self.camera_mock = patch("app.api.v1.camera_webrtc.CameraRepository.get_by_id", new_callable=AsyncMock)
        self.get_camera = self.camera_mock.start()
        self.get_camera.return_value = SimpleNamespace(rtsp_url="rtsp://synthetic-camera/stream")

    async def asyncTearDown(self):
        await self.http.aclose()
        await asyncio.gather(*(client.close() for client in self.clients))
        await self.service.shutdown()
        self.camera_mock.stop()
        for source in self.sources:
            self.assertFalse(source.thread.is_alive(), "Preview capture thread leaked")

    async def make_offer(self):
        client = RTCPeerConnection(RTCConfiguration(iceServers=[], bundlePolicy=RTCBundlePolicy.MAX_BUNDLE))
        self.clients.append(client)
        client.addTransceiver("video", direction="recvonly")
        channel = client.createDataChannel("telemetry", ordered=False, maxRetransmits=0)
        await client.setLocalDescription(await client.createOffer())
        offer = WebRTCOffer(session_id=uuid4(), type="offer", sdp=client.localDescription.sdp)
        return client, channel, offer

    async def connect(self, client, offer, owner_id=None):
        answer = await self.service.negotiate(self.camera_id, "rtsp://test", owner_id or self.owner_id, offer)
        await client.setRemoteDescription(RTCSessionDescription(sdp=answer["sdp"], type=answer["type"]))
        async def connected():
            while client.connectionState not in {"connected", "failed"}:
                await asyncio.sleep(0.01)
            self.assertEqual(client.connectionState, "connected")
        await asyncio.wait_for(connected(), 10)

    async def test_http_handshake_receives_changing_video_and_capture_telemetry(self):
        client, channel, offer = await self.make_offer()
        track_received = asyncio.get_running_loop().create_future()
        pong = asyncio.get_running_loop().create_future()

        @client.on("track")
        def received(track):
            track_received.set_result(track)

        @channel.on("open")
        def opened():
            channel.send(json.dumps({"type": "ping", "id": 42}))

        @channel.on("message")
        def message(payload):
            if not pong.done():
                pong.set_result(json.loads(payload))

        response = await self.http.post(f"/api/v1/cameras/{self.camera_id}/webrtc/offer", json=offer.model_dump(mode="json"))
        self.assertEqual(response.status_code, 200, response.text)
        answer = response.json()
        await client.setRemoteDescription(RTCSessionDescription(sdp=answer["sdp"], type=answer["type"]))
        track = await asyncio.wait_for(track_received, 10)
        first = await asyncio.wait_for(track.recv(), 10)
        second = await asyncio.wait_for(track.recv(), 10)
        self.assertEqual((first.width, first.height), (160, 120))
        self.assertGreater(second.pts, first.pts)
        self.assertFalse(np.array_equal(first.to_ndarray(), second.to_ndarray()))
        payload = await asyncio.wait_for(pong, 10)
        self.assertEqual(payload["id"], 42)
        self.assertEqual(payload["source"]["state"], "live")
        self.assertGreater(payload["source"]["frames_captured"], 0)
        self.assertGreater(payload["source"]["capture_fps"], 0)
        response = await self.http.delete(f"/api/v1/cameras/{self.camera_id}/webrtc/{offer.session_id}")
        self.assertEqual(response.status_code, 204)
        self.assertEqual(self.service.sessions, {})
        self.assertEqual(self.service.sources, {})

    async def test_two_viewers_share_capture_and_close_only_after_last_viewer(self):
        first_client, _, first = await self.make_offer()
        second_client, _, second = await self.make_offer()
        await self.connect(first_client, first)
        await self.connect(second_client, second)
        self.assertEqual(len(self.sources), 1)
        await self.service.close_session(first.session_id, self.owner_id, self.camera_id)
        self.assertTrue(self.sources[0].thread.is_alive())
        self.assertEqual(len(self.service.sessions), 1)
        await self.service.close_session(second.session_id, self.owner_id, self.camera_id)
        self.assertFalse(self.sources[0].thread.is_alive())

    async def test_invalid_offer_releases_source(self):
        offer = WebRTCOffer(session_id=uuid4(), type="offer", sdp="invalid")
        with self.assertRaises(ValueError):
            await self.service.negotiate(self.camera_id, "rtsp://test", self.owner_id, offer)
        self.assertEqual(self.service.sessions, {})
        self.assertEqual(self.service.sources, {})
        self.assertFalse(self.sources[0].thread.is_alive())

    async def test_unreachable_capture_has_bounded_startup_and_cleanup(self):
        class Unreachable(SyntheticCapture):
            def isOpened(self):
                return False
        self.service.source_factory = lambda camera_id, url: OpenCVCameraSource(camera_id, url, capture_factory=Unreachable)
        _, _, offer = await self.make_offer()
        with patch.object(settings, "WEBRTC_FIRST_FRAME_TIMEOUT_SECONDS", 0.1):
            with self.assertRaises(PreviewUnavailable):
                await self.service.negotiate(self.camera_id, "rtsp://test", self.owner_id, offer)
        self.assertEqual(self.service.sessions, {})
        self.assertEqual(self.service.sources, {})

    async def test_cleanup_requires_owner_and_is_idempotent(self):
        client, _, offer = await self.make_offer()
        await self.connect(client, offer, uuid4())
        response = await self.http.delete(f"/api/v1/cameras/{self.camera_id}/webrtc/{offer.session_id}")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(len(self.service.sessions), 1)
        await self.service.close_session(offer.session_id)
        response = await self.http.delete(f"/api/v1/cameras/{self.camera_id}/webrtc/{offer.session_id}")
        self.assertEqual(response.status_code, 204)

    async def test_missing_camera_and_unsupported_source_do_not_start_capture(self):
        _, _, offer = await self.make_offer()
        self.get_camera.return_value = None
        response = await self.http.post(f"/api/v1/cameras/{self.camera_id}/webrtc/offer", json=offer.model_dump(mode="json"))
        self.assertEqual(response.status_code, 404)
        self.get_camera.return_value = SimpleNamespace(rtsp_url="file:///etc/test")
        response = await self.http.post(f"/api/v1/cameras/{self.camera_id}/webrtc/offer", json=offer.model_dump(mode="json"))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.sources, [])

    async def test_abandoned_session_expires_and_releases_capture(self):
        client, _, offer = await self.make_offer()
        await self.connect(client, offer)
        self.service.sessions[str(offer.session_id)].last_seen -= 100
        await asyncio.sleep(2.1)
        self.assertEqual(self.service.sessions, {})
        self.assertFalse(self.sources[0].thread.is_alive())

    async def test_signaling_requires_authentication(self):
        del self.app.dependency_overrides[get_current_user]
        response = await self.http.get("/api/v1/cameras/webrtc/config")
        self.assertEqual(response.status_code, 401)
        response = await self.http.delete(f"/api/v1/cameras/{self.camera_id}/webrtc/{uuid4()}")
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
