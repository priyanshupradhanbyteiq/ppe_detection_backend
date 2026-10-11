"""Demand-driven OpenCV capture shared by authenticated WebRTC viewers."""

import asyncio
import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from fractions import Fraction

import cv2
from aiortc import RTCBundlePolicy, RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription, VideoStreamTrack
from aiortc.mediastreams import MediaStreamError
from av import VideoFrame

from app.core.config import settings

logger = logging.getLogger(__name__)


class PreviewUnavailable(RuntimeError):
    pass


class OpenCVCameraSource:
    """One capture thread and one latest-frame slot, never an unbounded queue.

    VideoCapture construction/read/release all happen on its owning thread.
    OpenCV timeouts bound an unreachable camera's startup and shutdown.
    """

    def __init__(self, camera_id, url, *, capture_factory=None, max_width=None,
                 open_timeout=None, read_timeout=None):
        self.camera_id = str(camera_id)
        self.url = url
        self.max_width = max_width or settings.WEBRTC_MAX_WIDTH
        self.open_timeout = open_timeout or settings.WEBRTC_OPEN_TIMEOUT_SECONDS
        self.read_timeout = read_timeout or settings.WEBRTC_READ_TIMEOUT_SECONDS
        self.capture_factory = capture_factory or self._open_capture
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.updated = asyncio.Event()
        self.loop = asyncio.get_running_loop()
        self.latest = None
        self.sequence = 0
        self.arrivals = deque()
        self.read_ms = None
        self.state = "connecting"
        self.thread = threading.Thread(target=self._run, name=f"preview-{self.camera_id[:8]}", daemon=True)

    def _open_capture(self, url):
        return cv2.VideoCapture(url, cv2.CAP_FFMPEG, [
            cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, int(self.open_timeout * 1000),
            cv2.CAP_PROP_READ_TIMEOUT_MSEC, int(self.read_timeout * 1000),
        ])

    def start(self):
        self.thread.start()

    def _notify(self):
        if not self.loop.is_closed():
            try:
                self.loop.call_soon_threadsafe(self.updated.set)
            except RuntimeError:
                pass  # The process is shutting down its event loop.

    def _run(self):
        while not self.stopping.is_set():
            capture = None
            try:
                capture = self.capture_factory(self.url)
                if not capture.isOpened():
                    raise PreviewUnavailable("Camera could not be opened")
                while not self.stopping.is_set():
                    started = time.monotonic()
                    success, image = capture.read()
                    captured = time.monotonic()
                    if not success or image is None:
                        raise PreviewUnavailable("Camera stopped producing frames")
                    height, width = image.shape[:2]
                    if width > self.max_width:
                        image = cv2.resize(image, (self.max_width, max(2, round(height * self.max_width / width))))
                    image = image.copy()
                    with self.lock:
                        self.sequence += 1
                        self.latest = (self.sequence, image, captured)
                        self.read_ms = (captured - started) * 1000
                        self.arrivals.append(captured)
                        while len(self.arrivals) > 1 and self.arrivals[0] < captured - 2:
                            self.arrivals.popleft()
                        self.state = "live"
                    self._notify()
            except Exception:
                # Never include a credential-bearing RTSP URL in application logs.
                with self.lock:
                    self.state = "reconnecting"
                self._notify()
            finally:
                if capture is not None:
                    capture.release()
            if self.stopping.wait(1):
                break
        with self.lock:
            self.state = "closed"
        self._notify()

    async def next_frame(self, after_sequence, timeout=10):
        deadline = time.monotonic() + timeout
        while not self.stopping.is_set():
            self.updated.clear()
            with self.lock:
                latest = self.latest
            if latest is not None and latest[0] > after_sequence and time.monotonic() - latest[2] < 3:
                return latest
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(self.updated.wait(), remaining)
            except asyncio.TimeoutError:
                break
        raise PreviewUnavailable("Camera is unreachable or is not producing video frames.")

    def telemetry(self):
        with self.lock:
            age = (time.monotonic() - self.latest[2]) * 1000 if self.latest else None
            fps = ((len(self.arrivals) - 1) / (self.arrivals[-1] - self.arrivals[0])
                   if len(self.arrivals) >= 2 and self.arrivals[-1] > self.arrivals[0] else None)
            return {"state": self.state, "capture_fps": fps, "capture_read_ms": self.read_ms,
                    "last_frame_age_ms": age, "frames_captured": self.sequence}

    async def close(self):
        self.stopping.set()
        self.updated.set()
        if self.thread.is_alive():
            await asyncio.to_thread(self.thread.join, self.open_timeout + self.read_timeout + 2)
        if self.thread.is_alive():
            logger.warning("Camera preview capture did not exit within its timeout: camera=%s", self.camera_id)


class CameraVideoTrack(VideoStreamTrack):
    def __init__(self, source, max_fps):
        super().__init__()
        self.source = source
        self.max_fps = max_fps
        self.last_sequence = 0
        self.last_sent = 0.0
        self.origin = None
        self.skipped_frames = 0

    async def recv(self):
        if self.readyState != "live":
            raise MediaStreamError
        await asyncio.sleep(max(0, 1 / self.max_fps - (time.monotonic() - self.last_sent)))
        try:
            sequence, image, captured = await self.source.next_frame(self.last_sequence)
        except PreviewUnavailable:
            self.stop()
            raise MediaStreamError
        if self.readyState != "live":
            raise MediaStreamError
        if self.last_sequence:
            self.skipped_frames += max(0, sequence - self.last_sequence - 1)
        self.last_sequence = sequence
        self.last_sent = time.monotonic()
        if self.origin is None:
            self.origin = captured
        frame = VideoFrame.from_ndarray(image, format="bgr24")
        frame.pts = round((captured - self.origin) * 90000)
        frame.time_base = Fraction(1, 90000)
        return frame


@dataclass
class PreviewSession:
    camera_id: str
    owner_id: str
    source_key: tuple
    source: OpenCVCameraSource
    peer: RTCPeerConnection
    track: CameraVideoTrack
    last_seen: float


class CameraWebRTCService:
    def __init__(self, *, source_factory=None, ice_servers=None, max_peers=None, idle_seconds=None):
        self.source_factory = source_factory or OpenCVCameraSource
        self.ice_servers = settings.WEBRTC_ICE_SERVERS if ice_servers is None else ice_servers
        self.max_peers = max_peers or settings.WEBRTC_MAX_PEERS
        self.idle_seconds = idle_seconds or settings.WEBRTC_PEER_IDLE_SECONDS
        self.sessions = {}
        self.sources = {}
        self.lock = asyncio.Lock()
        self.reaper = None
        self.closing = set()

    async def negotiate(self, camera_id, url, owner_id, offer):
        session_id = str(offer.session_id)
        async with self.lock:
            if session_id in self.sessions:
                raise ValueError("Preview session already exists")
            if len(self.sessions) >= self.max_peers:
                raise PreviewUnavailable("Camera preview capacity is reached. Try again later.")
            configuration = RTCConfiguration(iceServers=[RTCIceServer(**server) for server in self.ice_servers],
                                             bundlePolicy=RTCBundlePolicy.MAX_BUNDLE)
            source_key = (str(camera_id), url)
            if source_key not in self.sources:
                source = self.source_factory(camera_id, url)
                source.start()
                self.sources[source_key] = [source, 0]
            source, references = self.sources[source_key]
            self.sources[source_key][1] = references + 1
            peer = RTCPeerConnection(configuration)
            track = CameraVideoTrack(source, settings.WEBRTC_MAX_FPS)
            session = PreviewSession(str(camera_id), str(owner_id), source_key, source, peer, track, time.monotonic())
            self.sessions[session_id] = session
            if self.reaper is None or self.reaper.done():
                self.reaper = asyncio.create_task(self._expire_sessions(), name="camera-preview-reaper")

        @peer.on("connectionstatechange")
        async def connection_changed():
            if peer.connectionState in {"failed", "closed"}:
                await self.close_session(session_id)

        @track.on("ended")
        def track_ended():
            asyncio.create_task(self.close_session(session_id))

        @peer.on("datachannel")
        def channel_received(channel):
            if channel.label != "telemetry":
                channel.close()
                return

            @channel.on("message")
            def message_received(message):
                if not isinstance(message, str) or len(message) > 1024:
                    return
                try:
                    payload = json.loads(message)
                    if not isinstance(payload, dict) or payload.get("type") != "ping":
                        return
                    session.last_seen = time.monotonic()
                    if channel.readyState == "open":
                        channel.send(json.dumps({"type": "pong", "id": payload.get("id"),
                                                 "source": {**source.telemetry(), "skipped_frames": track.skipped_frames}}))
                except (ValueError, TypeError):
                    return

        try:
            await source.next_frame(0, settings.WEBRTC_FIRST_FRAME_TIMEOUT_SECONDS)
            peer.addTrack(track)
            await peer.setRemoteDescription(RTCSessionDescription(sdp=offer.sdp, type=offer.type))
            answer = await peer.createAnswer()
            await asyncio.wait_for(peer.setLocalDescription(answer), 15)
            if not any(transceiver.kind == "video" and transceiver.currentDirection in {"sendrecv", "sendonly"}
                       for transceiver in peer.getTransceivers()):
                raise ValueError("Offer must receive a video track")
            if session_id not in self.sessions:
                raise PreviewUnavailable("Camera preview was cancelled.")
            session.last_seen = time.monotonic()
            return {"session_id": session_id, "sdp": peer.localDescription.sdp, "type": peer.localDescription.type}
        except BaseException:
            await self.close_session(session_id)
            raise

    async def close_session(self, session_id, owner_id=None, camera_id=None):
        session_id = str(session_id)
        async with self.lock:
            session = self.sessions.get(session_id)
            if session is None:
                return False
            if owner_id is not None and session.owner_id != str(owner_id):
                raise PermissionError("Preview session belongs to another user")
            if camera_id is not None and session.camera_id != str(camera_id):
                return False
            self.sessions.pop(session_id)
            source_entry = self.sources[session.source_key]
            source_entry[1] -= 1
            stop_source = source_entry[1] == 0
            if stop_source:
                self.sources.pop(session.source_key)
        # Cancellation of an HTTP request or the reaper must not abandon capture.
        cleanup = asyncio.create_task(self._release(session, stop_source))
        self.closing.add(cleanup)
        cleanup.add_done_callback(self.closing.discard)
        await asyncio.shield(cleanup)
        return True

    async def _release(self, session, stop_source):
        session.track.stop()
        try:
            await session.peer.close()
        finally:
            if stop_source:
                await session.source.close()

    async def _expire_sessions(self):
        while self.sessions:
            await asyncio.sleep(2)
            for session_id, session in list(self.sessions.items()):
                if time.monotonic() - session.last_seen > self.idle_seconds:
                    await self.close_session(session_id)

    async def shutdown(self):
        if self.reaper is not None:
            self.reaper.cancel()
            await asyncio.gather(self.reaper, return_exceptions=True)
            self.reaper = None
        await asyncio.gather(*(self.close_session(session_id) for session_id in list(self.sessions)))
        if self.closing:
            await asyncio.gather(*self.closing)


camera_webrtc_service = CameraWebRTCService()
