
import logging
import queue
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .ppe_association import PPEAssociation
from .ppe_decision_engine import PPEEventEngine

logger = logging.getLogger(__name__)


@dataclass
class CameraDecisionSession:
    camera_id: str
    session_id: str
    engine: PPEEventEngine
    camera_session_id: str | None = None
    ready: bool = True
    event_times: dict = field(default_factory=dict)
    clock_samples: deque = field(default_factory=deque)
    last_timestamp: float = 0.0
    last_arrival: float = field(default_factory=time.monotonic)


class MultiCameraPPEProcessor:
    def __init__(self, max_queue_size=2000, action_sink=None, snapshot_sink=None):
        self.association = PPEAssociation()
        self.sessions = {}
        self.queue = queue.Queue(maxsize=max_queue_size)
        self.lock = threading.RLock()
        self.submission_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.worker = None
        self.dropped_frames = 0
        self.action_sink = action_sink
        self.snapshot_sink = snapshot_sink
        self.snapshot_lock = threading.Lock()
        self.pending_snapshots = {}

    def start(self):
        with self.submission_lock:
            if self.worker and self.worker.is_alive():
                return

            self.stop_event.clear()
            self.worker = threading.Thread(
                target=self._run,
                name="ppe-decision-worker",
                daemon=True,
            )
            self.worker.start()

    def register_camera(self, camera_id, session_id=None, camera_session_id=None, ready=True):
        camera_id = str(camera_id)
        session_id = str(session_id or uuid.uuid4())

        with self.lock:
            existing = self.sessions.get(camera_id)

            if existing:
                if existing.session_id == session_id:
                    return
                self._close_camera(camera_id, "SESSION_REPLACED")

            engine = PPEEventEngine(
                # Tracking identity is distinct from the DB CameraSession ID.
                session_id=str(uuid.uuid4()),
                window_seconds=1.5,
                min_observations=5,
                min_evidence_seconds=0.8,
                violation_threshold=0.80,
                recovery_threshold=0.80,
                recovery_window_seconds=2.0,
                track_timeout_seconds=3.0,
            )

            self.sessions[camera_id] = CameraDecisionSession(
                camera_id=camera_id,
                session_id=session_id,
                engine=engine,
                camera_session_id=str(camera_session_id or session_id),
                ready=ready,
            )

        logger.info("PPE camera registered: %s", camera_id)

    def activate_camera(self, camera_id, camera_session_id):
        with self.lock:
            session = self.sessions[str(camera_id)]
            session.camera_session_id = str(camera_session_id)
            session.ready = True

    def submit_frame(
        self, camera_id, session_id, timestamp, objects, observed_at=None
    ):
        item = (
            str(camera_id),
            str(session_id),
            float(timestamp),
            tuple(dict(obj) for obj in objects),
            observed_at or datetime.now(timezone.utc),
        )

        # Synchronize acceptance with stop so no frame arrives after draining.
        with self.submission_lock:
            if self.stop_event.is_set():
                return False
            try:
                self.queue.put_nowait(item)
                return True
            except queue.Full:
                self.dropped_frames += 1
                return False

    def _run(self):
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                item = self.queue.get(timeout=0.2)
            except queue.Empty:
                self._expire_tracks()
                continue

            try:
                camera_id, session_id, timestamp, objects, observed_at = item

                with self.lock:
                    session = self.sessions.get(camera_id)

                    # Ignore queued frames from old camera sessions.
                    if (
                        session is None
                        or session.session_id != session_id
                        or not session.ready
                    ):
                        continue

                    observations = self.association.associate(
                        camera_id=camera_id,
                        timestamp=timestamp,
                        objects=objects,
                    )
                    session.clock_samples.append((timestamp, observed_at))
                    retention = max(session.engine.window_seconds, session.engine.recovery_window_seconds)
                    while session.clock_samples and session.clock_samples[0][0] < timestamp - retention:
                        session.clock_samples.popleft()

                    for observation in observations:
                        actions = session.engine.process(observation)

                        for action in actions:
                            self._handle_action(action, session, timestamp, observed_at)

                    session.last_timestamp = max(
                        session.last_timestamp, timestamp
                    )
                    session.last_arrival = time.monotonic()

            except Exception:
                logger.exception("PPE decision processing failed")
            finally:
                self.queue.task_done()

            self._expire_tracks()

    def _expire_tracks(self):
        # Queued observations may still refer to tracks that appear stale.
        if self.stop_event.is_set() or not self.queue.empty():
            return

        with self.lock:
            now = time.monotonic()
            observed_at = datetime.now(timezone.utc)

            for session in self.sessions.values():
                # Observation timestamps use monotonic time.
                # Do not expire based on a different clock.
                actions = session.engine.expire_tracks(now)

                for action in actions:
                    self._handle_action(action, session, now, observed_at)

    def _handle_action(self, action, session, timestamp, observed_at):
        event = action["event"]

        # Anchor each transition to its captured UTC time. Creation times are
        # preserved in this cache instead of being overwritten on every update.
        times = session.event_times.get(event["id"])
        if times is None:
            times = {
                key: next((utc for monotonic, utc in session.clock_samples if monotonic == event[key]),
                          observed_at + timedelta(seconds=event[key] - timestamp))
                for key in ("first_observed_at", "confirmed_at")
            }
            session.event_times[event["id"]] = times
        payload = event.copy()
        payload.update({key: times[key] for key in ("first_observed_at", "confirmed_at")})
        last_timestamp = event["last_observed_at"]
        previous_observation = times.get("_last_observation")
        if previous_observation is not None and previous_observation[0] == last_timestamp:
            last_observed_at = previous_observation[1]
        else:
            last_observed_at = next(
                (utc for monotonic, utc in session.clock_samples if monotonic == last_timestamp),
                observed_at + timedelta(seconds=last_timestamp - timestamp),
            )
        times["_last_observation"] = (last_timestamp, last_observed_at)
        payload["last_observed_at"] = last_observed_at
        payload["resolved_at"] = (
            observed_at + timedelta(seconds=event["resolved_at"] - timestamp)
            if event["resolved_at"] is not None else None
        )
        payload["camera_session_id"] = session.camera_session_id
        if self.action_sink is not None:
            self.action_sink({**action, "event": payload})

        if action["action"] == "CREATED" and self.snapshot_sink is not None:
            with self.snapshot_lock:
                self.pending_snapshots[event["id"]] = {
                    "event_id": event["id"], "camera_id": session.camera_id,
                    "session_id": session.session_id, "person_track_id": event["person_track_id"],
                    "ppe_type": event["ppe_type"], "requested_at": time.monotonic(),
                }
        elif action["action"] == "RESOLVED":
            session.event_times.pop(event["id"], None)
            self.complete_snapshot(event["id"])

        # UPDATED can occur every frame. Avoid noisy logs.
        if action["action"] == "UPDATED":
            return

        logger.info(
            "PPE event: action=%s camera=%s "
            "track=%s violation=%s status=%s",
            action["action"],
            event["camera_id"],
            event["person_track_id"],
            event["violation_type"],
            event["status"],
        )

    def snapshot_requests(self, camera_id, session_id, timestamp, objects):
        """Select evidence only for pending confirmations with matching PPE."""
        with self.snapshot_lock:
            now = time.monotonic()
            for event_id, request in list(self.pending_snapshots.items()):
                if now - request["requested_at"] > 5.0:
                    logger.warning("PPE evidence frame unavailable: event=%s", event_id)
                    del self.pending_snapshots[event_id]
            requests = [request.copy() for request in self.pending_snapshots.values()
                        if request["camera_id"] == str(camera_id)
                        and request["session_id"] == str(session_id)]
        if not requests:
            return []
        observations = self.association.associate(camera_id, timestamp, objects)
        return [request for request in requests if any(
            obs["track_id"] == request["person_track_id"]
            and obs["ppe"][request["ppe_type"]]["state"] == "MISSING"
            for obs in observations
        )]

    def complete_snapshot(self, event_id):
        with self.snapshot_lock:
            self.pending_snapshots.pop(event_id, None)

    def _close_camera(self, camera_id, reason):
        session = self.sessions.pop(str(camera_id), None)

        if session is None:
            return

        timestamp = time.monotonic()
        observed_at = datetime.now(timezone.utc)
        actions = session.engine.close_session(
            timestamp=timestamp,
            reason=reason,
        )

        for action in actions:
            self._handle_action(action, session, timestamp, observed_at)

        session.engine.checkpoint()

    def remove_camera(self, camera_id):
        with self.lock:
            self._close_camera(camera_id, "STREAM_STOPPED")

    def stop(self):
        with self.submission_lock:
            self.stop_event.set()

        if self.worker:
            self.worker.join(timeout=5)

            if self.worker.is_alive():
                raise RuntimeError(
                    "PPE worker did not stop cleanly"
                )
        else:
            # Frames can be submitted before pipeline startup completes.
            self._run()

        with self.lock:
            for camera_id in list(self.sessions):
                self._close_camera(camera_id, "PIPELINE_STOPPED")
