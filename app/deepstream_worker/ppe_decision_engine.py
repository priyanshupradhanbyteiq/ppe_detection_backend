
import math
import uuid
from collections import deque


PPE_TYPES = ("helmet", "mask", "vest")
VALID_STATES = {"PRESENT", "MISSING", "UNKNOWN"}

VIOLATION_TYPES = {
    "helmet": "NO_HARDHAT",
    "mask": "NO_MASK",
    "vest": "NO_SAFETY_VEST",
}


class PPEEventEngine:
    def __init__(
        self,
        window_seconds=1.5,
        min_observations=5,
        min_evidence_seconds=0.8,
        violation_threshold=0.80,
        recovery_threshold=0.80,
        recovery_window_seconds=2.0,
        track_timeout_seconds=3.0,
        unknown_grace_seconds=2.0,
        session_id=None,
    ):
        self.window_seconds = window_seconds
        self.min_observations = min_observations
        self.min_evidence_seconds = min_evidence_seconds
        self.violation_threshold = violation_threshold
        self.recovery_threshold = recovery_threshold
        self.recovery_window_seconds = recovery_window_seconds
        self.track_timeout_seconds = track_timeout_seconds
        self.unknown_grace_seconds = unknown_grace_seconds

        # Unique stream/tracking session.
        self.session_id = session_id or str(uuid.uuid4())

        # (camera, track, PPE) -> deque of observations
        self.history = {}

        # (camera, track) -> last observed timestamp
        self.last_seen = {}

        # (camera, track, PPE) -> last known valid state time
        self.last_valid_seen = {}

        # Event identity -> event dictionary
        self.open_events = {}

        # Persistence and restart recovery belong to the external consumer.
        self.events = []

    @staticmethod
    def normalize(observation):
        camera_id = str(observation["camera_id"])
        track_id = int(observation["track_id"])
        timestamp = float(observation["timestamp"])

        if not camera_id or track_id < 0:
            raise ValueError("Invalid camera or track")

        if not math.isfinite(timestamp):
            raise ValueError("Invalid timestamp")

        raw_ppe = observation.get("ppe", {})
        normalized_ppe = {}

        for equipment in PPE_TYPES:
            item = raw_ppe.get(equipment, {})
            state = item.get("state", "UNKNOWN")
            confidence = item.get("confidence")

            if state not in VALID_STATES:
                state = "UNKNOWN"

            if confidence is not None:
                confidence = float(confidence)

            if (
                confidence is not None
                and (
                    not math.isfinite(confidence)
                    or not 0 <= confidence <= 1
                )
            ):
                confidence = None
                state = "UNKNOWN"

            if state == "UNKNOWN" or confidence is None:
                state = "UNKNOWN"
                confidence = None

            normalized_ppe[equipment] = {
                "state": state,
                "confidence": confidence,
            }

        return {
            "camera_id": camera_id,
            "track_id": track_id,
            "timestamp": timestamp,
            "ppe": normalized_ppe,
        }

    def _get_history(self, key):
        if key not in self.history:
            self.history[key] = deque()
        return self.history[key]

    @staticmethod
    def _calculate_stats(history):
        missing = sum(
            x["state"] == "MISSING" for x in history
        )
        present = sum(
            x["state"] == "PRESENT" for x in history
        )
        unknown = sum(
            x["state"] == "UNKNOWN" for x in history
        )

        valid = missing + present
        ratio = missing / valid if valid else 0.0
        present_ratio = present / valid if valid else 0.0

        valid_samples = [
            x for x in history
            if x["state"] != "UNKNOWN"
        ]

        duration = (
            valid_samples[-1]["timestamp"]
            - valid_samples[0]["timestamp"]
            if len(valid_samples) >= 2 else 0.0
        )

        return {
            "missing": missing,
            "present": present,
            "unknown": unknown,
            "valid": valid,
            "violation_ratio": ratio,
            "present_ratio": present_ratio,
            "duration": duration,
        }

    def _window_stats(self, history, now, seconds):
        samples = [
            x for x in history
            if now - seconds <= x["timestamp"] <= now
        ]
        return self._calculate_stats(samples)

    def _enough_evidence(self, stats):
        return (
            stats["valid"] >= self.min_observations
            and stats["duration"] >= self.min_evidence_seconds
        )

    def _event_key(self, camera_id, track_id, equipment):
        return (
            self.session_id,
            camera_id,
            track_id,
            equipment,
        )

    def _create_event(
        self, key, timestamp, stats, confidence, first_observed_at
    ):
        _, camera_id, track_id, equipment = key

        event = {
            "id": str(uuid.uuid4()),
            "session_id": self.session_id,
            "camera_id": camera_id,
            "person_track_id": track_id,
            "ppe_type": equipment,
            "violation_type": VIOLATION_TYPES[equipment],
            "event_type": "PPE_VIOLATION",
            "status": "OPEN",
            "severity": "HIGH",
            "confidence": confidence,
            "violation_ratio": round(
                stats["violation_ratio"], 4
            ),
            "missing_count": stats["missing"],
            "present_count": stats["present"],
            "unknown_count": stats["unknown"],
            "first_observed_at": first_observed_at,
            "confirmed_at": timestamp,
            "last_observed_at": timestamp,
            "resolved_at": None,
            "resolution_reason": None,
            "snapshot_url": None,
            "clip_url": None,
        }

        self.events.append(event)
        self.open_events[key] = event

        return {
            "action": "CREATED",
            "event": event.copy(),
        }

    def _update_event(
        self, event, timestamp, stats, confidence
    ):
        event["last_observed_at"] = timestamp
        event["violation_ratio"] = round(
            stats["violation_ratio"], 4
        )
        event["confidence"] = confidence
        event["missing_count"] = stats["missing"]
        event["present_count"] = stats["present"]
        event["unknown_count"] = stats["unknown"]

        # Do not rewrite JSON on every frame.
        return {
            "action": "UPDATED",
            "event": event.copy(),
        }

    def _resolve_event(self, key, timestamp, reason):
        event = self.open_events.pop(key, None)

        if event is None:
            return None

        event["status"] = "RESOLVED"
        event["resolved_at"] = timestamp
        event["resolution_reason"] = reason

        return {
            "action": "RESOLVED",
            "event": event.copy(),
        }

    def process(self, observation):
        """
        Process one person observation.

        Returns a list of event actions:
        CREATED, UPDATED or RESOLVED.
        """
        obs = self.normalize(observation)

        camera_id = obs["camera_id"]
        track_id = obs["track_id"]
        timestamp = obs["timestamp"]

        person_key = (camera_id, track_id)
        previous_seen = self.last_seen.get(person_key)

        # Reject out-of-order observations for this track.
        if previous_seen is not None and timestamp < previous_seen:
            return []

        self.last_seen[person_key] = timestamp

        actions = []

        for equipment, item in obs["ppe"].items():
            key = self._event_key(
                camera_id, track_id, equipment
            )
            history_key = (
                camera_id, track_id, equipment
            )

            history = self._get_history(history_key)
            history.append({
                "timestamp": timestamp,
                "state": item["state"],
                "confidence": item["confidence"],
            })

            # Keep enough history for recovery evaluation.
            retention = max(
                self.window_seconds,
                self.recovery_window_seconds,
            )

            while (
                history
                and history[0]["timestamp"]
                < timestamp - retention
            ):
                history.popleft()

            if item["state"] != "UNKNOWN":
                self.last_valid_seen[history_key] = timestamp

            violation_stats = self._window_stats(
                history, timestamp, self.window_seconds
            )
            recovery_stats = self._window_stats(
                history,
                timestamp,
                self.recovery_window_seconds,
            )

            event = self.open_events.get(key)

            # ------------------------------------
            # No active event: evaluate violation
            # ------------------------------------
            if event is None:
                if (
                    self._enough_evidence(violation_stats)
                    and violation_stats["violation_ratio"]
                    >= self.violation_threshold
                ):
                    missing_confidences = [
                        x["confidence"]
                        for x in history
                        if (
                            x["state"] == "MISSING"
                            and x["timestamp"]
                            >= timestamp - self.window_seconds
                        )
                    ]

                    confidence = (
                        sum(missing_confidences)
                        / len(missing_confidences)
                    )

                    actions.append(
                        self._create_event(
                            key,
                            timestamp,
                            violation_stats,
                            round(confidence, 4),
                            next(
                                x["timestamp"] for x in history
                                if x["state"] == "MISSING"
                                and x["timestamp"] >= timestamp - self.window_seconds
                            ),
                        )
                    )

                continue

            # ------------------------------------
            # Active event: evaluate recovery
            # ------------------------------------
            if (
                self._enough_evidence(recovery_stats)
                and recovery_stats["present_ratio"]
                >= self.recovery_threshold
            ):
                actions.append(
                    self._resolve_event(
                        key,
                        timestamp,
                        "PPE_COMPLIANCE_RESTORED",
                    )
                )
                continue

            # Still open; do not create a duplicate.
            actions.append(
                self._update_event(
                    event,
                    timestamp,
                    violation_stats,
                    item["confidence"],
                )
            )

        return actions

    def expire_tracks(self, now):
        """
        Call periodically using the same time domain
        as observation timestamps.
        """
        actions = []

        for person_key, last_seen in list(
            self.last_seen.items()
        ):
            if now - last_seen < self.track_timeout_seconds:
                continue

            camera_id, track_id = person_key

            for equipment in PPE_TYPES:
                key = self._event_key(
                    camera_id, track_id, equipment
                )
                result = self._resolve_event(
                    key,
                    now,
                    "PERSON_LEFT_CAMERA",
                )
                if result is not None:
                    actions.append(result)

                history_key = (
                    camera_id, track_id, equipment
                )
                self.history.pop(history_key, None)
                self.last_valid_seen.pop(
                    history_key, None
                )

            del self.last_seen[person_key]

        return actions

    def checkpoint(self):
        """Event actions are persisted by the external consumer."""
        return None

    def close_session(self, timestamp, reason="STREAM_STOPPED"):
        """Close active events before a tracking-session reset."""
        actions = []

        for key in list(self.open_events):
            result = self._resolve_event(
                key, timestamp, reason
            )
            if result is not None:
                actions.append(result)

        self.history.clear()
        self.last_seen.clear()
        self.last_valid_seen.clear()
        self.session_id = str(uuid.uuid4())
        return actions
