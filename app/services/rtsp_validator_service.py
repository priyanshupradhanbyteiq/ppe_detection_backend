import asyncio
import os
import socket
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import cv2
import numpy as np


class RTSPValidatorService:
    """
    Asynchronously validates live RTSP/video stream URLs using OpenCV.
    - Runs fast TCP socket pre-probing.
    - Streams and verifies feed for a continuous 5-second interval.
    - Captures the stabilized 5th-second video snapshot for Cloudinary thumbnail storage.
    """

    ALLOWED_SCHEMES = {"rtsp", "rtsps", "http", "https"}

    def __init__(self, default_duration_seconds: int = 5):
        self.default_duration_seconds = default_duration_seconds

    def parse_and_validate_url(self, rtsp_url: str) -> Tuple[str, str, int]:
        """
        Validates standard URL structure and protocol scheme.
        Returns (scheme, hostname, port).
        """
        if not rtsp_url or not isinstance(rtsp_url, str):
            raise ValueError("RTSP URL must be a non-empty string.")

        parsed = urlparse(rtsp_url.strip())
        scheme = parsed.scheme.lower()
        if scheme not in self.ALLOWED_SCHEMES:
            allowed = ", ".join(self.ALLOWED_SCHEMES)
            raise ValueError(
                f"Invalid URL protocol '{parsed.scheme}'. Must start with one of: {allowed}"
            )

        if not parsed.hostname:
            raise ValueError("RTSP URL must contain a valid hostname or IP address.")

        port = parsed.port or (554 if "rtsp" in scheme else (443 if scheme == "https" else 80))
        return scheme, parsed.hostname, port

    def _check_socket_reachability(self, host: str, port: int, timeout_sec: float = 2.0) -> Tuple[bool, Optional[str]]:
        """
        Fast TCP pre-flight check to verify if the camera IP and port are reachable
        before launching OpenCV VideoCapture (prevents C++ socket freeze on dead IPs).
        """
        try:
            with socket.create_connection((host, port), timeout=timeout_sec):
                return True, None
        except socket.timeout:
            return False, f"Connection timed out. Host '{host}:{port}' did not respond within {timeout_sec}s."
        except ConnectionRefusedError:
            return False, f"Connection refused by '{host}:{port}'. Ensure RTSP service is running on the camera."
        except Exception as e:
            return False, f"Cannot reach '{host}:{port}': {str(e)}"

    def _probe_stream_sync(
        self,
        rtsp_url: str,
        duration_seconds: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Connects, continuously reads frames for 5 seconds, and captures
        the 5th-second stabilized snapshot.
        Runs inside a thread pool to prevent blocking the async event loop.
        """
        timeout_us = int(duration_seconds * 1_000_000)
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            f"rtsp_transport;tcp|stimeout;{timeout_us}|timeout;{timeout_us}"
        )

        cap = cv2.VideoCapture(rtsp_url.strip(), cv2.CAP_FFMPEG)

        try:
            if not cap.isOpened():
                return {
                    "is_valid": False,
                    "frame": None,
                    "resolution": None,
                    "fps": None,
                    "width": None,
                    "height": None,
                    "error": "Failed to open RTSP stream. Check credentials (username/password) or RTSP path.",
                }

            start_time = time.time()
            frames_read = 0
            latest_frame: Optional[np.ndarray] = None

            # Stream continuously across the 5-second interval
            while True:
                ret, frame = cap.read()
                if not ret or frame is None:
                    # If stream drops immediately before reaching 1s
                    if frames_read == 0:
                        return {
                            "is_valid": False,
                            "frame": None,
                            "resolution": None,
                            "fps": None,
                            "width": None,
                            "height": None,
                            "error": "Connected to stream, but failed to decode video frames (empty or corrupt feed).",
                        }
                    break

                frames_read += 1
                latest_frame = frame

                elapsed = time.time() - start_time
                if elapsed >= duration_seconds:
                    break

            if latest_frame is None:
                return {
                    "is_valid": False,
                    "frame": None,
                    "resolution": None,
                    "fps": None,
                    "width": None,
                    "height": None,
                    "error": "Failed to receive continuous video frames over the 5-second duration.",
                }

            # Clone the 5th second stabilized snapshot
            snapshot_5s: np.ndarray = latest_frame.copy()
            height, width = snapshot_5s.shape[:2]
            fps = cap.get(cv2.CAP_PROP_FPS) or 0.0

            if fps <= 0 or fps > 240:
                fps = 30.0

            actual_elapsed = round(time.time() - start_time, 2)

            return {
                "is_valid": True,
                "frame": snapshot_5s,
                "stream_duration_verified": actual_elapsed,
                "total_frames_sampled": frames_read,
                "resolution": f"{width}x{height}",
                "width": int(width),
                "height": int(height),
                "fps": round(float(fps), 2),
                "error": None,
            }

        except Exception as e:
            return {
                "is_valid": False,
                "frame": None,
                "resolution": None,
                "fps": None,
                "width": None,
                "height": None,
                "error": f"Error decoding stream during 5-second check: {str(e)}",
            }
        finally:
            cap.release()

    async def validate_rtsp_stream(
        self,
        rtsp_url: str,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Asynchronously validates the RTSP stream over 5 seconds and captures the 5th-second frame.
        """
        duration = float(timeout_seconds or self.default_duration_seconds)

        # 1. Syntax check
        try:
            scheme, host, port = self.parse_and_validate_url(rtsp_url)
        except ValueError as e:
            return {
                "is_valid": False,
                "frame": None,
                "resolution": None,
                "fps": None,
                "width": None,
                "height": None,
                "error": str(e),
            }

        # 2. Fast Socket Pre-Check (1.5s timeout)
        is_reachable, socket_error = await asyncio.to_thread(
            self._check_socket_reachability, host, port, min(duration, 2.0)
        )
        if not is_reachable:
            return {
                "is_valid": False,
                "frame": None,
                "resolution": None,
                "fps": None,
                "width": None,
                "height": None,
                "error": socket_error,
            }

        # 3. OpenCV 5-Second Stream Probe & 5th-Second Frame Capture
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._probe_stream_sync, rtsp_url, duration),
                timeout=duration + 3.0,
            )
        except asyncio.TimeoutError:
            return {
                "is_valid": False,
                "frame": None,
                "resolution": None,
                "fps": None,
                "width": None,
                "height": None,
                "error": f"RTSP stream reading timed out after {duration} seconds.",
            }
        except Exception as e:
            return {
                "is_valid": False,
                "frame": None,
                "resolution": None,
                "fps": None,
                "width": None,
                "height": None,
                "error": f"Unexpected error during RTSP validation: {str(e)}",
            }


rtsp_validator_service = RTSPValidatorService()
