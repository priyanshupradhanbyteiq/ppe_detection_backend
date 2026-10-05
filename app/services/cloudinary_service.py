import asyncio
import io
import logging
import uuid
from typing import Optional

import cloudinary
import cloudinary.uploader
import cv2
import numpy as np

from app.core.config import settings

logger = logging.getLogger("cloudinary_service")


class CloudinaryService:
    """
    Handles in-memory image compression and asynchronous uploading to Cloudinary
    for 5th-second camera preview thumbnails and AI violation snapshots.
    """

    def __init__(self):
        self._is_configured = False
        self._configure()

    def _configure(self) -> None:
        """Initializes Cloudinary SDK with credentials from application settings."""
        if (
            settings.CLOUDINARY_CLOUD_NAME
            and settings.CLOUDINARY_API_KEY
            and settings.CLOUDINARY_API_SECRET
        ):
            cloudinary.config(
                cloud_name=settings.CLOUDINARY_CLOUD_NAME,
                api_key=settings.CLOUDINARY_API_KEY,
                api_secret=settings.CLOUDINARY_API_SECRET,
                secure=True,
            )
            self._is_configured = True
            logger.info(" Cloudinary SDK initialized successfully.")
        else:
            logger.warning(
                " Cloudinary credentials missing in .env. Frame uploads will be bypassed."
            )

    def _compress_frame_to_jpeg_bytes(
        self,
        frame: np.ndarray,
        quality: int = 90,
    ) -> Optional[bytes]:
        """
        Compresses an OpenCV BGR numpy array frame into in-memory JPEG bytes (Quality: 90).
        """
        if frame is None or not isinstance(frame, np.ndarray) or frame.size == 0:
            return None

        success, encoded_image = cv2.imencode(
            ".jpg",
            frame,
            [int(cv2.IMWRITE_JPEG_QUALITY), quality],
        )
        if not success:
            return None

        return encoded_image.tobytes()

    def _sync_upload(
        self,
        image_bytes: bytes,
        folder: str,
        public_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Synchronously uploads image bytes to Cloudinary.
        Executed inside a threadpool via asyncio.to_thread.
        """
        try:
            upload_params = {
                "folder": folder,
                "resource_type": "image",
            }
            if public_id:
                upload_params["public_id"] = public_id

            response = cloudinary.uploader.upload(
                image_bytes,
                **upload_params,
            )
            return response.get("secure_url")
        except Exception as e:
            print(f" Cloudinary upload error: {e}")
            return None

    async def upload_thumbnail(
        self,
        frame: np.ndarray,
        camera_name: str,
        quality: int = 90,
    ) -> Optional[str]:
        """
        Compresses the 5th-second stabilized video frame to high-quality JPEG (90%) in memory
        and uploads directly to Cloudinary under 'camera_thumbnails/'.

        Returns:
            HTTPS secure URL of the uploaded thumbnail, or None if failed/unconfigured.
        """
        if not self._is_configured:
            # Re-attempt configuration in case credentials were updated in .env
            self._configure()
            if not self._is_configured:
                return None

        jpeg_bytes = self._compress_frame_to_jpeg_bytes(frame, quality=quality)
        if not jpeg_bytes:
            print(" Failed to encode video frame to JPEG.")
            return None

        # Sanitize filename for Cloudinary
        safe_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in camera_name).lower()
        unique_id = f"thumb_{safe_name}_{uuid.uuid4().hex[:8]}"

        return await asyncio.to_thread(
            self._sync_upload,
            jpeg_bytes,
            "camera_thumbnails",
            unique_id,
        )

    # Alias for backward compatibility
    upload_first_frame = upload_thumbnail

    async def upload_violation_snapshot(
        self,
        frame: np.ndarray,
        camera_id: str,
        violation_type: str,
    ) -> Optional[str]:
        """
        Uploads AI safety violation snapshot frame to 'violation_events/' (Quality: 90).
        """
        if not self._is_configured:
            self._configure()
            if not self._is_configured:
                return None

        jpeg_bytes = self._compress_frame_to_jpeg_bytes(frame, quality=90)
        if not jpeg_bytes:
            return None

        unique_id = f"violation_{camera_id}_{violation_type.lower()}_{uuid.uuid4().hex[:8]}"
        return await asyncio.to_thread(
            self._sync_upload,
            jpeg_bytes,
            "violation_events",
            unique_id,
        )


cloudinary_service = CloudinaryService()
