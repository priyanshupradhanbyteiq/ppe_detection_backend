"""Cloudinary uploads operate on owned CPU images or staged JPEG files."""

import asyncio
import io

import cloudinary
import cloudinary.uploader
import cv2

from app.core.config import settings


class PPESnapshotService:
    def __init__(self):
        self.enabled = bool(settings.CLOUDINARY_CLOUD_NAME and settings.CLOUDINARY_API_KEY
                            and settings.CLOUDINARY_API_SECRET)
        if self.enabled:
            cloudinary.config(cloud_name=settings.CLOUDINARY_CLOUD_NAME,
                              api_key=settings.CLOUDINARY_API_KEY,
                              api_secret=settings.CLOUDINARY_API_SECRET, secure=True)

    @staticmethod
    def encode(frame):
        success, encoded = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not success:
            raise RuntimeError("Failed to encode PPE snapshot")
        return encoded.tobytes()

    def _upload_sync(self, image, camera_id, event_id):
        if not self.enabled:
            raise RuntimeError("Cloudinary snapshot uploads are not configured")
        result = cloudinary.uploader.upload(
            image, folder=f"ppe_events/{camera_id}", public_id=str(event_id),
            resource_type="image", overwrite=False, unique_filename=False, timeout=15,
        )
        return {"snapshot_url": result["secure_url"],
                "snapshot_public_id": result["public_id"]}

    async def upload_snapshot(self, frame, camera_id, event_id):
        image = await asyncio.to_thread(self.encode, frame)
        return await asyncio.to_thread(self._upload_sync, io.BytesIO(image), camera_id, event_id)

    async def upload_file(self, path, camera_id, event_id):
        return await asyncio.to_thread(self._upload_sync, str(path), camera_id, event_id)
