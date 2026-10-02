import os
import shutil
import uuid
from pathlib import Path
from typing import Optional, Set
from uuid import UUID

import aiofiles
from fastapi import HTTPException, UploadFile, status


class VideoStorageService:
    """
    Handles asynchronous chunked file streaming, validation, 
    and lifecycle management for uploaded video files.
    """

    ALLOWED_EXTENSIONS: Set[str] = {".mp4", ".avi", ".mov", ".mkv", ".webm"}
    CHUNK_SIZE: int = 1024 * 1024  # 1 MB chunk size for streaming to disk

    def __init__(
        self,
        temp_dir: str = "uploads/temp",
        videos_dir: str = "uploads/videos",
    ):
        self.temp_dir = Path(temp_dir)
        self.videos_dir = Path(videos_dir)
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Ensure both staging and permanent storage directories exist."""
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.videos_dir.mkdir(parents=True, exist_ok=True)

    def validate_file_type(
        self,
        filename: Optional[str],
        content_type: Optional[str],
    ) -> str:
        """
        Validates file extension and MIME type.
        Returns the sanitized lowercased extension (e.g. '.mp4').
        """
        if not filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No filename provided in upload payload.",
            )

        ext = Path(filename).suffix.lower()
        if ext not in self.ALLOWED_EXTENSIONS:
            allowed = ", ".join(self.ALLOWED_EXTENSIONS)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file format '{ext}'. Allowed video formats: {allowed}",
            )

        # Validate Content-Type header if present
        if content_type and not content_type.startswith("video/") and content_type != "application/octet-stream":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid MIME type '{content_type}'. Must be a valid video stream.",
            )

        return ext

    async def save_temp_video(
        self,
        file: UploadFile,
    ) -> tuple[str, str, int]:
        """
        Asynchronously streams uploaded video chunks (1MB) to staging folder.
        Prevents server RAM exhaustion for large video files.

        Returns:
            tuple of (temp_file_id, temp_file_path, total_size_bytes)
        """
        ext = self.validate_file_type(file.filename, file.content_type)
        temp_id = str(uuid.uuid4())
        temp_filename = f"temp_{temp_id}{ext}"
        temp_path = self.temp_dir / temp_filename

        total_bytes = 0

        try:
            async with aiofiles.open(temp_path, "wb") as buffer:
                while chunk := await file.read(self.CHUNK_SIZE):
                    await buffer.write(chunk)
                    total_bytes += len(chunk)

            if total_bytes == 0:
                await self.delete_temp_video(temp_path)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Uploaded video file is empty (0 bytes).",
                )

            return temp_id, str(temp_path.as_posix()), total_bytes

        except HTTPException:
            await self.delete_temp_video(temp_path)
            raise

        except Exception as e:
            await self.delete_temp_video(temp_path)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to stream and save uploaded video: {str(e)}",
            )
        finally:
            await file.close()

    async def delete_temp_video(self, file_path: str | Path) -> bool:
        """Safely removes a temporary file from disk if it exists."""
        try:
            path = Path(file_path)
            if path.exists() and path.is_file():
                path.unlink()
                return True
        except Exception:
            pass
        return False

    async def promote_temp_to_permanent(
        self,
        temp_path: str | Path,
        camera_id: str | UUID,
    ) -> str:
        """
        Moves a validated video from temporary staging to permanent storage:
        uploads/temp/temp_{uuid}.mp4 -> uploads/videos/{camera_id}.mp4
        """
        source = Path(temp_path)
        if not source.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Temporary video file not found for promotion.",
            )

        destination = self.videos_dir / f"{camera_id}{source.suffix.lower()}"
        shutil.move(str(source), str(destination))
        return str(destination.as_posix())


video_storage_service = VideoStorageService()
