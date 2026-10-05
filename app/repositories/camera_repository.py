from typing import List, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.camera import Camera
from app.schemas.camera import CameraCreateRequest, CameraUpdateRequest


class CameraRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(
        self,
        camera_data: CameraCreateRequest,
        status: str = "ONLINE",
        created_by: Optional[UUID] = None,
    ) -> Camera:
        camera = Camera(
            name=camera_data.name,
            rtsp_url=camera_data.rtsp_url,
            location_name=camera_data.location_name,
            latitude=camera_data.latitude,
            longitude=camera_data.longitude,
            ppe_enabled=camera_data.ppe_enabled,
            fire_enabled=camera_data.fire_enabled,
            smoke_enabled=camera_data.smoke_enabled,
            desired_state=camera_data.desired_state,
            status=status,
            source_type=camera_data.source_type,
            thumbnail_url=camera_data.thumbnail_url,
            created_by=created_by,
        )

        self.db.add(camera)
        await self.db.commit()
        await self.db.refresh(camera)
        return camera

    async def get_by_id(self, camera_id: UUID) -> Optional[Camera]:
        result = await self.db.execute(
            select(Camera).where(Camera.id == camera_id)
        )
        return result.scalar_one_or_none()

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 50,
    ) -> List[Camera]:
        result = await self.db.execute(
            select(Camera).offset(skip).limit(limit)
        )
        return list(result.scalars().all())

    async def update(
        self,
        camera_id: UUID,
        update_data: CameraUpdateRequest,
    ) -> Optional[Camera]:
        camera = await self.get_by_id(camera_id)
        if not camera:
            return None

        update_dict = update_data.model_dump(exclude_unset=True)
        for key, value in update_dict.items():
            setattr(camera, key, value)

        await self.db.commit()
        await self.db.refresh(camera)
        return camera

    async def delete(self, camera_id: UUID) -> bool:
        camera = await self.get_by_id(camera_id)
        if not camera:
            return False

        await self.db.delete(camera)
        await self.db.commit()
        return True
