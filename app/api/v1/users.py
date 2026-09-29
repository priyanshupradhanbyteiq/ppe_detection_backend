from uuid import UUID
from fastapi import (
    APIRouter,
    Depends,
    Query,
    Response,
    status,
)
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.repositories.user_repository import UserRepository
from app.schemas.user import (
    UserResponse,
    UserUpdate,
)
from app.services.user_service import UserService


router = APIRouter(
    
    tags=["Users"],
)


def get_user_service(
    db: AsyncSession = Depends(get_db),
) -> UserService:

    repository = UserRepository(db)

    return UserService(repository)


@router.get(
    "",
    response_model=list[UserResponse],
)
async def get_users(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    service: UserService = Depends(get_user_service),
):
    return await service.get_users(
        skip=skip,
        limit=limit,
    )


@router.get(
    "/{user_id}",
    response_model=UserResponse,
)
async def get_user(
    user_id: UUID,
    service: UserService = Depends(get_user_service),
):
    return await service.get_user(user_id)


@router.patch(
    "/{user_id}",
    response_model=UserResponse,
)
async def update_user(
    user_id: UUID,
    payload: UserUpdate,
    service: UserService = Depends(get_user_service),
):
    return await service.update_user(
        user_id,
        payload,
    )


@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_user(
    user_id: UUID,
    service: UserService = Depends(get_user_service),
):
    await service.delete_user(user_id)

    return Response(
        status_code=status.HTTP_204_NO_CONTENT
    )