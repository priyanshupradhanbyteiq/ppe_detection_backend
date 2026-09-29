from fastapi import APIRouter, Depends
from app.services.user_service import UserService
from app.schemas.auth import UserRegister, UserLogin,RefreshToken
from app.repositories.user_repository import UserRepository
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.security import create_refresh_token
from app.core.database import get_db
from app.dependecies.auth import get_current_user
router = APIRouter(
    tags=["Auth"]
)

def get_user_service(
    db: AsyncSession = Depends(get_db),
) -> UserService:

    repository = UserRepository(db)

    return UserService(repository)


@router.post("/register")
async def register_user(
    payload: UserRegister,
    service: UserService = Depends(get_user_service),
):
    return await service.create_user(payload)

@router.post("/login")
async def login_user(
    payload: UserLogin,
    service: UserService = Depends(get_user_service),
):
    return await service.login_user(payload)

@router.post("/refresh")
async def refresh_token(
    payload: RefreshToken,
    service: UserService = Depends(get_user_service),
):
    return await service.refresh_token(payload.refresh_token)

@router.post("/me")
async def me(current_user = Depends(get_current_user)):
    return {"message": "This is a protected route. You are authenticated.",
            "user":current_user.email
            }