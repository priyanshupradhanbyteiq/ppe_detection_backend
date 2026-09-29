from uuid import UUID

from fastapi import HTTPException, status

from app.repositories.user_repository import UserRepository
from app.schemas.user import UserCreate, UserUpdate
from app.core.security import verify_password,hash_password,create_access_token, create_refresh_token,get_access_token_expiration,decode_token
from app.schemas.auth import Token
class InvalidCredentialsError(ValueError):
    pass

class UserService:

    def __init__(
        self,
        repository: UserRepository,
    ):
        self.repository = repository

    async def create_user(
        self,
        user_data: UserCreate,
    ):
        existing_user = await self.repository.get_by_email(
            user_data.email
        )

        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User with this email already exists",
            )

        user_data.password = hash_password(user_data.password)

        return await self.repository.create(
            user_data
        )

    async def get_user(
        self,
        user_id: UUID,
    ):
        user = await self.repository.get_by_id(
            user_id
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )

        return user

    async def get_users(
        self,
        skip: int = 0,
        limit: int = 20,
    ):
        return await self.repository.get_all(
            skip=skip,
            limit=limit,
        )

    async def update_user(
        self,
        user_id: UUID,
        user_data: UserUpdate,
    ):
        user = await self.repository.get_by_id(
            user_id
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )

        if user_data.email:
            existing_user = await self.repository.get_by_email(
                user_data.email
            )

            if (
                existing_user
                and existing_user.id != user.id
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Email already in use",
                )

        return await self.repository.update(
            user,
            user_data,
        )

    async def delete_user(
        self,
        user_id: UUID,
    ):
        user = await self.repository.get_by_id(
            user_id
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )

        await self.repository.delete(user)

    async def login_user(
        self,
        payload
    ):        
        user = await self.repository.get_by_email(
            payload.email
        )
        if not user:
            raise InvalidCredentialsError("Invalid email or password")

        if not verify_password(payload.password, user.password):
            raise InvalidCredentialsError("Invalid email or password")

        token_data = {"sub": str(user.id), "email": user.email}

        return Token(
            access_token=create_access_token(token_data),
            refresh_token=create_refresh_token(token_data),
            token_type="bearer",
            expires_in=get_access_token_expiration(),
        )

    async def refresh_token(
        self,
        refresh_token: str,
    ):
        try:
            payload = decode_token(refresh_token)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(exc),
            )

        if payload.get("token_type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token",
            )

        email = payload.get("email")
        if not email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )

        user = await self.repository.get_by_email(email)
        if not user or payload.get("sub") != str(user.id):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
            )

        

        token_data = {"sub": str(user.id), "email": user.email}

        return Token(
            access_token=create_access_token(token_data),
            refresh_token=create_refresh_token(token_data),
            token_type="bearer",
            expires_in=get_access_token_expiration(),
        )