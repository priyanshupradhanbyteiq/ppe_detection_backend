from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate


class UserRepository:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(
        self,
        user_data: UserCreate,
    ) -> User:

        user = User(
            name=user_data.name,
            email=user_data.email,
            password=user_data.password,
            phone=user_data.phone,
        )

        self.db.add(user)

        await self.db.commit()
        await self.db.refresh(user)

        return user

    async def get_by_id(
        self,
        user_id: UUID,
    ) -> User | None:

        result = await self.db.execute(
            select(User).where(
                User.id == user_id
            )
        )

        return result.scalar_one_or_none()

    async def get_by_email(
        self,
        email: str,
    ) -> User | None:

        result = await self.db.execute(
            select(User).where(
                User.email == email
            )
        )

        return result.scalar_one_or_none()

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 20,
    ) -> list[User]:

        result = await self.db.execute(
            select(User)
            .offset(skip)
            .limit(limit)
            .order_by(User.created_at.desc())
        )

        return list(result.scalars().all())

    async def update(
        self,
        user: User,
        user_data: UserUpdate,
    ) -> User:

        update_data = user_data.model_dump(
            exclude_unset=True
        )

        for field, value in update_data.items():
            setattr(user, field, value)

        await self.db.commit()
        await self.db.refresh(user)

        return user

    async def delete(
        self,
        user: User,
    ) -> None:

        await self.db.delete(user)
        await self.db.commit()

    