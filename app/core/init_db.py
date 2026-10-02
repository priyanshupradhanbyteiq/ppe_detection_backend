import logging
from urllib.parse import urlparse, urlunparse

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings
from app.core.database import AsyncSessionLocal, engine
from app.core.security import hash_password
from app.models import Base, User

logger = logging.getLogger("init_db")


async def ensure_database_exists() -> None:
    """
    Connects to the default 'postgres' database and creates the target database 
    (e.g., 'ppe_detection') if it does not already exist.
    """
    try:
        parsed = urlparse(settings.DATABASE_URL)
        target_db = parsed.path.lstrip("/") or "ppe_detection"

        # Construct connection URL pointing to default 'postgres' maintenance DB
        maintenance_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            "/postgres",
            parsed.params,
            parsed.query,
            parsed.fragment,
        ))

        temp_engine = create_async_engine(
            maintenance_url,
            isolation_level="AUTOCOMMIT",
        )

        async with temp_engine.connect() as conn:
            check_query = text("SELECT 1 FROM pg_database WHERE datname = :db_name")
            result = await conn.execute(check_query, {"db_name": target_db})
            exists = result.scalar_one_or_none()

            if not exists:
                print(f" Database '{target_db}' does not exist. Creating it automatically...")
                # CREATE DATABASE cannot run in a transaction block; AUTOCOMMIT isolation is required
                await conn.execute(text(f'CREATE DATABASE "{target_db}"'))
                print(f" Database '{target_db}' created successfully.")

        await temp_engine.dispose()
    except Exception as e:
        print(f" Database pre-check notice (safe to ignore if db already exists): {e}")


async def seed_default_admin() -> None:
    """
    Seeds the default Administrator account if not already present in the database.
    """
    admin_email = "admin@example.com"
    default_password = "Password123"

    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(
                select(User).where(User.email == admin_email)
            )
            existing_admin = result.scalar_one_or_none()

            if not existing_admin:
                print(f" Creating default admin user: {admin_email}...")
                admin_user = User(
                    name="Admin User",
                    email=admin_email,
                    password=hash_password(default_password),
                    phone="+919876543210",
                )
                session.add(admin_user)
                await session.commit()
                print(f" Default admin created successfully ({admin_email} / {default_password})")
            else:
                print(f" Default admin '{admin_email}' is already present.")
        except Exception as e:
            print(f" Failed to seed default admin: {e}")
            await session.rollback()
