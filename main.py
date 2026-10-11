from fastapi import FastAPI
import logging
from app.core.database import get_db
from app.models import Base
from app.api.route import api_router
from contextlib import asynccontextmanager
from app.core.database import engine
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from app.messaging.rabbitmq import rabbitmq_publisher
from app.core.init_db import ensure_database_exists, seed_default_admin
from app.services.camera_webrtc_service import camera_webrtc_service
from app.services.ppe_notifications import ppe_notification_hub
logger = logging.getLogger(__name__)
@asynccontextmanager
async def lifespan(app: FastAPI):

    try:
        # 1. Ensure target database exists in PostgreSQL
        await ensure_database_exists()

        # 2. Test connection and create missing tables
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

        logger.info(" PostgreSQL connected successfully")

        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

        logger.info(" Database tables verified/created successfully")

        # 3. Seed default Admin user
        await seed_default_admin()
        await rabbitmq_publisher.connect()
    except Exception as e:
        logger.error(f"PostgreSQL initialization failed: {e}")
        logger.exception(
            "RabbitMQ startup connection failed."
        )

    await ppe_notification_hub.start()
    try:
        yield
    finally:
        await ppe_notification_hub.stop()
        await camera_webrtc_service.shutdown()
        await rabbitmq_publisher.close()
        await engine.dispose()


app = FastAPI(lifespan=lifespan)


origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}

app.include_router(api_router, prefix="/api/v1")

