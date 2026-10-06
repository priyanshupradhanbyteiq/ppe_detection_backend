from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str = "super-secret-key-change-in-production-1234567890"
    ALGORITHM: str = "HS256"
    EXPIRE: int = 60 * 24
    REFRESH_EXPIRE: int = 60 * 24 * 7
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"

    CAMERA_COMMAND_EXCHANGE: str = "camera.commands"
    DEEPSTREAM_WORKER_NAME: str = Field("deepstream-default", min_length=1, max_length=100)
    CAMERA_RECONCILE_SECONDS: float = Field(10, gt=0)
    CAMERA_FRAME_TIMEOUT_SECONDS: float = Field(30, gt=0)
    # Cloudinary Credentials
    CLOUDINARY_CLOUD_NAME: Optional[str] = None
    CLOUDINARY_API_KEY: Optional[str] = None
    CLOUDINARY_API_SECRET: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()