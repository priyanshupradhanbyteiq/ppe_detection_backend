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
    PPE_OUTBOX_PATH: str = "data/ppe_outbox.sqlite3"
    WEBRTC_ICE_SERVERS: list[dict] = Field(default_factory=list)
    WEBRTC_MAX_PEERS: int = Field(20, ge=1, le=200)
    WEBRTC_MAX_FPS: int = Field(20, ge=1, le=60)
    WEBRTC_MAX_WIDTH: int = Field(1280, ge=160, le=3840)
    WEBRTC_OPEN_TIMEOUT_SECONDS: float = Field(5, ge=1, le=30)
    WEBRTC_READ_TIMEOUT_SECONDS: float = Field(2, ge=0.5, le=10)
    WEBRTC_FIRST_FRAME_TIMEOUT_SECONDS: float = Field(10, ge=1, le=60)
    WEBRTC_PEER_IDLE_SECONDS: float = Field(45, ge=15, le=300)
    WEBSOCKET_ALLOWED_ORIGINS: list[str] = Field(default_factory=lambda: [
        "http://localhost:5173", "http://localhost:5174", "http://127.0.0.1:5173",
        "http://localhost:3000",
    ])
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
