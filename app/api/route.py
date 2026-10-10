from fastapi import APIRouter
from app.api.v1.analytics import router as analytics_router
from app.api.v1.auth import router as auth_router
from app.api.v1.cameras import router as cameras_router
from app.api.v1.camera_webrtc import router as camera_webrtc_router
from app.api.v1.events import router as events_router
from app.api.v1.event_notifications import router as event_notifications_router
from app.api.v1.runtime_logs import router as runtime_logs_router
from app.api.v1.sessions import router as sessions_router
from app.api.v1.users import router as users_router

api_router = APIRouter()


api_router.include_router(
    auth_router,
    prefix="/auth",
    tags=["Auth"],
)

api_router.include_router(
    users_router,
    prefix="/users",
    tags=["Users"],
)

api_router.include_router(
    camera_webrtc_router,
    prefix="/cameras",
    tags=["Camera Preview"],
)

api_router.include_router(
    cameras_router,
    prefix="/cameras",
    tags=["Cameras"],
)

api_router.include_router(
    event_notifications_router,
    prefix="/events",
    tags=["PPE Notifications"],
)

api_router.include_router(
    events_router,
    prefix="/events",
    tags=["PPE Events"],
)

api_router.include_router(
    runtime_logs_router,
    prefix="/runtime-logs",
    tags=["Runtime Logs"],
)

api_router.include_router(
    analytics_router,
    prefix="/analytics",
    tags=["Analytics"],
)

api_router.include_router(
    sessions_router,
    prefix="/sessions",
    tags=["Camera Sessions"],
)
