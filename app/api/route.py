from fastapi import APIRouter
from app.api.v1.auth import router as auth_router
from app.api.v1.cameras import router as cameras_router
from app.api.v1.events import router as events_router
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
    cameras_router,
    prefix="/cameras",
    tags=["Cameras"],
)

api_router.include_router(
    events_router,
    prefix="/events",
    tags=["PPE Events"],
)



