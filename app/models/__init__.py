from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


from app.models.user import User  # noqa: E402
from app.models.camera import Camera  # noqa: E402
from app.models.detection_event import DetectionEvent  # noqa: E402
from app.models.camera_status_log import CameraStatusLog  # noqa: E402
from app.models.deepstream_worker import DeepStreamWorker  # noqa: E402
from app.models.camera_session import CameraSession  # noqa: E402
from app.models.camera_runtime_log import CameraRuntimeLog  # noqa: E402

__all__ = [
    "Base", "User", "Camera", "DetectionEvent", "CameraStatusLog",
    "DeepStreamWorker", "CameraSession", "CameraRuntimeLog",
]
