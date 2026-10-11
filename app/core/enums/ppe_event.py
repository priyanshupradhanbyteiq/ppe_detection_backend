from enum import Enum


class PPEType(str, Enum):
    HELMET = "helmet"
    MASK = "mask"
    VEST = "vest"


class PPEViolationType(str, Enum):
    NO_HARDHAT = "NO_HARDHAT"
    NO_MASK = "NO_MASK"
    NO_SAFETY_VEST = "NO_SAFETY_VEST"


class PPEEventType(str, Enum):
    PPE_VIOLATION = "PPE_VIOLATION"


class PPEEventStatus(str, Enum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class PPEEventSeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class PPEResolutionReason(str, Enum):
    PPE_COMPLIANCE_RESTORED = "PPE_COMPLIANCE_RESTORED"
    PERSON_LEFT_CAMERA = "PERSON_LEFT_CAMERA"
    STREAM_STOPPED = "STREAM_STOPPED"
    SESSION_REPLACED = "SESSION_REPLACED"
    PIPELINE_STOPPED = "PIPELINE_STOPPED"
    SESSION_INTERRUPTED = "SESSION_INTERRUPTED"
