"""One event shape for REST history and WebSocket delivery."""

from app.models.ppe_event import PPEEvent
from app.schemas.ppe_event import PPEEventResponse


def serialize_event(event: PPEEvent) -> PPEEventResponse:
    return PPEEventResponse(
        id=event.id,
        camera_id=event.camera_id,
        camera_name=event.camera.name if event.camera else None,
        location_name=event.camera.location_name if event.camera else None,
        session_id=event.session_id,
        tracking_session_id=event.tracking_session_id,
        person_track_id=event.person_track_id,
        ppe_type=event.ppe_type,
        violation_type=event.violation_type,
        event_type=event.event_type,
        status=event.status,
        severity=event.severity,
        confidence=event.confidence,
        violation_ratio=event.violation_ratio,
        missing_count=event.missing_count,
        present_count=event.present_count,
        unknown_count=event.unknown_count,
        first_observed_at=event.first_observed_at,
        confirmed_at=event.confirmed_at,
        last_observed_at=event.last_observed_at,
        resolved_at=event.resolved_at,
        resolution_reason=event.resolution_reason,
        snapshot_url=event.snapshot_url,
        snapshot_public_id=event.snapshot_public_id,
        snapshot_captured_at=event.snapshot_captured_at,
        clip_url=event.clip_url,
        created_at=event.created_at,
        updated_at=event.updated_at,
    )

