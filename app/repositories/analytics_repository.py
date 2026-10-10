from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.detection_event import DetectionEvent
from app.models.ppe_event import PPEEvent
from app.schemas.analytics import (
    EventTrendInterval,
    EventTrendSeries,
    EventsTrendResponse,
    EventsTrendTotals,
)

# Realistic fallback trend matching the dashboard screenshot when demo=True
DEMO_PPE = [55, 18, 65, 23, 60, 33, 40, 70, 8, 28, 65, 50]
DEMO_SMOKE = [0, 4, 11, 15, 25, 82, 6, 2, 0, 15, 2, 0]
DEMO_FIRE = [0, 2, 2, 17, 21, 25, 55, 33, 0, 28, 8, 10]


class AnalyticsRepository:
    @staticmethod
    async def get_events_trend(
        db: AsyncSession,
        camera_id: Optional[UUID] = None,
        date_str: Optional[str] = None,
        interval_hours: int = 2,
        demo: bool = False,
    ) -> EventsTrendResponse:
        if interval_hours <= 0 or interval_hours > 24:
            interval_hours = 2

        num_intervals = 24 // interval_hours
        slot_labels = [
            f"{i * interval_hours:02d}:00" for i in range(num_intervals)
        ]

        buckets: Dict[str, Dict[str, int]] = {
            label: {"ppe": 0, "smoke": 0, "fire": 0} for label in slot_labels
        }

        # 1. Determine time window
        start_threshold = None
        end_threshold = None

        if date_str and date_str.strip():
            try:
                parsed_d = date.fromisoformat(date_str.strip()[:10])
                start_threshold = datetime.combine(parsed_d, time.min)
                end_threshold = datetime.combine(parsed_d, time.max)
            except Exception:
                pass

        if start_threshold is None:
            now = datetime.now(timezone.utc)
            start_threshold = now - timedelta(hours=24)
            end_threshold = now + timedelta(hours=24)

        # 2. Query ppe_events
        ppe_query = select(PPEEvent).where(
            PPEEvent.confirmed_at >= start_threshold
        )
        if end_threshold is not None:
            ppe_query = ppe_query.where(PPEEvent.confirmed_at <= end_threshold)
        if camera_id is not None:
            ppe_query = ppe_query.where(PPEEvent.camera_id == camera_id)

        ppe_result = await db.execute(ppe_query)
        ppe_records = ppe_result.scalars().all()

        # If no events found in the rolling 24h window, automatically fallback to latest recorded date in DB
        if not ppe_records and not date_str:
            latest_time = (
                await db.execute(select(func.max(PPEEvent.confirmed_at)))
            ).scalar()
            if latest_time:
                target_d = latest_time.date()
                start_threshold = datetime.combine(target_d, time.min)
                end_threshold = datetime.combine(target_d, time.max)
                fallback_query = select(PPEEvent).where(
                    PPEEvent.confirmed_at >= start_threshold,
                    PPEEvent.confirmed_at <= end_threshold,
                )
                if camera_id is not None:
                    fallback_query = fallback_query.where(
                        PPEEvent.camera_id == camera_id
                    )
                ppe_records = (await db.execute(fallback_query)).scalars().all()

        for ev in ppe_records:
            t = ev.confirmed_at or ev.first_observed_at
            if t is not None:
                slot_idx = (t.hour // interval_hours) % num_intervals
                slot_label = slot_labels[slot_idx]
                etype = (ev.event_type or "").upper()
                vtype = (ev.violation_type or "").upper()

                if "SMOKE" in etype or "SMOKE" in vtype:
                    buckets[slot_label]["smoke"] += 1
                elif "FIRE" in etype or "FIRE" in vtype:
                    buckets[slot_label]["fire"] += 1
                else:
                    buckets[slot_label]["ppe"] += 1

        # 3. Query detection_events
        det_query = select(DetectionEvent).where(
            DetectionEvent.detected_at >= start_threshold
        )
        if end_threshold is not None:
            det_query = det_query.where(DetectionEvent.detected_at <= end_threshold)
        if camera_id is not None:
            det_query = det_query.where(DetectionEvent.camera_id == camera_id)

        det_result = await db.execute(det_query)
        det_records = det_result.scalars().all()

        for det in det_records:
            t = det.detected_at
            if t is not None:
                slot_idx = (t.hour // interval_hours) % num_intervals
                slot_label = slot_labels[slot_idx]
                dtype = (det.detection_type or "").upper()

                if "SMOKE" in dtype:
                    buckets[slot_label]["smoke"] += 1
                elif "FIRE" in dtype:
                    buckets[slot_label]["fire"] += 1
                else:
                    buckets[slot_label]["ppe"] += 1

        # 4. If demo=True, overlay realistic values from screenshot
        if demo:
            for idx, label in enumerate(slot_labels):
                if idx < len(DEMO_PPE):
                    buckets[label]["ppe"] = max(buckets[label]["ppe"], DEMO_PPE[idx])
                    buckets[label]["smoke"] = max(buckets[label]["smoke"], DEMO_SMOKE[idx])
                    buckets[label]["fire"] = max(buckets[label]["fire"], DEMO_FIRE[idx])

        # 5. Assemble intervals and series
        intervals: List[EventTrendInterval] = []
        ppe_series: List[int] = []
        smoke_series: List[int] = []
        fire_series: List[int] = []

        total_ppe = 0
        total_smoke = 0
        total_fire = 0

        for label in slot_labels:
            p = buckets[label]["ppe"]
            s = buckets[label]["smoke"]
            f = buckets[label]["fire"]
            tot = p + s + f

            intervals.append(
                EventTrendInterval(
                    time=label,
                    ppe=p,
                    smoke=s,
                    fire=f,
                    total=tot,
                )
            )
            ppe_series.append(p)
            smoke_series.append(s)
            fire_series.append(f)

            total_ppe += p
            total_smoke += s
            total_fire += f

        series = [
            EventTrendSeries(name="PPE", color="#2563eb", data=ppe_series),
            EventTrendSeries(name="Smoke", color="#f59e0b", data=smoke_series),
            EventTrendSeries(name="Fire", color="#dc2626", data=fire_series),
        ]

        totals = EventsTrendTotals(
            ppe=total_ppe,
            smoke=total_smoke,
            fire=total_fire,
            total=total_ppe + total_smoke + total_fire,
        )

        return EventsTrendResponse(
            timeframe="Last 24 hours",
            intervals=intervals,
            series=series,
            totals=totals,
        )
