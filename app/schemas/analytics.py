from typing import List, Optional
from pydantic import BaseModel, Field


class EventTrendInterval(BaseModel):
    time: str = Field(..., description="Time label (e.g. 00:00, 02:00)")
    ppe: int = Field(0, description="PPE violation count in this interval")
    smoke: int = Field(0, description="Smoke detection count in this interval")
    fire: int = Field(0, description="Fire detection count in this interval")
    total: int = Field(0, description="Total events in this interval")


class EventTrendSeries(BaseModel):
    name: str = Field(..., description="Series label: PPE, Smoke, or Fire")
    color: str = Field(..., description="Hex color code for the series")
    data: List[int] = Field(..., description="List of counts matching interval order")


class EventsTrendTotals(BaseModel):
    ppe: int = Field(0, description="Total PPE events in the timeframe")
    smoke: int = Field(0, description="Total Smoke events in the timeframe")
    fire: int = Field(0, description="Total Fire events in the timeframe")
    total: int = Field(0, description="Grand total events")


class EventsTrendResponse(BaseModel):
    timeframe: str = Field("Last 24 hours", description="Timeframe description")
    intervals: List[EventTrendInterval] = Field(
        ...,
        description="Bucket list of objects (ideal for Recharts data=[...])",
    )
    series: List[EventTrendSeries] = Field(
        ...,
        description="Series array format (ideal for ApexCharts / Chart.js series=[...])",
    )
    totals: EventsTrendTotals


class AnalyticsStats(BaseModel):
    total_events: int = Field(0, description="Total events across all categories")
    total_trend: str = Field("+0%", description="Trend percentage vs previous period")
    ppe_violations: int = Field(0, description="Total PPE violations")
    ppe_trend: str = Field("+0%", description="PPE violations trend percentage")
    smoke_events: int = Field(0, description="Smoke detection events")
    smoke_trend: str = Field("+0%", description="Smoke detection trend percentage")
    fire_events: int = Field(0, description="Fire detection events")
    fire_trend: str = Field("+0%", description="Fire detection trend percentage")


class AnalyticsTrendItem(BaseModel):
    date: str = Field(..., description="Date or hour label (e.g. Sep 25, 14:00)")
    ppe: int = Field(0, description="PPE violation count")
    smoke: int = Field(0, description="Smoke detection count")
    fire: int = Field(0, description="Fire detection count")
    total: int = Field(0, description="Total events in this interval")


class AnalyticsLocationItem(BaseModel):
    location: str = Field(..., description="Location or area name")
    count: int = Field(0, description="Event count in this location")
    percentage: float = Field(0.0, description="Relative percentage share")


class AnalyticsOverviewResponse(BaseModel):
    timeframe: str = Field(..., description="Selected timeframe description")
    stats: AnalyticsStats
    trend_data: List[AnalyticsTrendItem]
    location_data: List[AnalyticsLocationItem]
