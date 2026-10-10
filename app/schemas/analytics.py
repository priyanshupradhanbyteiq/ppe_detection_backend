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
