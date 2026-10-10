from typing import Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field


class WebRTCOffer(BaseModel):
    session_id: UUID
    type: Literal["offer"]
    sdp: str = Field(min_length=1, max_length=65536)


class WebRTCAnswer(BaseModel):
    session_id: UUID
    type: Literal["answer"]
    sdp: str


class ICEServer(BaseModel):
    urls: Union[str, list[str]]
    username: Optional[str] = None
    credential: Optional[str] = None


class WebRTCConfiguration(BaseModel):
    ice_servers: list[ICEServer]
