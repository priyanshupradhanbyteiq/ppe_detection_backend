"""Browser notification channel authenticated by the first socket message."""

import asyncio
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.security import decode_token
from app.repositories.user_repository import UserRepository
from app.services.ppe_notifications import ppe_notification_hub

router = APIRouter()


async def authenticate_event_socket(token):
    if not isinstance(token, str) or not token or len(token) > 4096:
        raise ValueError("Missing access token")
    payload = decode_token(token)
    if payload.get("token_type") != "access" or not payload.get("email"):
        raise ValueError("Invalid access token")
    async with AsyncSessionLocal() as db:
        user = await UserRepository(db).get_by_email(payload["email"])
        if user is None or payload.get("sub") != str(user.id):
            raise ValueError("User not found")
    if not isinstance(payload.get("exp"), (int, float)) or payload["exp"] <= time.time():
        raise ValueError("Expired access token")
    return payload


def get_notification_hub():
    return ppe_notification_hub


@router.websocket("/ws")
async def event_notifications(socket: WebSocket):
    origin = socket.headers.get("origin")
    if origin and origin not in settings.WEBSOCKET_ALLOWED_ORIGINS:
        await socket.close(code=4403)
        return
    await socket.accept()
    queue = None
    sender = None
    receiver = None
    hub = get_notification_hub()
    try:
        try:
            authentication = await asyncio.wait_for(socket.receive_json(), 10)
            if not isinstance(authentication, dict) or authentication.get("type") != "auth":
                raise ValueError("Authentication must be the first message")
            claims = await authenticate_event_socket(authentication.get("token"))
        except (ValueError, asyncio.TimeoutError):
            await socket.close(code=4401)
            return
        try:
            queue = await hub.subscribe()
        except asyncio.TimeoutError:
            await socket.close(code=1013, reason="Event notification service is reconnecting")
            return
        await socket.send_json({"type": "ready"})

        async def send_notifications():
            while True:
                await asyncio.wait_for(socket.send_json(await queue.get()), 10)

        async def receive_heartbeats():
            while True:
                remaining = claims["exp"] - time.time()
                if remaining <= 0:
                    await socket.close(code=4401)
                    return
                try:
                    message = await asyncio.wait_for(socket.receive_json(), min(remaining, 40))
                except asyncio.TimeoutError:
                    await socket.close(code=4401 if time.time() >= claims["exp"] else 1001)
                    return
                if isinstance(message, dict) and message.get("type") == "ping" and not queue.full():
                    queue.put_nowait({"type": "pong"})

        sender = asyncio.create_task(send_notifications())
        receiver = asyncio.create_task(receive_heartbeats())
        _, pending = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(sender, receiver, return_exceptions=True)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        if queue is not None:
            hub.unsubscribe(queue)
        tasks = [task for task in (sender, receiver) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await socket.close()
        except RuntimeError:
            pass
