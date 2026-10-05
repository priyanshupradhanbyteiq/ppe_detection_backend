import json
import logging
from typing import Optional

import aio_pika
from aio_pika import (
    DeliveryMode,
    ExchangeType,
    Message,
    RobustChannel,
    RobustConnection,
    RobustExchange,
)

from app.core.config import settings


logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    """
    Application-level RabbitMQ publisher.

    One connection is created when FastAPI starts and reused
    throughout the lifetime of the application.
    """

    def __init__(self) -> None:
        self.connection: Optional[RobustConnection] = None
        self.channel: Optional[RobustChannel] = None
        self.command_exchange: Optional[RobustExchange] = None

    async def connect(self) -> None:
        """
        Connect to RabbitMQ and declare the command exchange.
        """

        if (
            self.connection is not None
            and not self.connection.is_closed
        ):
            return

        logger.info("Connecting to RabbitMQ...")

        self.connection = await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
        )

        self.channel = await self.connection.channel(
            publisher_confirms=True,
        )

        self.command_exchange = await self.channel.declare_exchange(
            settings.CAMERA_COMMAND_EXCHANGE,
            ExchangeType.TOPIC,
            durable=True,
        )

        logger.info(
            "RabbitMQ connected. exchange=%s",
            settings.CAMERA_COMMAND_EXCHANGE,
        )

    async def publish(
        self,
        routing_key: str,
        payload: dict,
        message_id: str,
    ) -> None:
        """
        Publish a persistent RabbitMQ message.
        """

        if (
            self.connection is None
            or self.connection.is_closed
            or self.channel is None
            or self.channel.is_closed
            or self.command_exchange is None
        ):
            logger.warning(
                "RabbitMQ connection unavailable. Reconnecting..."
            )

            await self.connect()

        body = json.dumps(payload).encode("utf-8")

        message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message_id,
            type=routing_key,
        )

        await self.command_exchange.publish(
            message,
            routing_key=routing_key,
            mandatory=True,
        )

        logger.info(
            "RabbitMQ message published. "
            "routing_key=%s message_id=%s camera_id=%s",
            routing_key,
            message_id,
            payload.get("camera_id"),
        )

    async def close(self) -> None:

        if (
            self.connection is not None
            and not self.connection.is_closed
        ):
            await self.connection.close()

        self.connection = None
        self.channel = None
        self.command_exchange = None

        logger.info("RabbitMQ connection closed.")


rabbitmq_publisher = RabbitMQPublisher()