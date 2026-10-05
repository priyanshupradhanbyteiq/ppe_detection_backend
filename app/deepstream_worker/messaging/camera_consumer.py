# deepstream_worker/messaging/camera_consumer.py

import json
import logging

import aio_pika


logger = logging.getLogger(__name__)


class CameraCommandConsumer:

    def __init__(
        self,
        rabbitmq_url: str,
        camera_manager,
    ):
        self.rabbitmq_url = rabbitmq_url
        self.camera_manager = camera_manager

        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None

    async def connect(self):

        logger.info(
            "Connecting DeepStream worker to RabbitMQ..."
        )

        self.connection = (
            await aio_pika.connect_robust(
                self.rabbitmq_url
            )
        )

        self.channel = (
            await self.connection.channel()
        )

        # Prevent one worker from taking too many
        # unacknowledged commands.
        await self.channel.set_qos(
            prefetch_count=10
        )

        self.exchange = (
            await self.channel.declare_exchange(
                "camera.commands",
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        )

        self.queue = (
            await self.channel.declare_queue(
                "deepstream.camera.commands",
                durable=True,
            )
        )

        await self.queue.bind(
            self.exchange,
            routing_key="camera.*",
        )

        logger.info(
            "DeepStream RabbitMQ consumer ready."
        )

    async def start(self):

        if self.connection is None:
            await self.connect()

        await self.queue.consume(
            self.handle_message
        )

        logger.info(
            "Listening for camera commands..."
        )

    async def handle_message(
        self,
        message: aio_pika.IncomingMessage,
    ):

        async with message.process(
            requeue=False
        ):

            payload = json.loads(
                message.body.decode("utf-8")
            )

            event_id = payload.get(
                "event_id"
            )

            event_type = payload.get(
                "event_type"
            )

            camera_id = payload.get(
                "camera_id"
            )

            if not event_type:
                raise ValueError(
                    "event_type missing"
                )

            if not camera_id:
                raise ValueError(
                    "camera_id missing"
                )

            logger.info(
                "Camera command received. "
                "event_id=%s "
                "event_type=%s "
                "camera_id=%s",
                event_id,
                event_type,
                camera_id,
            )

            await self.dispatch(
                event_type=event_type,
                camera_id=camera_id,
            )

    async def dispatch(
        self,
        event_type: str,
        camera_id: str,
    ):

        if event_type == "camera.created":

            await self.camera_manager.register_camera(
                camera_id
            )

        elif event_type == "camera.start":

            await self.camera_manager.start_camera(
                camera_id
            )

        elif event_type == "camera.stop":

            await self.camera_manager.stop_camera(
                camera_id
            )

        elif event_type == "camera.updated":

            await self.camera_manager.update_camera(
                camera_id
            )

        elif event_type == "camera.restart":

            await self.camera_manager.restart_camera(
                camera_id
            )

        elif event_type == "camera.deleted":

            await self.camera_manager.remove_camera(
                camera_id
            )

        else:

            logger.warning(
                "Unknown camera event. "
                "event_type=%s",
                event_type,
            )

    async def close(self):

        if self.connection:
            await self.connection.close()

        logger.info(
            "RabbitMQ consumer closed."
        )