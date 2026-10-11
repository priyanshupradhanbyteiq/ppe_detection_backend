import json
import logging

import aio_pika


logger = logging.getLogger(__name__)


class CameraCommandConsumer:

    def __init__(
        self,
        rabbitmq_url,
        camera_manager,
        exchange_name="camera.commands",
    ):

        self.rabbitmq_url = rabbitmq_url
        self.camera_manager = camera_manager
        self.exchange_name = exchange_name

        self.connection = None
        self.channel = None
        self.queue = None

    # =====================================================
    # CONNECT
    # =====================================================

    async def connect(self):

        logger.info(
            "Connecting to RabbitMQ..."
        )

        self.connection = (
            await aio_pika.connect_robust(
                self.rabbitmq_url
            )
        )

        self.channel = (
            await self.connection.channel()
        )

        exchange = (
            await self.channel.declare_exchange(
                self.exchange_name,
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
            exchange,
            routing_key="camera.*",
        )

        logger.info(
            "RabbitMQ connected"
        )

    # =====================================================
    # START LISTENING
    # =====================================================

    async def start(self):

        logger.info(
            "Listening for camera commands..."
        )

        await self.queue.consume(
            self.handle_message
        )

    # =====================================================
    # MESSAGE
    # =====================================================

    async def handle_message(
        self,
        message,
    ):

        async with message.process():

            payload = json.loads(
                message.body.decode()
            )

            event_type = payload.get(
                "event_type"
            )

            camera_id = payload.get(
                "camera_id"
            )

            logger.info(
                "Camera command: %s camera=%s",
                event_type,
                camera_id,
            )

            try:

                await self.dispatch(
                    event_type,
                    camera_id,
                )

            except Exception:

                logger.exception(
                    "Camera command failed"
                )

    # =====================================================
    # DISPATCH
    # =====================================================

    async def dispatch(
        self,
        event_type,
        camera_id,
    ):

        if event_type == "camera.start":

            await self.camera_manager.start_camera(
                camera_id
            )

        elif event_type == "camera.stop":

            await self.camera_manager.stop_camera(
                camera_id
            )

        elif event_type == "camera.restart":

            await self.camera_manager.restart_camera(
                camera_id
            )

        elif event_type == "camera.updated":

            await self.camera_manager.update_camera(
                camera_id
            )

        elif event_type == "camera.deleted":

            await self.camera_manager.remove_camera(
                camera_id
            )

        else:

            logger.warning(
                "Unknown event: %s",
                event_type,
            )

    # =====================================================
    # CLOSE
    # =====================================================

    async def close(self):

        if self.connection:

            await self.connection.close()