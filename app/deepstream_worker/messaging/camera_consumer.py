# deepstream_worker/messaging/camera_consumer.py

import asyncio
import json
import logging

import aio_pika


logger = logging.getLogger(__name__)


class CameraCommandConsumer:

    def __init__(
        self,
        rabbitmq_url: str,
        camera_manager,
        exchange_name: str = "camera.commands",
    ):
        self.rabbitmq_url = rabbitmq_url
        self.camera_manager = camera_manager
        self.exchange_name = exchange_name
        self.consumer_tag = None
        self._active_handlers = set()

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
            prefetch_count=1
        )

        self.exchange = (
            await self.channel.declare_exchange(
                self.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        )

        await self.channel.declare_queue("deepstream.camera.commands.failed", durable=True)

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

        self.consumer_tag = await self.queue.consume(
            self.handle_message
        )

        logger.info(
            "Listening for camera commands..."
        )

    async def run(self, stop_event, retry_seconds=5):
        """Broker outages must not stop the worker's DB reconciliation loop."""
        while not stop_event.is_set():
            try:
                await self.start()
                await stop_event.wait()
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("RabbitMQ unavailable; database reconciliation remains active")
                try:
                    await self.close()
                except Exception:
                    logger.exception("Could not close failed RabbitMQ connection")
                self.connection = None
                self.channel = None
                self.exchange = None
                self.queue = None
                self.consumer_tag = None
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=retry_seconds)
                except asyncio.TimeoutError:
                    pass

    async def handle_message(self, message: aio_pika.IncomingMessage):
        task = asyncio.current_task()
        self._active_handlers.add(task)
        try:
            async with message.process(requeue=True):
                try:
                    payload = json.loads(message.body.decode("utf-8"))
                    event_type = payload.get("event_type")
                    camera_id = payload.get("camera_id")
                    if not event_type or not camera_id:
                        raise ValueError("event_type and camera_id are required")
                    await self.dispatch(event_type, camera_id)
                except Exception:
                    logger.exception("Camera command failed; preserving it in the failed queue")
                    await self.channel.default_exchange.publish(
                        aio_pika.Message(body=message.body, content_type="application/json",
                                         delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                         message_id=message.message_id),
                        routing_key="deepstream.camera.commands.failed", mandatory=True,
                    )
                    # DB reconciliation retries runtime failures. If parking the
                    # command fails, the outer context requeues the original.
        finally:
            self._active_handlers.discard(task)

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

        if self.queue and self.consumer_tag:
            await self.queue.cancel(self.consumer_tag)
            self.consumer_tag = None
        if self._active_handlers:
            await asyncio.gather(*self._active_handlers, return_exceptions=True)

        if self.connection:
            await self.connection.close()

        logger.info(
            "RabbitMQ consumer closed."
        )