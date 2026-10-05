# deepstream_worker/main.py

import asyncio
import logging
import os
import signal

from camera_manager import CameraManager

from messaging.camera_consumer import (
    CameraCommandConsumer,
)


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s - "
        "%(message)s"
    ),
)


logger = logging.getLogger(__name__)


RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/",
)


async def main():

    camera_manager = CameraManager()

    consumer = CameraCommandConsumer(
        rabbitmq_url=RABBITMQ_URL,
        camera_manager=camera_manager,
    )

    await consumer.connect()

    await consumer.start()

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):

        loop.add_signal_handler(
            sig,
            stop_event.set,
        )

    logger.info(
        "DeepStream camera worker started."
    )

    await stop_event.wait()

    logger.info(
        "DeepStream camera worker shutting down..."
    )

    await consumer.close()


if __name__ == "__main__":

    asyncio.run(
        main()
    )