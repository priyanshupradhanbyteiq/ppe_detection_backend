import asyncio
import logging
from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.deepstream_worker.pipeline import DynamicPipeline
from app.deepstream_worker.camera_manager import CameraManager
from app.deepstream_worker.messaging.camera_consumer import CameraCommandConsumer
from app.repositories.camera_runtime_repository import CameraRuntimeRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


async def main():

    # -----------------------------------
    # 1. Create pipeline
    # -----------------------------------

    pipeline = DynamicPipeline()

    pipeline.start()

    # -----------------------------------
    # 2. Database repository
    # -----------------------------------

    repository = CameraRuntimeRepository(
        AsyncSessionLocal
    )

    # -----------------------------------
    # 3. Create worker
    # -----------------------------------

    worker_id = await repository.get_or_create_worker(
        name=settings.DEEPSTREAM_WORKER_NAME,
        max_cameras=5,
    )

    # -----------------------------------
    # 4. Camera manager
    # -----------------------------------

    manager = CameraManager(
        repository=repository,
        pipeline=pipeline,
        worker_id=worker_id,
    )

    # -----------------------------------
    # 5. RabbitMQ
    # -----------------------------------

    consumer = CameraCommandConsumer(
        rabbitmq_url=settings.RABBITMQ_URL,
        camera_manager=manager,
        exchange_name=settings.CAMERA_COMMAND_EXCHANGE,
    )

    await consumer.connect()

    logger.info(
        "DeepStream worker ready"
    )

    try:

        await consumer.start()

        # Keep worker alive
        await asyncio.Event().wait()

    except KeyboardInterrupt:

        logger.info(
            "Stopping DeepStream worker..."
        )

    finally:

        await consumer.close()

        await manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())