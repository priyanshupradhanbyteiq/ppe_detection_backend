import asyncio
import logging
import signal
import socket
from app.core.config import settings
from app.core.database import AsyncSessionLocal, engine
from app.deepstream_worker.pipeline import DynamicPipeline
from app.deepstream_worker.camera_manager import CameraManager
from app.deepstream_worker.messaging.camera_consumer import CameraCommandConsumer
from app.deepstream_worker.worker_lock import worker_lock
from app.repositories.camera_runtime_repository import CameraRuntimeRepository
from app.services.ppe_event_persistence import PPEEventPersistence

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


async def main():
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    try:
        # Acquire ownership before creating a pipeline or recovering sessions.
        async with worker_lock(engine):
            await _run_worker(CameraRuntimeRepository(AsyncSessionLocal), stop_event)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await engine.dispose()


async def _run_worker(repository, stop_event):
    pipeline = DynamicPipeline()
    manager = None
    consumer = None
    worker_id = None
    persistence = None
    try:
        pipeline.start()
        worker_id = await repository.initialize_worker(
            name=settings.DEEPSTREAM_WORKER_NAME,
            hostname=socket.gethostname(),
            max_cameras=pipeline.MAX_CAMERAS,
        )
        persistence = PPEEventPersistence(AsyncSessionLocal, outbox_path=settings.PPE_OUTBOX_PATH)
        await persistence.start(worker_id)
        pipeline.set_ppe_persistence(persistence)
        manager = CameraManager(repository, pipeline, worker_id)
        consumer = CameraCommandConsumer(
            rabbitmq_url=settings.RABBITMQ_URL,
            camera_manager=manager,
            exchange_name=settings.CAMERA_COMMAND_EXCHANGE,
        )
        await consumer.connect()
        await manager.restore_cameras()
        await consumer.start()
        logger.info("DeepStream worker ready")
        await stop_event.wait()
    finally:
        logger.info("Stopping DeepStream worker...")
        try:
            if consumer is not None:
                await consumer.close()
        finally:
            try:
                if manager is not None:
                    await manager.shutdown()
                else:
                    pipeline.shutdown()
            finally:
                try:
                    # Decision shutdown emits final RESOLVED actions first.
                    if persistence is not None:
                        await persistence.stop()
                finally:
                    if worker_id is not None:
                        await repository.heartbeat(
                            worker_id,
                            current_cameras=len(manager.cameras) if manager is not None else 0,
                            offline=True,
                        )


if __name__ == "__main__":
    asyncio.run(main())
