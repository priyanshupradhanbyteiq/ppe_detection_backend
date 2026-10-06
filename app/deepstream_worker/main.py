"""Run with: python -m app.deepstream_worker.main"""

import asyncio
import logging
import signal
import socket
import time

from sqlalchemy import text

from app.core.config import settings
from app.core.database import AsyncSessionLocal, engine
from app.deepstream_worker.camera_manager import CameraManager
from app.deepstream_worker.messaging.camera_consumer import CameraCommandConsumer
from app.deepstream_worker.pipeline import DynamicPipeline
from app.repositories.camera_runtime_repository import CameraRuntimeRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)
# This deployment supports one worker/default pipeline. Keep ownership on a
# dedicated PostgreSQL connection for the worker's entire lifetime.
WORKER_LOCK_ID = 734292201


async def maintenance(manager, ownership, stop_event):
    next_reconcile = 0
    while not stop_event.is_set():
        # Losing the lock connection must terminate this worker immediately.
        await ownership.execute(text("SELECT 1"))
        await ownership.commit()
        try:
            await manager.flush_frames()
            if time.monotonic() >= next_reconcile:
                await manager.reconcile_all()
                next_reconcile = time.monotonic() + settings.CAMERA_RECONCILE_SECONDS
        except Exception:
            logger.exception("Runtime maintenance failed; retrying on the next tick")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass


async def run_worker(ownership):
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    pipeline = DynamicPipeline()
    repository = CameraRuntimeRepository(AsyncSessionLocal)
    manager = None
    consumer = None
    consumer_task = None
    try:
        pipeline.start()
        worker_id = await repository.initialize_worker(
            settings.DEEPSTREAM_WORKER_NAME, socket.gethostname(), pipeline.MAX_CAMERAS,
        )
        manager = CameraManager(repository, pipeline, worker_id,
                                frame_timeout=settings.CAMERA_FRAME_TIMEOUT_SECONDS)
        consumer = CameraCommandConsumer(settings.RABBITMQ_URL, manager,
                                         exchange_name=settings.CAMERA_COMMAND_EXCHANGE)
        await manager.reconcile_all()
        consumer_task = asyncio.create_task(consumer.run(stop_event))
        logger.info("DeepStream worker ready: worker=%s", worker_id)
        await maintenance(manager, ownership, stop_event)
    finally:
        stop_event.set()
        if consumer_task:
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
        try:
            if consumer:
                await consumer.close()
        finally:
            if manager:
                await manager.shutdown()
            else:
                pipeline.shutdown()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


async def main():
    try:
        async with engine.connect() as ownership:
            acquired = (await ownership.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": WORKER_LOCK_ID},
            )).scalar()
            await ownership.commit()
            if not acquired:
                raise RuntimeError("Another DeepStream worker already owns this deployment")
            try:
                await run_worker(ownership)
            finally:
                if not ownership.invalidated:
                    await ownership.execute(text("SELECT pg_advisory_unlock(:lock_id)"),
                                            {"lock_id": WORKER_LOCK_ID})
                    await ownership.commit()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
