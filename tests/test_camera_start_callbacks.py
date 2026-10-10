"""Exercise the current manager interface without live cameras or services."""

import asyncio
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from app.deepstream_worker.camera_manager import CameraManager
from app.models import Base, Camera, CameraSession, DeepStreamWorker
from app.repositories.camera_runtime_repository import CameraRuntimeRepository
from test_camera_lifecycle import AsyncSQLiteSession, FakePipeline


class CameraStartCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://")

        @event.listens_for(self.engine, "connect")
        def foreign_keys(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(self.engine)
        self.repository = CameraRuntimeRepository(lambda: AsyncSQLiteSession(self.engine))
        self.worker_id = await self.repository.get_or_create_worker("callback-test")
        self.camera_id = uuid4()
        with Session(self.engine) as db, db.begin():
            db.add(Camera(id=self.camera_id, name="Test", rtsp_url="rtsp://localhost/test",
                          desired_state="RUNNING", source_type="RTSP_STREAM"))
        self.pipeline = FakePipeline()
        self.manager = CameraManager(self.repository, self.pipeline, self.worker_id)

    async def asyncTearDown(self):
        await self.manager.shutdown()
        self.engine.dispose()

    def read_one(self, model):
        with Session(self.engine) as db:
            return db.scalars(select(model)).one()

    async def drain_callbacks(self):
        # The thread enqueues a callback, which then creates an asyncio task.
        await asyncio.sleep(0)
        if self.manager._callback_tasks:
            await asyncio.gather(*self.manager._callback_tasks)

    async def test_start_shares_session_id_with_pipeline_and_database(self):
        runtime = await self.manager.start_camera(self.camera_id)
        source = self.pipeline.sources[str(self.camera_id)]
        self.assertEqual(source["session_id"], runtime["session_id"])
        self.assertEqual(str(self.read_one(CameraSession).id), runtime["session_id"])
        self.assertTrue(callable(source["on_frame"]))
        self.assertEqual(self.read_one(Camera).status, "CONNECTING")
        await self.manager.stop_camera(self.camera_id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.read_one(CameraSession).status, "STOPPED")
        self.assertEqual(self.read_one(Camera).status, "OFFLINE")
        self.assertEqual(self.read_one(DeepStreamWorker).current_cameras, 0)

    async def test_concurrent_start_creates_one_session(self):
        await asyncio.gather(*(self.manager.start_camera(self.camera_id) for _ in range(5)))
        self.assertEqual(len(self.pipeline.sources), 1)
        self.assertEqual(self.read_one(DeepStreamWorker).current_cameras, 1)
        self.read_one(CameraSession)

    async def test_source_probe_forwards_real_buffers_and_persists_online_status(self):
        try:
            from app.deepstream_worker.camera_source import CameraSourceBin, Gst
        except (ImportError, ValueError) as exc:
            self.skipTest(f"GStreamer bindings unavailable: {exc}")

        runtime = await self.manager.start_camera(self.camera_id)
        # Feed CPU buffers through the actual PyGObject pad-probe bridge. GPU
        # decoding and a live RTSP camera are unnecessary to verify the callback.
        source = CameraSourceBin.__new__(CameraSourceBin)
        source.camera_id = str(self.camera_id)
        source.frame_count = 0
        source.last_frame_arrival = None
        source.on_frame = lambda: self.manager.notify_frame(source.camera_id, runtime["session_id"])
        returns = []

        def observe_return(pad, info):
            result = source._on_frame(pad, info)
            returns.append(result)
            return result

        Gst.init(None)
        stream = Gst.parse_launch("fakesrc name=frames num-buffers=3 ! fakesink")
        stream.get_by_name("frames").get_static_pad("src").add_probe(
            Gst.PadProbeType.BUFFER, observe_return
        )
        try:
            self.assertNotEqual(stream.set_state(Gst.State.PLAYING), Gst.StateChangeReturn.FAILURE)
            message = stream.get_bus().timed_pop_filtered(
                5 * Gst.SECOND, Gst.MessageType.EOS | Gst.MessageType.ERROR
            )
            self.assertIsNotNone(message)
            self.assertEqual(message.type, Gst.MessageType.EOS)
        finally:
            stream.set_state(Gst.State.NULL)
        await self.drain_callbacks()
        self.assertEqual(returns, [Gst.PadProbeReturn.OK] * 3)
        self.assertEqual(source.frame_count, 3)
        self.assertIsNotNone(source.last_frame_arrival)
        self.assertEqual(self.read_one(CameraSession).frames_processed, 3)
        self.assertEqual(self.read_one(Camera).status, "ONLINE")

    async def test_database_failure_rolls_back_and_removes_source(self):
        with patch.object(self.repository, "_transition", AsyncMock(side_effect=RuntimeError("DB failed"))):
            with self.assertRaisesRegex(RuntimeError, "DB failed"):
                await self.manager.start_camera(self.camera_id)
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.manager.cameras, {})
        with Session(self.engine) as db:
            self.assertEqual(db.scalars(select(CameraSession)).all(), [])
        self.assertEqual(self.read_one(DeepStreamWorker).current_cameras, 0)

    async def test_frame_callback_from_streaming_thread_marks_camera_online(self):
        runtime = await self.manager.start_camera(self.camera_id)
        source = self.pipeline.sources[str(self.camera_id)]
        thread = threading.Thread(target=source["on_frame"], args=(str(self.camera_id), runtime["session_id"]))
        thread.start()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        await self.drain_callbacks()
        self.assertEqual(self.read_one(CameraSession).frames_processed, 1)
        self.assertEqual(self.read_one(Camera).status, "ONLINE")
        self.assertIsNotNone(self.read_one(Camera).last_frame_at)

    async def test_frame_burst_is_throttled_and_stop_persists_final_count(self):
        runtime = await self.manager.start_camera(self.camera_id)
        original = self.repository.record_frames
        with patch.object(self.repository, "record_frames", AsyncMock(wraps=original)) as record:
            for _ in range(30):
                self.manager.notify_frame(str(self.camera_id), runtime["session_id"])
            await self.drain_callbacks()
            self.assertEqual(record.await_count, 1)
            self.assertEqual(self.read_one(CameraSession).frames_processed, 30)
            self.manager.notify_frame(str(self.camera_id), runtime["session_id"])
            await self.drain_callbacks()
            self.assertEqual(record.await_count, 1)
            await self.manager.stop_camera(self.camera_id)
            self.assertEqual(self.read_one(CameraSession).frames_processed, 31)

    async def test_restart_ignores_old_session_frame_and_error_callbacks(self):
        old_runtime = await self.manager.start_camera(self.camera_id)
        runtime = await self.manager.restart_camera(self.camera_id)
        self.assertNotEqual(runtime["session_id"], old_runtime["session_id"])
        self.manager.notify_frame(str(self.camera_id), old_runtime["session_id"])
        self.pipeline.on_error(str(self.camera_id), old_runtime["session_id"], "late error")
        await self.drain_callbacks()
        self.assertEqual(runtime["frames_processed"], 0)
        self.assertIn(str(self.camera_id), self.pipeline.sources)

    async def test_source_error_closes_matching_session(self):
        runtime = await self.manager.start_camera(self.camera_id)
        with self.assertLogs("app.deepstream_worker.camera_manager", level="ERROR"):
            self.pipeline.on_error(str(self.camera_id), runtime["session_id"], "RTSP disconnected")
            await self.drain_callbacks()
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.manager.cameras, {})
        self.assertEqual(self.read_one(CameraSession).status, "ERROR")
        self.assertEqual(self.read_one(CameraSession).error_message, "RTSP disconnected")
        self.assertEqual(self.read_one(Camera).status, "ERROR")
        self.assertEqual(self.read_one(DeepStreamWorker).current_cameras, 0)

    async def test_shutdown_stops_shared_pipeline_before_closing_camera_sessions(self):
        await self.manager.start_camera(self.camera_id)
        remove_camera = self.pipeline.remove_camera
        with patch.object(self.pipeline, "shutdown", wraps=self.pipeline.shutdown) as shutdown:
            def remove_after_shutdown(camera_id):
                self.assertTrue(shutdown.called, "Live source removed before pipeline shutdown")
                return remove_camera(camera_id)

            with patch.object(self.pipeline, "remove_camera", side_effect=remove_after_shutdown):
                await self.manager.shutdown()
        self.assertEqual(self.manager.cameras, {})
        self.assertEqual(self.read_one(CameraSession).status, "STOPPED")
        self.assertEqual(self.read_one(DeepStreamWorker).current_cameras, 0)


class PipelineCallbackTests(unittest.TestCase):
    def setUp(self):
        try:
            from app.deepstream_worker.pipeline import DynamicPipeline, Gst
        except (ImportError, ValueError) as exc:
            self.skipTest(f"GStreamer bindings unavailable: {exc}")
        self.Gst = Gst
        # Exercise source wiring without creating GPU elements or a live stream.
        self.pipeline = DynamicPipeline.__new__(DynamicPipeline)
        self.pipeline.sources = {}
        self.pipeline.free_indexes = set(range(5))
        self.pipeline.pipeline = Mock()
        self.pipeline.pipeline.add.return_value = None
        self.pipeline.streammux = Mock()
        self.pipeline.on_error = Mock()
        self.pipeline._update_tiler_layout = Mock()
        self.pipeline.ppe_processor = Mock()
        self.source = Mock()
        self.source.bin.get_static_pad.return_value.link.return_value = Gst.PadLinkReturn.OK
        self.source.bin.sync_state_with_parent.return_value = True
        self.source.bin.get_name.return_value = "camera-source-0"
        self.source.bin.get_parent.return_value = self.pipeline.pipeline

    def test_source_callbacks_keep_camera_and_session_identity(self):
        on_frame = Mock()
        with patch("app.deepstream_worker.pipeline.CameraSourceBin", return_value=self.source) as source:
            index = self.pipeline.add_camera("camera", "rtsp://localhost/test", "session", on_frame)
        self.assertEqual(index, 0)
        self.assertEqual(self.pipeline.sources["camera"]["session_id"], "session")
        source.call_args.kwargs["on_frame"]()
        on_frame.assert_called_once_with("camera", "session")
        source.call_args.kwargs["on_error"]("Unsupported codec")
        self.pipeline.on_error.assert_called_once_with("camera", "session", "Unsupported codec")

    def test_source_attaches_to_real_gstreamer_pipeline(self):
        # Use the actual PyGObject override, whose successful add returns None.
        self.Gst.init(None)
        self.pipeline.pipeline = self.Gst.Pipeline.new("attachment-test")
        self.source.bin = self.Gst.Bin.new("camera-source-0")
        output_pad = self.Gst.Pad.new("src", self.Gst.PadDirection.SRC)
        self.source.bin.add_pad(output_pad)
        mux_pad = self.Gst.Pad.new("sink_0", self.Gst.PadDirection.SINK)
        self.pipeline.streammux.request_pad_simple.return_value = mux_pad

        with patch("app.deepstream_worker.pipeline.CameraSourceBin", return_value=self.source):
            index = self.pipeline.add_camera("camera", "rtsp://localhost/test", "session", Mock())

        self.assertEqual(index, 0)
        self.assertEqual(self.source.bin.get_parent(), self.pipeline.pipeline)
        self.assertEqual(output_pad.get_peer(), mux_pad)
        self.assertNotIn(index, self.pipeline.free_indexes)

        self.assertTrue(self.pipeline.remove_camera("camera"))
        self.assertIsNone(self.source.bin.get_parent())
        self.assertFalse(output_pad.is_linked())
        self.assertIn(index, self.pipeline.free_indexes)
        self.assertEqual(self.pipeline.sources, {})
        self.pipeline.streammux.release_request_pad.assert_called_once_with(mux_pad)

    def test_failed_source_link_removes_bin_and_releases_slot(self):
        self.source.bin.get_static_pad.return_value.link.return_value = self.Gst.PadLinkReturn.NOFORMAT
        with patch("app.deepstream_worker.pipeline.CameraSourceBin", return_value=self.source):
            with self.assertLogs("app.deepstream_worker.pipeline", level="ERROR"):
                with self.assertRaisesRegex(RuntimeError, "Could not link camera"):
                    self.pipeline.add_camera("camera", "rtsp://localhost/test", "session", Mock())
        self.pipeline.pipeline.remove.assert_called_once_with(self.source.bin)
        self.pipeline.streammux.release_request_pad.assert_called_once()
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.pipeline.free_indexes, set(range(5)))

    def test_bus_routes_source_errors_and_ignores_removed_source_errors(self):
        self.pipeline.sources = {
            "camera": {"source": self.source, "session_id": "session"},
            "other": {"source": Mock(), "session_id": "other-session"},
        }
        child = Mock()
        child.get_name.return_value = "decoder-0"
        child.get_parent.return_value = self.source.bin
        message = SimpleNamespace(type=self.Gst.MessageType.ERROR, src=child,
                                  parse_error=lambda: ("RTSP disconnected", None))
        with self.assertLogs("app.deepstream_worker.pipeline", level="ERROR"):
            self.pipeline._on_bus_message(None, message)
        self.pipeline.on_error.assert_called_once_with("camera", "session", "RTSP disconnected")
        self.pipeline.sources.pop("camera")
        self.source.bin.get_parent.return_value = None
        self.pipeline.on_error.reset_mock()
        with self.assertLogs("app.deepstream_worker.pipeline", level="ERROR"):
            self.pipeline._on_bus_message(None, message)
        self.pipeline.on_error.assert_not_called()


if __name__ == "__main__":
    unittest.main()
