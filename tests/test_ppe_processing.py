"""PPE regressions using CPU metadata doubles, without GPU or live cameras."""

import importlib.util
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from app.deepstream_worker.ppe_association import PPEAssociation, UNTRACKED_ID
from app.deepstream_worker.ppe_decision_engine import PPEEventEngine
from app.deepstream_worker.ppe_processor import MultiCameraPPEProcessor


def detections(person_confidence=0.9):
    return [
        {"class_id": 5, "track_id": 42, "confidence": person_confidence,
         "bbox": (0, 0, 100, 200)},
        {"class_id": 2, "track_id": None, "confidence": 0.9,
         "bbox": (30, 0, 40, 40)},
    ]


class AssociationTests(unittest.TestCase):
    def test_tracker_only_person_anchors_fresh_detector_evidence(self):
        observations = PPEAssociation().associate("camera", 1, detections(-0.10000000149))
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["ppe"]["helmet"],
                         {"state": "MISSING", "confidence": 0.9})

    def test_tracker_only_ppe_is_unknown(self):
        objects = detections(-0.1)
        objects[1]["confidence"] = -0.1
        observation = PPEAssociation().associate("camera", 1, objects)[0]
        self.assertEqual(observation["ppe"]["helmet"]["state"], "UNKNOWN")

    def test_invalid_people_and_disabled_tracker_fallback_are_rejected(self):
        for confidence in (-1, float("nan"), 0.2, 1.1):
            self.assertEqual(PPEAssociation().associate("camera", 1, detections(confidence)), [])
        objects = detections(-0.1)
        objects[0]["track_id"] = UNTRACKED_ID
        self.assertEqual(PPEAssociation().associate("camera", 1, objects), [])
        association = PPEAssociation(allow_tracker_only_persons=False)
        self.assertEqual(association.associate("camera", 1, detections(-0.1)), [])


class ProcessorTests(unittest.TestCase):
    def test_uncommitted_camera_session_does_not_generate_events(self):
        actions = []
        processor = MultiCameraPPEProcessor(action_sink=actions.append)
        processor.register_camera("camera", "session", ready=False)
        for index in range(5):
            processor.submit_frame("camera", "session", index * .25, detections())
        processor.stop()
        self.assertEqual(actions, [])

    def test_utc_creation_times_survive_a_wall_clock_adjustment(self):
        actions = []
        processor = MultiCameraPPEProcessor(action_sink=actions.append)
        processor.register_camera("camera", "session")
        first = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        for index in range(5):
            observed = first + timedelta(seconds=index * .25, minutes=10 if index == 4 else 0)
            processor.submit_frame("camera", "session", 100 + index * .25, detections(), observed_at=observed)
        processor.stop()
        event = actions[0]["event"]
        self.assertEqual(event["first_observed_at"], first)
        self.assertEqual(event["confirmed_at"], first + timedelta(minutes=10, seconds=1))
        self.assertEqual(actions[-1]["event"]["confirmed_at"], event["confirmed_at"])
        self.assertEqual(actions[-1]["event"]["last_observed_at"], event["last_observed_at"])
        self.assertNotIn("_last_observation", actions[-1]["event"])

    def test_shutdown_drains_frames_and_emits_final_event(self):
        for start_worker in (False, True):
            with self.subTest(start_worker=start_worker):
                actions = []
                processor = MultiCameraPPEProcessor(action_sink=actions.append)
                processor.register_camera("camera", "session")
                timestamp = time.monotonic() - 1
                for index in range(5):
                    self.assertTrue(processor.submit_frame("camera", "session", timestamp + index * 0.25,
                                                           detections()))
                if start_worker:
                    processor.start()
                processor.stop()
                self.assertEqual(processor.queue.unfinished_tasks, 0)
                self.assertEqual(processor.sessions, {})
                self.assertFalse(processor.worker and processor.worker.is_alive())
                self.assertFalse(processor.submit_frame("camera", "session", timestamp, []))
                self.assertEqual([action["action"] for action in actions], ["CREATED", "RESOLVED"])
                event = actions[-1]["event"]
                self.assertEqual(event["status"], "RESOLVED")
                self.assertEqual(event["resolution_reason"], "PIPELINE_STOPPED")
                self.assertEqual(event["camera_session_id"], "session")
                self.assertNotEqual(event["session_id"], "session")

    def test_stale_session_frames_do_not_reach_current_engine(self):
        engine = Mock(process=Mock(return_value=[]), close_session=Mock(return_value=[]))
        engine.window_seconds = 1.5
        engine.recovery_window_seconds = 2.0
        processor = MultiCameraPPEProcessor()
        with patch("app.deepstream_worker.ppe_processor.PPEEventEngine", return_value=engine):
            processor.register_camera("camera", "current")
        processor.submit_frame("camera", "old", 1, detections())
        processor.submit_frame("camera", "current", 2, detections())
        processor.stop()
        engine.process.assert_called_once()
        self.assertEqual(engine.process.call_args.args[0]["timestamp"], 2)

    def test_expiration_waits_for_backlog_and_uses_monotonic_time(self):
        engine = Mock(expire_tracks=Mock(return_value=[]))
        processor = MultiCameraPPEProcessor()
        with patch("app.deepstream_worker.ppe_processor.PPEEventEngine", return_value=engine):
            processor.register_camera("camera", "session")
        processor.submit_frame("camera", "session", 1, [])
        processor._expire_tracks()
        engine.expire_tracks.assert_not_called()
        processor.queue.get_nowait()
        processor.queue.task_done()
        with patch("app.deepstream_worker.ppe_processor.time.monotonic", return_value=20):
            processor._expire_tracks()
        engine.expire_tracks.assert_called_once_with(20)

    def test_queue_overflow_and_submission_snapshot(self):
        processor = MultiCameraPPEProcessor(max_queue_size=1)
        objects = detections()
        self.assertTrue(processor.submit_frame("camera", "session", 1, objects))
        objects[0]["confidence"] = 0.1
        objects.clear()
        self.assertFalse(processor.submit_frame("camera", "session", 2, []))
        self.assertEqual(processor.dropped_frames, 1)
        queued = processor.queue.get_nowait()[3]
        processor.queue.task_done()
        self.assertEqual(len(queued), 2)
        self.assertEqual(queued[0]["confidence"], 0.9)


class EngineTests(unittest.TestCase):
    def test_first_observation_precedes_confirmation_and_new_engine_is_empty(self):
        engine = PPEEventEngine(session_id="old")
        for index in range(5):
            observation = PPEAssociation().associate("camera", 100 + index * 0.25, detections())[0]
            engine.process(observation)
        event = engine.events[0]
        self.assertEqual(event["first_observed_at"], 100)
        self.assertEqual(event["confirmed_at"], 101)
        restarted = PPEEventEngine(session_id="new")
        self.assertEqual(restarted.events, [])
        self.assertEqual(restarted.open_events, {})
        self.assertEqual(restarted.history, {})


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load the real pipeline methods with explicit metadata/binding doubles.
        # Restore sys.modules immediately to leave hardware tests independent.
        gi = ModuleType("gi")
        gi.require_version = Mock()
        repository = ModuleType("gi.repository")
        cls.Gst = SimpleNamespace(
            State=SimpleNamespace(PLAYING="PLAYING", NULL="NULL"),
            StateChangeReturn=SimpleNamespace(FAILURE=SimpleNamespace(value_nick="failure")),
            PadLinkReturn=SimpleNamespace(OK="OK"),
            PadProbeReturn=SimpleNamespace(OK="OK"),
        )
        repository.Gst = cls.Gst
        repository.GLib = SimpleNamespace(MainLoop=Mock())
        cls.pyds = SimpleNamespace(
            gst_buffer_get_nvds_batch_meta=Mock(),
            NvDsFrameMeta=SimpleNamespace(cast=lambda value: value),
            NvDsObjectMeta=SimpleNamespace(cast=lambda value: value),
        )
        camera_source = ModuleType("app.deepstream_worker.camera_source")
        camera_source.CameraSourceBin = Mock()
        path = Path(__file__).resolve().parents[1] / "app/deepstream_worker/pipeline.py"
        spec = importlib.util.spec_from_file_location("app.deepstream_worker._ppe_pipeline_tests", path)
        cls.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"gi": gi, "gi.repository": repository,
                                     "pyds": cls.pyds,
                                     "app.deepstream_worker.camera_source": camera_source}):
            spec.loader.exec_module(cls.module)

    def setUp(self):
        self.pipeline = self.module.DynamicPipeline.__new__(self.module.DynamicPipeline)
        self.pipeline.sources = {}
        self.pipeline.free_indexes = set(range(5))
        self.pipeline.pipeline = Mock()
        self.pipeline.streammux = Mock()
        self.pipeline._update_tiler_layout = Mock()
        self.pipeline.ppe_processor = Mock(submit_frame=Mock(return_value=True),
                                           snapshot_requests=Mock(return_value=[]))
        self.pipeline.loop = None
        self.pipeline.loop_thread = None

    @staticmethod
    def linked_list(items):
        node = None
        for item in reversed(items):
            node = SimpleNamespace(data=item, next=node)
        return node

    def test_probe_submits_each_complete_frame_and_advances_filtered_objects(self):
        self.pipeline.sources = {
            "camera": {"source_index": 0, "session_id": "session"},
            "other": {"source_index": 1, "session_id": "other-session"},
        }
        objects = [SimpleNamespace(unique_component_id=99)]
        for detection in detections():
            objects.append(SimpleNamespace(
                unique_component_id=1, class_id=detection["class_id"],
                object_id=detection["track_id"] if detection["track_id"] is not None else UNTRACKED_ID,
                obj_label="object", confidence=detection["confidence"],
                rect_params=SimpleNamespace(left=0, top=0, width=10, height=20)))
        frames = [
            SimpleNamespace(pad_index=0, frame_num=1, obj_meta_list=self.linked_list(objects)),
            SimpleNamespace(pad_index=1, frame_num=2, obj_meta_list=None),
            SimpleNamespace(pad_index=9, frame_num=3, obj_meta_list=self.linked_list(objects)),
        ]
        self.pyds.gst_buffer_get_nvds_batch_meta.return_value = SimpleNamespace(
            frame_meta_list=self.linked_list(frames))
        info = Mock(get_buffer=Mock(return_value=object()))
        with patch.object(self.module.time, "monotonic", return_value=123):
            self.assertEqual(self.pipeline._tracker_probe(None, info), self.Gst.PadProbeReturn.OK)
        calls = self.pipeline.ppe_processor.submit_frame.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].kwargs["camera_id"], "camera")
        self.assertEqual(calls[0].kwargs["session_id"], "session")
        self.assertEqual(calls[0].kwargs["timestamp"], 123)
        self.assertEqual(len(calls[0].kwargs["objects"]), 2)
        self.assertIsNone(calls[0].kwargs["objects"][1]["track_id"])
        self.assertEqual(calls[1].kwargs["objects"], [])

    def test_start_failure_cleans_loop_without_starting_decision_worker(self):
        self.pipeline.pipeline.set_state.return_value = self.Gst.StateChangeReturn.FAILURE
        with self.assertRaisesRegex(RuntimeError, "Failed starting DeepStream"):
            self.pipeline.start()
        self.pipeline.ppe_processor.start.assert_not_called()
        self.pipeline.pipeline.set_state.assert_called_with(self.Gst.State.NULL)
        self.assertIsNone(self.pipeline.loop)
        self.assertIsNone(self.pipeline.loop_thread)

    def test_evidence_copy_survives_surface_reuse_and_jetson_unmapping(self):
        try:
            import numpy as np
            import cv2
        except ImportError as error:
            self.skipTest(f"Image dependencies unavailable: {error}")
        surface = np.zeros((4, 4, 4), dtype=np.uint8)
        surface[:] = (100, 50, 20, 255)
        self.pipeline._is_jetson = True
        request = {"event_id": "confirmed-event"}
        self.pipeline.ppe_processor.snapshot_requests.return_value = [request]
        self.pipeline.ppe_processor.snapshot_sink.return_value = True
        with patch.object(self.pyds, "get_nvds_buf_surface", return_value=surface, create=True), \
             patch.object(self.pyds, "unmap_nvds_buf_surface", create=True) as unmap:
            self.pipeline._capture_ppe_snapshot(object(), SimpleNamespace(batch_id=1), "camera", "session",
                                               100, datetime.now(timezone.utc), detections())
        unmap.assert_called_once()
        frame = self.pipeline.ppe_processor.snapshot_sink.call_args.args[1]
        surface[:] = 0
        self.assertEqual(frame[0, 0].tolist(), [20, 50, 100])
        self.pipeline.ppe_processor.complete_snapshot.assert_called_once_with("confirmed-event")

    def test_successful_start_starts_decision_worker(self):
        self.pipeline.pipeline.set_state.return_value = SimpleNamespace(value_nick="success")
        self.pipeline.start()
        self.pipeline.ppe_processor.start.assert_called_once()
        self.pipeline.loop_thread.join(timeout=1)

    def test_remove_camera_closes_decision_session(self):
        source = Mock()
        self.pipeline.sources["camera"] = dict(source=source, source_index=0, mux_pad=Mock())
        self.pipeline.free_indexes.remove(0)
        self.assertTrue(self.pipeline.remove_camera("camera"))
        self.pipeline.ppe_processor.remove_camera.assert_called_once_with("camera")
        self.assertFalse(self.pipeline.remove_camera("camera"))
        self.assertEqual(self.pipeline.sources, {})
        self.assertIn(0, self.pipeline.free_indexes)

    def test_failed_add_rolls_back_decision_session_and_source(self):
        source = Mock()
        source.bin.get_parent.return_value = self.pipeline.pipeline
        source.bin.get_static_pad.return_value.link.return_value = self.Gst.PadLinkReturn.OK
        source.bin.sync_state_with_parent.return_value = False
        with patch.object(self.module, "CameraSourceBin", return_value=source):
            with self.assertLogs(self.module.__name__, level="ERROR"):
                with self.assertRaisesRegex(RuntimeError, "Could not start camera source"):
                    self.pipeline.add_camera("camera", "rtsp://test", "session", Mock())
        self.pipeline.ppe_processor.remove_camera.assert_called_once_with("camera")
        self.assertEqual(self.pipeline.sources, {})
        self.assertEqual(self.pipeline.free_indexes, set(range(5)))
        self.pipeline.pipeline.remove.assert_called_once_with(source.bin)

    def test_shutdown_stops_stream_then_drains_decisions_before_removing_sources(self):
        self.pipeline.sources["camera"] = {"session_id": "session"}
        self.pipeline._remove_camera_internal = Mock()
        order = []
        self.pipeline.pipeline.set_state.side_effect = lambda state: order.append("stream-stopped")
        self.pipeline.ppe_processor.stop.side_effect = lambda: order.append("decisions-drained")
        self.pipeline._remove_camera_internal.side_effect = lambda camera: order.append("source-removed")
        self.pipeline.shutdown()
        self.assertEqual(order, ["stream-stopped", "decisions-drained", "source-removed"])
        self.assertIsNone(self.pipeline.loop)


if __name__ == "__main__":
    unittest.main()
