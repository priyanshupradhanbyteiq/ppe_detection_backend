import logging
import threading

import gi

gi.require_version("Gst", "1.0")

from gi.repository import Gst, GLib

from .camera_source import CameraSourceBin


logger = logging.getLogger(__name__)


class DynamicPipeline:

    PIPELINE_ID = "default-pipeline"
    MAX_CAMERAS = 5

    DISPLAY_WIDTH = 1280
    DISPLAY_HEIGHT = 720

    def __init__(self):
        Gst.init(None)

        self.pipeline_id = self.PIPELINE_ID
        self.sources = {}
        self.free_indexes = set(range(self.MAX_CAMERAS))

        self.on_error = None

        self.loop = None
        self.loop_thread = None

        self._mux_buffer_count = 0
        self._display_buffer_count = 0

        self.pipeline = Gst.Pipeline.new(self.pipeline_id)

        if self.pipeline is None:
            raise RuntimeError("Could not create GStreamer pipeline")

        # --------------------------------------------------
        # Create permanent pipeline elements
        # --------------------------------------------------

        self.streammux = self._make_element(
            "nvstreammux",
            "streammux",
        )

        self.tiler = self._make_element(
            "nvmultistreamtiler",
            "tiler",
        )

        self.converter = self._make_element(
            "nvvideoconvert",
            "display-converter",
        )

        self.capsfilter = self._make_element(
            "capsfilter",
            "display-rgba-caps",
        )

        self.osd = self._make_element(
            "nvdsosd",
            "osd",
        )

        self.sink = self._make_element(
            "nveglglessink",
            "display-sink",
        )

        # --------------------------------------------------
        # Configure legacy nvstreammux
        # --------------------------------------------------

        self.streammux.set_property(
            "batch-size",
            self.MAX_CAMERAS,
        )
        self.streammux.set_property("width", 1280)
        self.streammux.set_property("height", 720)
        self.streammux.set_property("live-source", True)
        self.streammux.set_property("batched-push-timeout", 40000)

        # Avoid downstream EOS after the final source ends,
        # where this property is supported.
        if self.streammux.find_property("drop-pipeline-eos"):
            self.streammux.set_property("drop-pipeline-eos", True)

        # --------------------------------------------------
        # Configure tiler
        # --------------------------------------------------

        self.tiler.set_property("width", self.DISPLAY_WIDTH)
        self.tiler.set_property("height", self.DISPLAY_HEIGHT)
        self.tiler.set_property("rows", 1)
        self.tiler.set_property("columns", 1)

        # --------------------------------------------------
        # Explicit RGBA output for OSD/display
        # --------------------------------------------------

        self.capsfilter.set_property(
            "caps",
            Gst.Caps.from_string(
                "video/x-raw(memory:NVMM), format=RGBA"
            ),
        )

        # --------------------------------------------------
        # Configure display
        # --------------------------------------------------

        self.sink.set_property("sync", False)
        self.sink.set_property("qos", False)

        # Important for a pipeline started without sources:
        # do not wait for a preroll buffer to change state.
        self.sink.set_property("async", False)

        # --------------------------------------------------
        # Add and link elements
        # --------------------------------------------------

        elements = [
            self.streammux,
            self.tiler,
            self.converter,
            self.capsfilter,
            self.osd,
            self.sink,
        ]

        for element in elements:
            self.pipeline.add(element)

            if element.get_parent() != self.pipeline:
                raise RuntimeError(
                    f"Could not add element: {element.get_name()}"
                )

        for upstream, downstream in zip(elements, elements[1:]):
            if not upstream.link(downstream):
                raise RuntimeError(
                    f"Could not link {upstream.get_name()} "
                    f"-> {downstream.get_name()}"
                )

        # --------------------------------------------------
        # Frame probes
        # --------------------------------------------------

        self.streammux.get_static_pad("src").add_probe(
            Gst.PadProbeType.BUFFER,
            self._mux_probe,
        )

        self.sink.get_static_pad("sink").add_probe(
            Gst.PadProbeType.BUFFER,
            self._display_probe,
        )

        # --------------------------------------------------
        # GStreamer bus
        # --------------------------------------------------

        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self._bus_handler_id = self.bus.connect(
            "message",
            self._on_bus_message,
        )

        logger.info(
            "DynamicPipeline created: pipeline=%s sink=%s",
            self.pipeline_id,
            self.sink.get_name(),
        )

    @staticmethod
    def _make_element(factory, name):
        element = Gst.ElementFactory.make(factory, name)

        if element is None:
            raise RuntimeError(
                f"Could not create GStreamer element: {factory}. "
                f"Check installation with: gst-inspect-1.0 {factory}"
            )

        return element

    # ======================================================
    # START
    # ======================================================

    def start(self):
        if self.loop is not None:
            logger.warning("Pipeline already started")
            return

        self.loop = GLib.MainLoop()

        self.loop_thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="deepstream-glib-loop",
        )
        self.loop_thread.start()

        result = self.pipeline.set_state(Gst.State.PLAYING)

        logger.info(
            "Pipeline PLAYING requested: result=%s",
            result.value_nick,
        )

        if result == Gst.StateChangeReturn.FAILURE:
            self.pipeline.set_state(Gst.State.NULL)
            self.loop.quit()
            self.loop_thread.join(timeout=5)

            self.loop = None
            self.loop_thread = None

            raise RuntimeError(
                "Failed starting DeepStream pipeline. "
                "Check GStreamer errors and the display environment."
            )

    def _run_loop(self):
        logger.info("DeepStream GLib loop started")
        self.loop.run()
        logger.info("DeepStream GLib loop stopped")

    # ======================================================
    # ADD CAMERA
    # ======================================================

    def add_camera(
        self,
        camera_id: str,
        rtsp_url: str,
        session_id: str,
        on_frame,
    ):
        camera_id = str(camera_id)

        if camera_id in self.sources:
            logger.info("Camera already exists: %s", camera_id)
            return self.sources[camera_id]["source_index"]

        if not self.free_indexes:
            raise RuntimeError(
                f"Maximum camera capacity reached: "
                f"{self.MAX_CAMERAS}/{self.MAX_CAMERAS}"
            )

        source_index = min(self.free_indexes)

        logger.info(
            "Adding camera: camera=%s source=%s",
            camera_id,
            source_index,
        )

        # Keep the callback signatures used by your existing
        # CameraSourceBin and camera manager.
        source = CameraSourceBin(
            camera_id=camera_id,
            source_index=source_index,
            rtsp_url=rtsp_url,
            on_frame=lambda: on_frame(camera_id, session_id),
            on_error=lambda message: self._notify_camera_error(
                camera_id,
                session_id,
                message,
            ),
        )

        mux_pad = None
        source_pad = None
        added = False
        registered = False

        try:
            # PyGObject's Gst.Bin.add() returns None on success and
            # raises on failure. Verify ownership instead of its return value.
            self.pipeline.add(source.bin)
            added = source.bin.get_parent() == self.pipeline

            if not added:
                raise RuntimeError(
                    f"Could not add source bin: {camera_id}"
                )

            pad_name = f"sink_{source_index}"

            if hasattr(self.streammux, "request_pad_simple"):
                mux_pad = self.streammux.request_pad_simple(pad_name)
            else:
                mux_pad = self.streammux.get_request_pad(pad_name)

            if mux_pad is None:
                raise RuntimeError(
                    f"Could not request nvstreammux {pad_name}"
                )

            source_pad = source.bin.get_static_pad("src")

            if source_pad is None:
                raise RuntimeError(
                    f"Source bin has no src pad: {camera_id}"
                )

            result = source_pad.link(mux_pad)

            if result != Gst.PadLinkReturn.OK:
                raise RuntimeError(
                    f"Could not link camera {camera_id} "
                    f"to {pad_name}: {result.value_nick}"
                )

            self.sources[camera_id] = {
                "source": source,
                "source_index": source_index,
                "mux_pad": mux_pad,
                "session_id": session_id,
            }

            self.free_indexes.remove(source_index)
            registered = True

            self._update_tiler_layout()

            if not source.bin.sync_state_with_parent():
                raise RuntimeError(
                    f"Could not start camera source: {camera_id}"
                )

            logger.info(
                "Camera source added: camera=%s source=%s "
                "active=%s/%s; waiting for video buffers",
                camera_id,
                source_index,
                len(self.sources),
                self.MAX_CAMERAS,
            )

            return source_index

        except Exception:
            source.bin.set_state(Gst.State.NULL)

            if source_pad is not None and mux_pad is not None:
                if source_pad.is_linked():
                    source_pad.unlink(mux_pad)

            if mux_pad is not None:
                self.streammux.release_request_pad(mux_pad)

            if added:
                self.pipeline.remove(source.bin)

            if registered:
                self.sources.pop(camera_id, None)
                self.free_indexes.add(source_index)
                self._update_tiler_layout()

            logger.exception(
                "Failed adding camera: camera=%s source=%s",
                camera_id,
                source_index,
            )
            raise

    # ======================================================
    # REMOVE CAMERA
    # ======================================================

    def remove_camera(self, camera_id):
        camera_id = str(camera_id)

        if camera_id not in self.sources:
            logger.info("Camera not running: %s", camera_id)
            return False

        return self._remove_camera_internal(camera_id)

    def _remove_camera_internal(self, camera_id):
        camera_id = str(camera_id)
        entry = self.sources.get(camera_id)

        if entry is None:
            return False

        source = entry["source"]
        source_index = entry["source_index"]
        mux_pad = entry["mux_pad"]

        logger.info(
            "Removing camera: camera=%s source=%s",
            camera_id,
            source_index,
        )

        source.bin.set_state(Gst.State.NULL)

        source_pad = source.bin.get_static_pad("src")

        if source_pad is not None and source_pad.is_linked():
            source_pad.unlink(mux_pad)

        self.streammux.release_request_pad(mux_pad)
        self.pipeline.remove(source.bin)

        del self.sources[camera_id]
        self.free_indexes.add(source_index)

        self._update_tiler_layout()

        logger.info(
            "Camera removed: camera=%s source=%s active=%s/%s",
            camera_id,
            source_index,
            len(self.sources),
            self.MAX_CAMERAS,
        )

        return True

    # ======================================================
    # DISPLAY LAYOUT
    # ======================================================

    def _update_tiler_layout(self):
        # Slots may contain gaps after removing a camera.
        # Size the grid to include the highest active slot.
        slots = max(
            (
                entry["source_index"] + 1
                for entry in self.sources.values()
            ),
            default=1,
        )

        if slots <= 1:
            rows, columns = 1, 1
        elif slots == 2:
            rows, columns = 1, 2
        elif slots <= 4:
            rows, columns = 2, 2
        else:
            rows, columns = 2, 3

        self.tiler.set_property("rows", rows)
        self.tiler.set_property("columns", columns)

        logger.info(
            "Display layout updated: cameras=%s layout=%sx%s",
            len(self.sources),
            rows,
            columns,
        )

    # ======================================================
    # FRAME DIAGNOSTICS
    # ======================================================

    def _mux_probe(self, pad, info):
        if info.get_buffer() is not None:
            self._mux_buffer_count += 1

            if self._mux_buffer_count == 1:
                logger.info(
                    "First video batch received from nvstreammux"
                )
            elif self._mux_buffer_count % 300 == 0:
                logger.info(
                    "nvstreammux video batches: %s",
                    self._mux_buffer_count,
                )

        return Gst.PadProbeReturn.OK

    def _display_probe(self, pad, info):
        if info.get_buffer() is not None:
            self._display_buffer_count += 1

            if self._display_buffer_count == 1:
                logger.info(
                    "First video buffer reached display sink"
                )
            elif self._display_buffer_count % 300 == 0:
                logger.info(
                    "Display video buffers: %s",
                    self._display_buffer_count,
                )

        return Gst.PadProbeReturn.OK

    # ======================================================
    # CAMERA INFORMATION
    # ======================================================

    def get_camera(self, camera_id):
        return self.sources.get(str(camera_id))

    def get_active_cameras(self):
        return {
            camera_id: entry["source_index"]
            for camera_id, entry in self.sources.items()
        }

    def get_camera_count(self):
        return len(self.sources)

    # ======================================================
    # ERROR CALLBACK
    # ======================================================

    def _notify_camera_error(
        self,
        camera_id,
        session_id,
        message,
    ):
        logger.error(
            "Camera error: camera=%s session=%s message=%s",
            camera_id,
            session_id,
            message,
        )

        if self.on_error is not None:
            try:
                self.on_error(
                    camera_id,
                    session_id,
                    str(message),
                )
            except Exception:
                logger.exception(
                    "Camera error callback failed: camera=%s",
                    camera_id,
                )

    # ======================================================
    # GSTREAMER BUS
    # ======================================================

    def _on_bus_message(self, bus, message):
        message_type = message.type

        element_name = (
            message.src.get_name()
            if message.src is not None
            else "unknown"
        )

        if message_type == Gst.MessageType.ERROR:
            error, debug = message.parse_error()

            logger.error(
                "GStreamer ERROR: element=%s error=%s debug=%s",
                element_name,
                error,
                debug,
            )

            entries = list(self.sources.items())
            affected = []
            source_message = False

            element = message.src

            while element is not None:
                if element.get_name().startswith("camera-source-"):
                    source_message = True

                affected = [
                    (camera_id, entry)
                    for camera_id, entry in entries
                    if entry["source"].bin == element
                ]

                if affected:
                    break

                element = element.get_parent()

            # Shared element failures affect all active cameras.
            # Ignore delayed errors from removed camera bins.
            if not affected and not source_message:
                affected = entries

            for camera_id, entry in affected:
                self._notify_camera_error(
                    camera_id,
                    entry["session_id"],
                    str(error),
                )

        elif message_type == Gst.MessageType.WARNING:
            warning, debug = message.parse_warning()

            logger.warning(
                "GStreamer WARNING: element=%s warning=%s debug=%s",
                element_name,
                warning,
                debug,
            )

        elif message_type == Gst.MessageType.STATE_CHANGED:
            if message.src == self.pipeline:
                old_state, new_state, pending = (
                    message.parse_state_changed()
                )

                logger.info(
                    "Pipeline state: %s -> %s pending=%s",
                    old_state.value_nick,
                    new_state.value_nick,
                    pending.value_nick,
                )

        elif message_type == Gst.MessageType.ASYNC_DONE:
            logger.info(
                "GStreamer asynchronous state change completed"
            )

        elif message_type == Gst.MessageType.LATENCY:
            self.pipeline.recalculate_latency()

        elif message_type == Gst.MessageType.EOS:
            logger.warning("GStreamer pipeline EOS received")

    # ======================================================
    # SHUTDOWN
    # ======================================================

    def shutdown(self):
        logger.info("Shutting down DeepStream pipeline")

        # Stop the pipeline before dismantling its sources.
        self.pipeline.set_state(Gst.State.NULL)

        for camera_id in list(self.sources):
            try:
                self._remove_camera_internal(camera_id)
            except Exception:
                logger.exception(
                    "Failed removing camera: %s",
                    camera_id,
                )

        if self.loop is not None:
            self.loop.quit()

        if (
            self.loop_thread is not None
            and self.loop_thread is not threading.current_thread()
        ):
            self.loop_thread.join(timeout=5)

        self.loop = None
        self.loop_thread = None

        logger.info("DeepStream pipeline stopped")
