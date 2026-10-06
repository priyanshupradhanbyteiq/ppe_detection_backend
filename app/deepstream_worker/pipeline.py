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

    def __init__(self):

        Gst.init(None)

        self.pipeline_id = self.PIPELINE_ID

        self.pipeline = Gst.Pipeline.new(
            self.PIPELINE_ID
        )

        if not self.pipeline:
            raise RuntimeError(
                "Unable to create pipeline"
            )

        # camera_id -> source information
        self.sources = {}

        # 0,1,2,3,4
        self.free_indexes = set(
            range(self.MAX_CAMERAS)
        )

        self.loop = None
        self.loop_thread = None
        self.on_error = None

        # -------------------------------------------------
        # Streammux
        # -------------------------------------------------

        self.streammux = (
            Gst.ElementFactory.make(
                "nvstreammux",
                "streammux",
            )
        )

        self.converter = (
            Gst.ElementFactory.make(
                "nvvideoconvert",
                "converter",
            )
        )

        self.sink = (
            Gst.ElementFactory.make(
                "fakesink",
                "sink",
            )
        )

        for element in [
            self.streammux,
            self.converter,
            self.sink,
        ]:
            if not element:
                raise RuntimeError(
                    "Unable to create pipeline element"
                )

            self.pipeline.add(
                element
            )

        # -------------------------------------------------
        # Streammux config
        # -------------------------------------------------

        self.streammux.set_property(
            "batch-size",
            self.MAX_CAMERAS,
        )

        self.streammux.set_property(
            "width",
            1280,
        )

        self.streammux.set_property(
            "height",
            720,
        )

        self.streammux.set_property(
            "live-source",
            True,
        )

        self.streammux.set_property(
            "batched-push-timeout",
            40000,
        )

        self.sink.set_property(
            "sync",
            False,
        )

        self.sink.set_property(
            "async",
            False,
        )

        if not self.streammux.link(
            self.converter
        ):
            raise RuntimeError(
                "streammux -> converter failed"
            )

        if not self.converter.link(
            self.sink
        ):
            raise RuntimeError(
                "converter -> sink failed"
            )

        # -------------------------------------------------
        # Bus
        # -------------------------------------------------

        bus = self.pipeline.get_bus()

        bus.add_signal_watch()

        bus.connect(
            "message",
            self._on_bus_message,
        )

    # =====================================================
    # START PIPELINE
    # =====================================================

    def start(self):

        if self.loop is not None:
            return

        result = self.pipeline.set_state(Gst.State.PLAYING)
        if result == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Failed starting DeepStream pipeline")

        self.loop = GLib.MainLoop()

        self.loop_thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="deepstream-glib-loop",
        )

        self.loop_thread.start()

        logger.info(
            "DeepStream GLib loop started."
        )

    def _run_loop(self):

        logger.info(
            "Pipeline started. pipeline=%s",
            self.pipeline_id,
        )

        self.loop.run()

    # =====================================================
    # ADD CAMERA
    # =====================================================

    def add_camera(
        self,
        camera_id: str,
        rtsp_url: str,
        session_id: str,
        on_frame,
    ):

        camera_id = str(camera_id)

        if camera_id in self.sources:

            entry = self.sources[
                camera_id
            ]

            return entry[
                "source_index"
            ]

        if not self.free_indexes:

            raise RuntimeError(
                "Default pipeline capacity reached: 5/5"
            )

        source_index = min(
            self.free_indexes
        )

        logger.info(
            "Adding camera. "
            "camera=%s pipeline=%s source=%s",
            camera_id,
            self.pipeline_id,
            source_index,
        )

        source = CameraSourceBin(
            camera_id=camera_id,
            source_index=source_index,
            rtsp_url=rtsp_url,
            on_frame=lambda: on_frame(camera_id, session_id),
            on_error=lambda message: self.on_error(camera_id, session_id, message) if self.on_error else None,
        )

        self.pipeline.add(
            source.bin
        )

        # ---------------------------------------------
        # nvstreammux sink_N
        # ---------------------------------------------

        mux_pad = (
            self.streammux
            .request_pad_simple(
                f"sink_{source_index}"
            )
        )

        if not mux_pad:

            self.pipeline.remove(
                source.bin
            )

            raise RuntimeError(
                f"Unable to request "
                f"streammux sink_{source_index}"
            )

        source_pad = (
            source.bin
            .get_static_pad("src")
        )

        result = source_pad.link(
            mux_pad
        )

        if (
            result
            != Gst.PadLinkReturn.OK
        ):

            self.streammux.release_request_pad(
                mux_pad
            )

            self.pipeline.remove(
                source.bin
            )

            raise RuntimeError(
                f"Unable to link camera "
                f"{camera_id} to streammux"
            )

        # ---------------------------------------------
        # Save mapping BEFORE starting
        # ---------------------------------------------

        self.sources[
            camera_id
        ] = {
            "source": source,
            "source_index": source_index,
            "mux_pad": mux_pad,
            "session_id": session_id,
        }

        self.free_indexes.remove(
            source_index
        )

        # ---------------------------------------------
        # Start source while parent already PLAYING
        # ---------------------------------------------

        if not source.bin.sync_state_with_parent():

            self._remove_camera_internal(
                camera_id
            )

            raise RuntimeError(
                f"Unable to start camera "
                f"{camera_id}"
            )

        logger.info(
            "Camera added. "
            "camera=%s pipeline=%s source=%s "
            "active=%s/%s",
            camera_id,
            self.pipeline_id,
            source_index,
            len(self.sources),
            self.MAX_CAMERAS,
        )

        return source_index

    # =====================================================
    # REMOVE CAMERA
    # =====================================================

    def remove_camera(
        self,
        camera_id: str,
    ):

        camera_id = str(camera_id)

        if camera_id not in self.sources:

            logger.info(
                "Camera not present in pipeline. "
                "camera=%s",
                camera_id,
            )

            return False

        return self._remove_camera_internal(
            camera_id
        )

    def _remove_camera_internal(
        self,
        camera_id,
    ):

        entry = self.sources[
            camera_id
        ]

        source = entry["source"]

        source_index = entry[
            "source_index"
        ]

        mux_pad = entry[
            "mux_pad"
        ]

        logger.info(
            "Removing camera. "
            "camera=%s source=%s",
            camera_id,
            source_index,
        )

        # Stop only this camera.
        if source.bin.set_state(Gst.State.NULL) == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError(f"Unable to stop camera {camera_id}")

        source_pad = (
            source.bin
            .get_static_pad("src")
        )

        if source_pad.is_linked():

            source_pad.unlink(
                mux_pad
            )

        self.streammux.release_request_pad(
            mux_pad
        )

        self.pipeline.remove(
            source.bin
        )

        del self.sources[
            camera_id
        ]

        self.free_indexes.add(
            source_index
        )

        logger.info(
            "Camera removed. "
            "camera=%s freed_source=%s "
            "active=%s/%s",
            camera_id,
            source_index,
            len(self.sources),
            self.MAX_CAMERAS,
        )

        return True

    # =====================================================
    # STATUS
    # =====================================================

    def get_camera(
        self,
        camera_id,
    ):

        return self.sources.get(
            str(camera_id)
        )

    def get_active_cameras(
        self,
    ):

        return {
            camera_id: data[
                "source_index"
            ]
            for camera_id, data
            in self.sources.items()
        }

    # =====================================================
    # BUS
    # =====================================================

    def _on_bus_message(
        self,
        bus,
        message,
    ):

        if (
            message.type
            == Gst.MessageType.ERROR
        ):

            error, debug = (
                message.parse_error()
            )

            logger.error("GStreamer error: %s debug=%s", error, debug)
            affected = []
            source_message = False
            element = message.src
            while element is not None:
                if element.get_name().startswith("camera-source-"):
                    source_message = True
                affected = [(camera_id, entry["session_id"])
                            for camera_id, entry in list(self.sources.items())
                            if entry["source"].bin == element]
                if affected:
                    break
                element = element.get_parent()
            # Ignore delayed errors from a source that has already been removed.
            if not affected and not source_message:
                affected = [(camera_id, entry["session_id"])
                            for camera_id, entry in list(self.sources.items())]
            if self.on_error:
                for camera_id, session_id in affected:
                    self.on_error(camera_id, session_id, str(error))

        elif (
            message.type
            == Gst.MessageType.WARNING
        ):

            error, debug = (
                message.parse_warning()
            )

            logger.warning(
                "GStreamer warning: %s",
                error,
            )

    # =====================================================
    # STOP WHOLE WORKER PIPELINE
    # =====================================================

    def shutdown(self):

        for camera_id in list(
            self.sources.keys()
        ):
            self.remove_camera(
                camera_id
            )

        self.pipeline.set_state(
            Gst.State.NULL
        )

        if self.loop:
            self.loop.quit()

        if self.loop_thread and self.loop_thread is not threading.current_thread():
            self.loop_thread.join(timeout=5)
        self.loop_thread = None
        self.loop = None

        logger.info(
            "Pipeline shutdown complete."
        )