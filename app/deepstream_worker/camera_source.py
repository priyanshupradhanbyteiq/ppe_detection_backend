import logging

import gi
import time
gi.require_version("Gst", "1.0")
gi.require_version("GstRtsp", "1.0")

from gi.repository import Gst, GstRtsp


logger = logging.getLogger(__name__)


class CameraSourceBin:

    def __init__(
        self,
        camera_id: str,
        source_index: int,
        rtsp_url: str,
        on_frame,
        on_error,
    ):
        self.camera_id = str(camera_id)
        self.source_index = source_index
        self.rtsp_url = rtsp_url
        self.on_frame = on_frame
        self.on_error = on_error
        #Camera Reconnect State
        self.frame_count = 0
        self.last_frame_arrival = None
        self.reconnect_delay = 2
        
        self.bin = Gst.Bin.new(
            f"camera-source-{source_index}"
        )

        if not self.bin:
            raise RuntimeError(
                "Unable to create camera source bin"
            )

        # -------------------------------------------------
        # Elements
        # -------------------------------------------------

        self.source = Gst.ElementFactory.make(
            "rtspsrc",
            f"rtspsrc-{source_index}",
        )

        self.queue1 = Gst.ElementFactory.make(
            "queue",
            f"queue1-{source_index}",
        )

        self.depay = Gst.ElementFactory.make(
            "rtph265depay",
            f"depay-{source_index}",
        )

        self.queue2 = Gst.ElementFactory.make(
            "queue",
            f"queue2-{source_index}",
        )

        self.parser = Gst.ElementFactory.make(
            "h265parse",
            f"parser-{source_index}",
        )

        self.queue3 = Gst.ElementFactory.make(
            "queue",
            f"queue3-{source_index}",
        )

        self.decoder = Gst.ElementFactory.make(
            "nvv4l2decoder",
            f"decoder-{source_index}",
        )

        elements = [
            self.source,
            self.queue1,
            self.depay,
            self.queue2,
            self.parser,
            self.queue3,
            self.decoder,
        ]

        for element in elements:
            if not element:
                raise RuntimeError(
                    f"Failed creating source element "
                    f"camera={camera_id}"
                )

            self.bin.add(element)

        # -------------------------------------------------
        # RTSP
        # -------------------------------------------------

        self.source.set_property(
            "location",
            rtsp_url,
        )

        self.source.set_property(
            "protocols",
            GstRtsp.RTSPLowerTrans.TCP,
        )

        self.source.set_property(
            "latency",
            200,
        )

        self.source.set_property(
            "drop-on-latency",
            True,
        )

        # -------------------------------------------------
        # Static source chain
        # -------------------------------------------------

        if not self.queue1.link(self.depay):
            raise RuntimeError(
                "queue1 -> depay failed"
            )

        if not self.depay.link(self.queue2):
            raise RuntimeError(
                "depay -> queue2 failed"
            )

        if not self.queue2.link(self.parser):
            raise RuntimeError(
                "queue2 -> parser failed"
            )

        if not self.parser.link(self.queue3):
            raise RuntimeError(
                "parser -> queue3 failed"
            )

        if not self.queue3.link(self.decoder):
            raise RuntimeError(
                "queue3 -> decoder failed"
            )

        # -------------------------------------------------
        # Dynamic rtspsrc
        # -------------------------------------------------

        self.source.connect(
            "pad-added",
            self._on_pad_added,
        )

        # -------------------------------------------------
        # Ghost output
        # -------------------------------------------------

        decoder_src_pad = (
            self.decoder.get_static_pad("src")
        )

        if not decoder_src_pad:
            raise RuntimeError(
                "Unable to get decoder src pad"
            )

        decoder_src_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_frame)

        ghost_pad = Gst.GhostPad.new(
            "src",
            decoder_src_pad,
        )

        if not ghost_pad:
            raise RuntimeError(
                "Unable to create ghost pad"
            )

        self.bin.add_pad(
            ghost_pad
        )

    # =====================================================
    # RTSP PAD
    # =====================================================

    def _on_frame(self, pad, info):
        buffer = info.get_buffer()

        if buffer is None:
            return Gst.PadProbeReturn.OK

        self.frame_count += 1

        self.last_frame_arrival = time.monotonic()



    def _on_pad_added(
        self,
        src,
        pad,
    ):
        caps = pad.get_current_caps()

        if caps is None:
            caps = pad.query_caps(None)

        if not caps:
            return

        structure = caps.get_structure(0)

        if (
            structure.get_name()
            != "application/x-rtp"
        ):
            return

        media = structure.get_value(
            "media"
        )

        encoding = structure.get_value(
            "encoding-name"
        )

        if media != "video":
            return

        # For now H265 only.
        if encoding != "H265":

            self.on_error(f"Unsupported RTSP codec: {encoding}; H265 is required")

            return

        sink_pad = (
            self.queue1.get_static_pad(
                "sink"
            )
        )

        if sink_pad.is_linked():
            return

        result = pad.link(
            sink_pad
        )

        if result != Gst.PadLinkReturn.OK:
            self.on_error(f"Unable to link RTSP video pad: {result.value_nick}")

        logger.info(
            "RTSP linked. "
            "camera=%s source=%s result=%s",
            self.camera_id,
            self.source_index,
            result.value_nick,
        )