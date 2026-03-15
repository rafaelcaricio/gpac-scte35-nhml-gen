# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Generate SCTE-35 event message track (NHML) for use with GPAC.

Probes the video file for timescale and duration, then converts a JSON
schedule of splice events into an NHML file with emib/emeb samples
following the ISO 23001-18 sample boundary algorithm.

Example:
  uv run scte35_nhml_gen.py events.json input.mp4 -o events.nhml
  gpac -i input.mp4 -i events.nhml:#ID=3 \\
    -o output/manifest.mpd:profile=live:cmaf=cmf2:segdur=6
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import struct
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass

SCTE35_SCHEME_URI = "urn:scte:scte35:2013:bin"
SUB_SEGMENT_TYPE_IDS = frozenset([0x30, 0x32, 0x34, 0x36, 0x38, 0x3A, 0x44, 0x46])

SEGMENTATION_TYPE_NAMES: dict[int, str] = {
    0x00: "NotIndicated",
    0x01: "ContentIdentification",
    0x02: "ProgramStart (Deprecated)",
    0x10: "ProgramStart",
    0x11: "ProgramEnd",
    0x12: "ProgramEarlyTermination",
    0x13: "ProgramBreakaway",
    0x14: "ProgramResumption",
    0x15: "ProgramRunoverPlanned",
    0x16: "ProgramRunoverUnplanned",
    0x17: "ProgramOverlapStart",
    0x18: "ProgramBlackoutOverride",
    0x19: "ProgramStartInProgress",
    0x1A: "ProgramJoin",
    0x20: "ChapterStart",
    0x21: "ChapterEnd",
    0x22: "BreakStart",
    0x23: "BreakEnd",
    0x24: "OpeningCreditStart",
    0x25: "OpeningCreditEnd",
    0x26: "ClosingCreditStart",
    0x27: "ClosingCreditEnd",
    0x30: "ProviderAdStart",
    0x31: "ProviderAdEnd",
    0x32: "DistributorAdStart",
    0x33: "DistributorAdEnd",
    0x34: "ProviderPOStart",
    0x35: "ProviderPOEnd",
    0x36: "DistributorPOStart",
    0x37: "DistributorPOEnd",
    0x38: "ProviderOverlayPOStart",
    0x39: "ProviderOverlayPOEnd",
    0x3A: "DistributorOverlayPOStart",
    0x3B: "DistributorOverlayPOEnd",
    0x3C: "ProviderPromoStart",
    0x3D: "ProviderPromoEnd",
    0x3E: "DistributorPromoStart",
    0x3F: "DistributorPromoEnd",
    0x40: "UnscheduledEventStart",
    0x41: "UnscheduledEventEnd",
    0x42: "AlternateContentOpportunityStart",
    0x43: "AlternateContentOpportunityEnd",
    0x44: "NetworkStart",
    0x45: "NetworkEnd",
    0x46: "ProviderAdBlockStart",
    0x47: "ProviderAdBlockEnd",
    0x50: "ContentStart",
    0x51: "ContentEnd",
}

SEGMENTATION_CLOSE_TO_OPEN: dict[int, int] = {
    0x11: 0x10,  # ProgramEnd → ProgramStart
    0x21: 0x20,  # ChapterEnd → ChapterStart
    0x31: 0x30,  # ProviderAdEnd → ProviderAdStart
    0x33: 0x32,  # DistributorAdEnd → DistributorAdStart
    0x35: 0x34,  # ProviderPOEnd → ProviderPOStart
    0x37: 0x36,  # DistributorPOEnd → DistributorPOStart
    0x41: 0x40,  # UnscheduledEventEnd → UnscheduledEventStart
    0x45: 0x44,  # NetworkEnd → NetworkStart
}


class BitWriter:
    def __init__(self) -> None:
        self._buf = bytearray()
        self._byte = 0
        self._bits_in_byte = 0

    def write_bits(self, value: int, num_bits: int) -> None:
        for i in range(num_bits - 1, -1, -1):
            bit = (value >> i) & 1
            self._byte = (self._byte << 1) | bit
            self._bits_in_byte += 1
            if self._bits_in_byte == 8:
                self._buf.append(self._byte)
                self._byte = 0
                self._bits_in_byte = 0

    def write_bytes(self, data: bytes) -> None:
        assert self._bits_in_byte == 0, "write_bytes requires byte alignment"
        self._buf.extend(data)

    def flush(self) -> bytes:
        if self._bits_in_byte > 0:
            self._byte <<= 8 - self._bits_in_byte
            self._buf.append(self._byte)
            self._byte = 0
            self._bits_in_byte = 0
        return bytes(self._buf)

    @property
    def byte_length(self) -> int:
        extra = 1 if self._bits_in_byte > 0 else 0
        return len(self._buf) + extra


_CRC32_TABLE: list[int] | None = None


def _crc32_table() -> list[int]:
    global _CRC32_TABLE
    if _CRC32_TABLE is None:
        table = []
        for i in range(256):
            crc = i << 24
            for _ in range(8):
                if crc & 0x80000000:
                    crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF
                else:
                    crc = (crc << 1) & 0xFFFFFFFF
            table.append(crc)
        _CRC32_TABLE = table
    return _CRC32_TABLE


def crc32_mpeg2(data: bytes) -> int:
    table = _crc32_table()
    crc = 0xFFFFFFFF
    for byte in data:
        crc = ((crc << 8) ^ table[((crc >> 24) ^ byte) & 0xFF]) & 0xFFFFFFFF
    return crc


def encode_splice_time(w: BitWriter, pts: int) -> None:
    w.write_bits(1, 1)  # time_specified_flag
    w.write_bits(0x3F, 6)  # reserved
    w.write_bits(pts & 0x1FFFFFFFF, 33)  # pts_time


def encode_break_duration(w: BitWriter, auto_return: bool, duration_90khz: int) -> None:
    w.write_bits(1 if auto_return else 0, 1)
    w.write_bits(0x3F, 6)  # reserved
    w.write_bits(duration_90khz & 0x1FFFFFFFF, 33)


def encode_splice_insert(event: SpliceEvent) -> bytes:
    w = BitWriter()
    w.write_bits(event.splice_event_id, 32)
    w.write_bits(0, 1)  # splice_event_cancel_indicator
    w.write_bits(0x7F, 7)  # reserved

    w.write_bits(1 if event.out_of_network else 0, 1)
    w.write_bits(1, 1)  # program_splice_flag
    has_duration = event.duration_90khz is not None and event.duration_90khz > 0
    w.write_bits(1 if has_duration else 0, 1)  # duration_flag
    w.write_bits(0, 1)  # splice_immediate_flag
    w.write_bits(0, 1)  # event_id_compliance_flag
    w.write_bits(0x7, 3)  # reserved

    encode_splice_time(w, event.pts_90khz)

    if has_duration:
        encode_break_duration(w, event.auto_return, event.duration_90khz)

    w.write_bits(0, 16)  # unique_program_id
    w.write_bits(0, 8)  # avail_num
    w.write_bits(0, 8)  # avails_expected

    return w.flush()


def encode_time_signal(pts_90khz: int) -> bytes:
    w = BitWriter()
    encode_splice_time(w, pts_90khz)
    return w.flush()


def encode_segmentation_descriptor(event: SpliceEvent) -> bytes:
    seg = event.segmentation
    if seg is None:
        return b""

    upid_bytes = seg.upid.encode("utf-8") if seg.upid else b""
    has_duration = seg.segmentation_duration_90khz is not None and seg.segmentation_duration_90khz > 0
    seg_type_id = seg.segmentation_type_id or 0
    has_sub_segments = seg_type_id in SUB_SEGMENT_TYPE_IDS

    body = BitWriter()
    body.write_bits(0x43554549, 32)  # identifier "CUEI"
    body.write_bits(seg.segmentation_event_id, 32)
    body.write_bits(0, 1)  # segmentation_event_cancel_indicator
    body.write_bits(0, 1)  # segmentation_event_id_compliance_indicator
    body.write_bits(0x3F, 6)  # reserved

    body.write_bits(1, 1)  # program_segmentation_flag
    body.write_bits(1 if has_duration else 0, 1)  # segmentation_duration_flag
    body.write_bits(1, 1)  # delivery_not_restricted_flag
    body.write_bits(0x1F, 5)  # reserved

    if has_duration:
        body.write_bits(seg.segmentation_duration_90khz & 0xFFFFFFFFFF, 40)

    body.write_bits(seg.upid_type, 8)
    body.write_bits(len(upid_bytes), 8)
    if upid_bytes:
        body.write_bytes(upid_bytes)

    body.write_bits(seg_type_id, 8)
    body.write_bits(0, 8)  # segment_num
    body.write_bits(0, 8)  # segments_expected

    if has_sub_segments:
        body.write_bits(0, 8)  # sub_segment_num
        body.write_bits(0, 8)  # sub_segments_expected

    body_bytes = body.flush()

    w = BitWriter()
    w.write_bits(0x02, 8)  # splice_descriptor_tag
    w.write_bits(len(body_bytes), 8)  # descriptor_length
    w.write_bytes(body_bytes)
    return w.flush()


def encode_splice_info_section(event: SpliceEvent) -> bytes:
    if event.command == "splice_insert":
        command_type = 0x05
        command_bytes = encode_splice_insert(event)
    elif event.command == "time_signal":
        command_type = 0x06
        command_bytes = encode_time_signal(event.pts_90khz)
    else:
        raise ValueError(f"Unknown command: {event.command}")

    descriptor_bytes = b""
    if event.segmentation is not None:
        descriptor_bytes = encode_segmentation_descriptor(event)

    # Build everything after section_length:
    # protocol_version(8) + encrypted_packet(1) + encryption_algorithm(6) +
    # pts_adjustment(33) + cw_index(8) + tier(12) + splice_command_length(12) +
    # splice_command_type(8) = 88 bits = 11 bytes
    # + command_bytes + descriptor_loop_length(16) + descriptor_bytes + CRC_32(32)
    payload_length = 11 + len(command_bytes) + 2 + len(descriptor_bytes) + 4

    w = BitWriter()
    w.write_bits(0xFC, 8)  # table_id
    w.write_bits(0, 1)  # section_syntax_indicator
    w.write_bits(0, 1)  # private_indicator
    w.write_bits(0x3, 2)  # sap_type
    w.write_bits(payload_length, 12)  # section_length

    w.write_bits(0, 8)  # protocol_version
    w.write_bits(0, 1)  # encrypted_packet
    w.write_bits(0, 6)  # encryption_algorithm
    w.write_bits(0, 33)  # pts_adjustment
    w.write_bits(0, 8)  # cw_index
    w.write_bits(0xFFF, 12)  # tier
    w.write_bits(len(command_bytes), 12)  # splice_command_length
    w.write_bits(command_type, 8)  # splice_command_type

    w.write_bytes(command_bytes)

    w.write_bits(len(descriptor_bytes), 16)  # descriptor_loop_length
    if descriptor_bytes:
        w.write_bytes(descriptor_bytes)

    data_before_crc = w.flush()
    crc = crc32_mpeg2(data_before_crc)
    return data_before_crc + struct.pack(">I", crc)


def parse_time_string(s: str) -> float:
    s = s.strip()
    if not s:
        raise ValueError("Empty time string")

    for suffix, factor in [("us", 1e-6), ("ms", 1e-3), ("s", 1.0)]:
        if s.endswith(suffix):
            num_part = s[: -len(suffix)]
            try:
                return float(num_part) * factor
            except ValueError:
                raise ValueError(f"Invalid time string: {s!r}")

    parts = s.split(":")
    if len(parts) == 3:
        try:
            h, m, sec = float(parts[0]), float(parts[1]), float(parts[2])
            return h * 3600 + m * 60 + sec
        except ValueError:
            raise ValueError(f"Invalid time string: {s!r}")
    elif len(parts) == 2:
        try:
            m, sec = float(parts[0]), float(parts[1])
            return m * 60 + sec
        except ValueError:
            raise ValueError(f"Invalid time string: {s!r}")
    elif len(parts) == 1:
        try:
            return float(s)
        except ValueError:
            raise ValueError(f"Invalid time string: {s!r}")
    else:
        raise ValueError(f"Invalid time string: {s!r}")


def parse_duration_field(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        seconds = parse_time_string(value)
        return int(seconds * 90000)
    raise ValueError(f"Invalid duration value: {value!r}")


@dataclass
class SegmentationInfo:
    segmentation_event_id: int
    segmentation_type_id: int | None
    segmentation_duration_90khz: int | None
    upid_type: int
    upid: str | None


@dataclass
class SpliceEvent:
    pts_90khz: int
    command: str
    splice_event_id: int
    out_of_network: bool
    duration_90khz: int | None
    auto_return: bool
    segmentation: SegmentationInfo | None
    pre_roll_90khz: int = 0


def parse_schedule(data: dict) -> list[SpliceEvent]:
    if data.get("version") != 1:
        raise ValueError(f"Unsupported schedule version: {data.get('version')}")

    raw_events = data.get("events")
    if not isinstance(raw_events, list):
        raise ValueError("Schedule must contain an 'events' array")

    global_pre_roll_90khz = 0
    if "pre_roll" in data:
        global_pre_roll_90khz = parse_duration_field(data["pre_roll"])
        if global_pre_roll_90khz < 0:
            raise ValueError("Global pre_roll must not be negative")

    events: list[SpliceEvent] = []
    next_id = 1

    for i, raw in enumerate(raw_events):
        has_time = "time" in raw
        has_pts = "pts" in raw
        if has_time and has_pts:
            raise ValueError(f"Event {i}: 'time' and 'pts' are mutually exclusive")
        if not has_time and not has_pts:
            raise ValueError(f"Event {i}: must have 'time' or 'pts'")

        if has_pts:
            pts_90khz = int(raw["pts"])
        else:
            pts_90khz = int(parse_time_string(raw["time"]) * 90000)

        command = raw.get("command")
        if command not in ("splice_insert", "time_signal"):
            raise ValueError(f"Event {i}: unknown command {command!r}")

        splice_event_id = raw.get("splice_event_id", next_id)
        next_id = splice_event_id + 1

        duration_90khz = None
        if "duration" in raw:
            duration_90khz = parse_duration_field(raw["duration"])

        seg_info = None
        raw_seg = raw.get("segmentation")
        if raw_seg is not None:
            seg_dur = None
            if "segmentation_duration" in raw_seg:
                seg_dur = parse_duration_field(raw_seg["segmentation_duration"])

            seg_info = SegmentationInfo(
                segmentation_event_id=raw_seg.get("segmentation_event_id", splice_event_id),
                segmentation_type_id=raw_seg.get("segmentation_type_id"),
                segmentation_duration_90khz=seg_dur,
                upid_type=raw_seg.get("upid_type", 0),
                upid=raw_seg.get("upid"),
            )
        elif command == "time_signal":
            print(
                f"Warning: event {i} is time_signal without segmentation "
                f"(SCTE-35 §9.3 requires segmentation_descriptor)",
                file=sys.stderr,
            )

        pre_roll_90khz = global_pre_roll_90khz
        if "pre_roll" in raw:
            pre_roll_90khz = parse_duration_field(raw["pre_roll"])
            if pre_roll_90khz < 0:
                raise ValueError(f"Event {i}: pre_roll must not be negative")

        events.append(
            SpliceEvent(
                pts_90khz=pts_90khz,
                command=command,
                splice_event_id=splice_event_id,
                out_of_network=raw.get("out_of_network", True),
                duration_90khz=duration_90khz,
                auto_return=raw.get("auto_return", False),
                segmentation=seg_info,
                pre_roll_90khz=pre_roll_90khz,
            )
        )

    events.sort(key=lambda e: e.pts_90khz)
    return events


@dataclass
class NHMLSample:
    dts: int
    duration: int
    emib_list: list[dict]


def _build_deactivation_map(events: list[SpliceEvent], timescale: int) -> dict[int, int]:
    """Map (seg_event_id of open event) → deactivation tick from closing event."""
    deactivations: dict[int, int] = {}

    open_events: dict[tuple[int, int], SpliceEvent] = {}
    for ev in events:
        if ev.segmentation is None:
            continue
        seg_type = ev.segmentation.segmentation_type_id or 0
        seg_eid = ev.segmentation.segmentation_event_id

        if seg_type in SEGMENTATION_CLOSE_TO_OPEN.values():
            open_events[(seg_eid, seg_type)] = ev

    for ev in events:
        if ev.segmentation is None:
            continue
        seg_type = ev.segmentation.segmentation_type_id or 0
        if seg_type not in SEGMENTATION_CLOSE_TO_OPEN:
            continue

        open_type = SEGMENTATION_CLOSE_TO_OPEN[seg_type]
        seg_eid = ev.segmentation.segmentation_event_id
        key = (seg_eid, open_type)

        if key in open_events:
            open_ev = open_events[key]
            close_tick = int(ev.pts_90khz * timescale / 90000)
            deactivations[id(open_ev)] = close_tick

    return deactivations


def compute_samples(
    seg_start: int,
    seg_end: int,
    events: list[SpliceEvent],
    timescale: int,
) -> list[NHMLSample]:
    deactivation_map = _build_deactivation_map(events, timescale)

    boundaries: set[int] = {seg_start, seg_end}

    for ev in events:
        ev_pts = int(ev.pts_90khz * timescale / 90000)
        if seg_start <= ev_pts < seg_end:
            boundaries.add(ev_pts)

        pre_roll_ticks = int(ev.pre_roll_90khz * timescale / 90000)
        if pre_roll_ticks > 0:
            pre_roll_start = max(seg_start, ev_pts - pre_roll_ticks)
            if pre_roll_start < ev_pts and seg_start <= pre_roll_start < seg_end:
                boundaries.add(pre_roll_start)

        ev_dur = 0
        if ev.duration_90khz and ev.duration_90khz > 0:
            ev_dur = int(ev.duration_90khz * timescale / 90000)

        paired_deactivation = deactivation_map.get(id(ev))

        if ev_dur > 0:
            deactivation = ev_pts + ev_dur
            if seg_start < deactivation < seg_end:
                boundaries.add(deactivation)

        if paired_deactivation is not None and seg_start < paired_deactivation < seg_end:
            boundaries.add(paired_deactivation)

    sorted_boundaries = sorted(boundaries)
    samples: list[NHMLSample] = []

    for idx in range(len(sorted_boundaries) - 1):
        t_i = sorted_boundaries[idx]
        t_next = sorted_boundaries[idx + 1]
        sample_duration = t_next - t_i
        if sample_duration <= 0:
            continue

        active: list[dict] = []
        for ev in events:
            ev_pts = int(ev.pts_90khz * timescale / 90000)
            ev_dur = 0
            if ev.duration_90khz and ev.duration_90khz > 0:
                ev_dur = int(ev.duration_90khz * timescale / 90000)

            pre_roll_ticks = int(ev.pre_roll_90khz * timescale / 90000)
            pre_roll_start = max(seg_start, ev_pts - pre_roll_ticks) if pre_roll_ticks > 0 else ev_pts
            is_visible = pre_roll_start <= t_i

            paired_deactivation = deactivation_map.get(id(ev))

            if ev_dur > 0:
                is_active = ev_pts + ev_dur > t_i
            elif paired_deactivation is not None:
                is_active = paired_deactivation > t_i
            else:
                is_active = True

            if is_visible and is_active:
                scte35_bytes = encode_splice_info_section(ev)
                active.append(
                    {
                        "presentation_time_delta": ev_pts - t_i,
                        "event_duration": ev_dur,
                        "event_id": ev.splice_event_id,
                        "message_data": scte35_bytes,
                    }
                )

        samples.append(NHMLSample(dts=t_i, duration=sample_duration, emib_list=active))

    return samples


def generate_nhml(
    events: list[SpliceEvent],
    timescale: int,
    total_duration_ticks: int,
    seg_duration_seconds: float,
    track_id: int,
) -> str:
    seg_ticks = round(seg_duration_seconds * timescale)

    segments: list[tuple[int, int]] = []
    pos = 0
    while pos < total_duration_ticks:
        seg_end = min(pos + seg_ticks, total_duration_ticks)
        segments.append((pos, seg_end))
        pos = seg_end

    all_samples: list[NHMLSample] = []
    for seg_start, seg_end in segments:
        seg_samples = compute_samples(seg_start, seg_end, events, timescale)
        all_samples.extend(seg_samples)

    root = ET.Element("NHNTStream")
    root.set("version", "1.0")
    root.set("timeScale", str(timescale))
    root.set("streamType", "Metadata")
    root.set("codecID", "evte")
    root.set("trackID", str(track_id))

    for sample in all_samples:
        sample_el = ET.SubElement(root, "NHNTSample")
        sample_el.set("DTS", str(sample.dts))
        sample_el.set("duration", str(sample.duration))
        sample_el.set("isRAP", "yes")

        if not sample.emib_list:
            ET.SubElement(sample_el, "EventMessageEmptyBox")
        else:
            for emib in sample.emib_list:
                emib_el = ET.SubElement(sample_el, "EventMessageInstanceBox")
                emib_el.set("presentation_time_delta", str(emib["presentation_time_delta"]))
                emib_el.set("event_duration", str(emib["event_duration"]))
                emib_el.set("event_id", str(emib["event_id"]))
                emib_el.set("scheme_id_uri", SCTE35_SCHEME_URI)
                emib_el.set("value", "")
                hex_data = "0x" + emib["message_data"].hex().upper()
                emib_el.set("message_data", hex_data)

    ET.indent(root, space="")
    xml_declaration = '<?xml version="1.0" encoding="UTF-8" ?>\n'
    tree_str = ET.tostring(root, encoding="unicode")
    return xml_declaration + tree_str + "\n"


def probe_video(video_path: str) -> tuple[int, int]:
    ffprobe_bin = _require_ffprobe()
    cmd = [
        ffprobe_bin, "-v", "quiet",
        "-select_streams", "v:0",
        "-show_entries", "stream=time_base,duration",
        "-of", "json", video_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired:
        print("Error: ffprobe timed out", file=sys.stderr)
        sys.exit(1)

    data = json.loads(result.stdout)
    streams = data.get("streams", [])
    if not streams:
        print("Error: No video stream found", file=sys.stderr)
        sys.exit(1)

    stream = streams[0]
    time_base = stream.get("time_base", "")
    parts = time_base.split("/")
    if len(parts) != 2 or parts[0] != "1":
        print(f"Error: Unexpected time_base format: {time_base}", file=sys.stderr)
        sys.exit(1)
    timescale = int(parts[1])

    duration_sec = float(stream.get("duration", 0))
    if duration_sec <= 0:
        print("Error: Could not determine video duration", file=sys.stderr)
        sys.exit(1)
    duration_ticks = round(duration_sec * timescale)

    return timescale, duration_ticks


SCHEDULE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://ffmpeg.org/schema/scte35_schedule.json",
    "title": "SCTE-35 Splice Schedule",
    "description": "Schedule format for the scte35_nhml_gen tool and FFmpeg scte35_inject BSF. Defines splice events per ANSI/SCTE 35.",
    "type": "object",
    "required": ["version", "events"],
    "additionalProperties": False,
    "properties": {
        "version": {
            "type": "integer",
            "const": 1,
            "description": "Schema version. Must be 1.",
        },
        "pre_roll": {
            "oneOf": [
                {"type": "number", "minimum": 0, "description": "Pre-roll in 90 kHz ticks."},
                {"type": "string", "description": "Pre-roll as time string (e.g. '300ms', '0.5s')."},
            ],
            "description": "Default pre-roll for all events. Events appear in samples before their splice time, giving clients advance notice via positive presentation_time_delta (ISO 23001-18 §7.4). Per-event pre_roll overrides this.",
        },
        "events": {
            "type": "array",
            "description": "Ordered list of splice events.",
            "items": {"$ref": "#/$defs/event"},
        },
    },
    "$defs": {
        "event": {
            "type": "object",
            "description": "A single SCTE-35 splice event.",
            "required": ["command"],
            "additionalProperties": False,
            "oneOf": [
                {"required": ["time"], "not": {"required": ["pts"]}},
                {"required": ["pts"], "not": {"required": ["time"]}},
            ],
            "properties": {
                "time": {
                    "type": "string",
                    "description": "Splice point as a time offset from stream start. Accepts HH:MM:SS, seconds, or suffixed (30s, 200ms). Mutually exclusive with 'pts'.",
                    "examples": ["00:05:00", "00:00:30.500", "120"],
                },
                "pts": {
                    "type": "number",
                    "description": "Splice point as absolute PTS in 90 kHz ticks. Mutually exclusive with 'time'.",
                    "minimum": 0,
                    "examples": [27000000, 8100000],
                },
                "command": {
                    "type": "string",
                    "enum": ["splice_insert", "time_signal"],
                    "description": "SCTE-35 command type. 'splice_insert' (0x05) or 'time_signal' (0x06). time_signal requires 'segmentation'.",
                },
                "splice_event_id": {
                    "type": "integer",
                    "description": "32-bit unique splice event identifier. Auto-assigned if omitted.",
                    "minimum": 0,
                    "maximum": 4294967295,
                },
                "out_of_network": {
                    "type": "boolean",
                    "description": "true = ad break start, false = return to network.",
                    "default": True,
                },
                "duration": {
                    "oneOf": [
                        {"type": "number", "minimum": 0, "description": "Break duration in 90 kHz ticks."},
                        {"type": "string", "description": "Break duration as time string (e.g. '00:00:30', '30s')."},
                    ],
                    "description": "Duration of the commercial break.",
                },
                "auto_return": {
                    "type": "boolean",
                    "description": "Auto-return to network after duration expires.",
                    "default": False,
                },
                "segmentation": {
                    "$ref": "#/$defs/segmentation",
                    "description": "Segmentation descriptor (SCTE 35 §10.3.3). Required for time_signal.",
                },
                "pre_roll": {
                    "oneOf": [
                        {"type": "number", "minimum": 0, "description": "Pre-roll in 90 kHz ticks."},
                        {"type": "string", "description": "Pre-roll as time string (e.g. '300ms', '0.5s')."},
                    ],
                    "description": "Per-event pre-roll. Overrides global pre_roll. The event appears in samples before its splice time with a positive presentation_time_delta.",
                },
            },
            "if": {
                "properties": {"command": {"const": "time_signal"}},
                "required": ["command"],
            },
            "then": {"required": ["segmentation"]},
        },
        "segmentation": {
            "type": "object",
            "description": "SCTE-35 segmentation_descriptor() per §10.3.3.",
            "additionalProperties": False,
            "properties": {
                "segmentation_event_id": {
                    "type": "integer",
                    "description": "32-bit unique segmentation event identifier. Defaults to splice_event_id.",
                    "minimum": 0,
                    "maximum": 4294967295,
                },
                "segmentation_type_id": {
                    "type": "integer",
                    "description": "Segmentation type per SCTE 35 Table 23.",
                    "minimum": 0,
                    "maximum": 255,
                    "enum": [
                        0, 1, 2,
                        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                        32, 33, 34, 35, 36, 37, 38, 39,
                        48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
                        64, 65, 66, 67, 68, 69, 70, 71,
                        80, 81,
                    ],
                },
                "segmentation_duration": {
                    "oneOf": [
                        {"type": "number", "minimum": 0, "description": "Duration in 90 kHz ticks."},
                        {"type": "string", "description": "Duration as time string (e.g. '30s')."},
                    ],
                    "description": "Segment duration.",
                },
                "upid_type": {
                    "type": "integer",
                    "description": "UPID type from SCTE 35 Table 22.",
                    "minimum": 0,
                    "maximum": 255,
                },
                "upid": {
                    "type": "string",
                    "description": "Unique Program Identifier value.",
                },
            },
        },
    },
}


CODEC_TO_ENCODER = {
    "h264": "libx264",
    "hevc": "libx265",
    "h265": "libx265",
    "av1": "libsvtav1",
}


@dataclass
class VideoInfo:
    codec_name: str
    width: int
    height: int
    duration: float
    frame_rate: str
    keyframe_pts: list[float]
    streams: list[dict]
    timescale: int
    duration_ticks: int


def _require_ffprobe() -> str:
    ffprobe_bin = shutil.which("ffprobe")
    if ffprobe_bin is None:
        print("Error: ffprobe not found in PATH. Install FFmpeg from https://ffmpeg.org", file=sys.stderr)
        sys.exit(1)
    return ffprobe_bin


def probe_video_detailed(video_path: str) -> VideoInfo:
    ffprobe_bin = _require_ffprobe()

    streams_cmd = [
        ffprobe_bin, "-v", "quiet",
        "-show_entries", "stream=index,codec_name,codec_type,width,height,r_frame_rate,duration",
        "-of", "json", video_path,
    ]
    result = subprocess.run(streams_cmd, capture_output=True, text=True, timeout=10)
    streams_data = json.loads(result.stdout)
    all_streams = streams_data.get("streams", [])

    video_stream = None
    for s in all_streams:
        if s.get("codec_type") == "video":
            video_stream = s
            break

    if video_stream is None:
        print("Error: No video stream found", file=sys.stderr)
        sys.exit(1)

    keyframes_cmd = [
        ffprobe_bin, "-v", "quiet",
        "-select_streams", "v:0",
        "-skip_frame", "nokey",
        "-show_entries", "frame=pts_time",
        "-of", "json", video_path,
    ]
    result = subprocess.run(keyframes_cmd, capture_output=True, text=True, timeout=120)
    frames_data = json.loads(result.stdout)
    keyframe_pts = [
        float(f["pts_time"])
        for f in frames_data.get("frames", [])
        if "pts_time" in f
    ]

    duration = float(video_stream.get("duration", 0))
    if duration == 0 and keyframe_pts:
        duration = keyframe_pts[-1]

    timescale, duration_ticks = probe_video(video_path)

    return VideoInfo(
        codec_name=video_stream.get("codec_name", "unknown"),
        width=int(video_stream.get("width", 0)),
        height=int(video_stream.get("height", 0)),
        duration=duration,
        frame_rate=video_stream.get("r_frame_rate", "0/1"),
        keyframe_pts=sorted(keyframe_pts),
        streams=all_streams,
        timescale=timescale,
        duration_ticks=duration_ticks,
    )


def find_nearest_keyframe(keyframe_pts: list[float], target: float) -> tuple[float | None, float]:
    if not keyframe_pts:
        return None, float("inf")
    best = None
    best_delta = float("inf")
    for kf in keyframe_pts:
        delta = abs(kf - target)
        if delta < best_delta:
            best = kf
            best_delta = delta
        elif kf > target + best_delta:
            break
    return best, best_delta


def _format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.3f}s"
    m, s = divmod(seconds, 60)
    h, m = divmod(int(m), 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:06.3f}"
    return f"{int(m)}:{s:06.3f}"


def build_ffmpeg_command(
    video_path: str,
    info: VideoInfo,
    force_keyframe_times: list[float],
    output_path: str | None = None,
) -> str:
    encoder = CODEC_TO_ENCODER.get(info.codec_name, "libx264")
    out = output_path or video_path.rsplit(".", 1)[0] + "_spliceready." + video_path.rsplit(".", 1)[-1]

    kf_expr = ",".join(f"{t:.3f}" for t in sorted(set(force_keyframe_times)))

    parts = ["ffmpeg", "-i", video_path, "-c:v", encoder]
    parts.extend(["-force_key_frames", f'"{kf_expr}"'])
    parts.extend(["-c:a", "copy"])
    parts.append(out)

    return " ".join(parts)


def collect_splice_boundaries(
    events: list[SpliceEvent], video_duration: float
) -> list[tuple[float, str]]:
    boundaries: list[tuple[float, str]] = []
    for ev in events:
        pts_sec = ev.pts_90khz / 90000
        boundaries.append((pts_sec, f"{ev.command} start (id={ev.splice_event_id})"))
        dur_90khz = ev.duration_90khz
        if not dur_90khz and ev.segmentation and ev.segmentation.segmentation_duration_90khz:
            dur_90khz = ev.segmentation.segmentation_duration_90khz
        if dur_90khz and dur_90khz > 0:
            end_sec = pts_sec + dur_90khz / 90000
            if end_sec <= video_duration:
                boundaries.append((end_sec, f"{ev.command} end (id={ev.splice_event_id})"))
    return boundaries


def _load_and_parse_schedule(schedule_path: str) -> tuple[dict, list[SpliceEvent]]:
    try:
        with open(schedule_path) as f:
            schedule_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Schedule file not found: {schedule_path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in schedule file: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        events = parse_schedule(schedule_data)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    return schedule_data, events


def cmd_generate(args: argparse.Namespace) -> None:
    _, events = _load_and_parse_schedule(args.schedule)
    timescale, total_duration_ticks = probe_video(args.video)

    valid_events = []
    for ev in events:
        ev_pts_ticks = int(ev.pts_90khz * timescale / 90000)
        if ev_pts_ticks >= total_duration_ticks:
            print(
                f"Warning: Event {ev.splice_event_id} at PTS {ev.pts_90khz} "
                f"is beyond video duration, skipping",
                file=sys.stderr,
            )
        else:
            valid_events.append(ev)

    nhml = generate_nhml(
        events=valid_events,
        timescale=timescale,
        total_duration_ticks=total_duration_ticks,
        seg_duration_seconds=args.seg_duration,
        track_id=args.track_id,
    )

    if args.o:
        with open(args.o, "w") as f:
            f.write(nhml)
        print(f"Wrote {args.o}", file=sys.stderr)
        print(
            f"\nMux with GPAC:\n"
            f"  gpac -i {args.video} -i {args.o}:#ID={args.track_id} \\\n"
            f"    -o output/manifest.mpd:profile=live:cmaf=cmf2:segdur={args.seg_duration:g}",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(nhml)


def _frame_duration(frame_rate: str) -> float:
    parts = frame_rate.split("/")
    if len(parts) == 2:
        num, den = float(parts[0]), float(parts[1])
        if num > 0:
            return den / num
    try:
        fps = float(frame_rate)
        if fps > 0:
            return 1.0 / fps
    except ValueError:
        pass
    return 1.0 / 24.0


def cmd_validate(args: argparse.Namespace) -> None:
    events: list[SpliceEvent] = []
    schedule_data: dict = {}
    if args.schedule:
        schedule_data, events = _load_and_parse_schedule(args.schedule)
        global_pre_roll = schedule_data.get("pre_roll")
        if global_pre_roll is not None:
            print(f"Schedule parsed: {len(events)} event(s), global pre_roll={global_pre_roll}", file=sys.stderr)
        else:
            print(f"Schedule parsed: {len(events)} event(s)", file=sys.stderr)
        for i, ev in enumerate(events):
            pts_sec = ev.pts_90khz / 90000
            dur_str = ""
            if ev.duration_90khz:
                dur_str = f", duration={ev.duration_90khz / 90000:.3f}s"
            seg_str = ""
            if ev.segmentation:
                seg_type = ev.segmentation.segmentation_type_id
                if seg_type is not None:
                    seg_name = SEGMENTATION_TYPE_NAMES.get(seg_type, "Unknown")
                    seg_str = f", {seg_name} (0x{seg_type:02X})"
            pre_roll_str = ""
            if ev.pre_roll_90khz > 0:
                pre_roll_str = f", pre_roll={ev.pre_roll_90khz / 90000 * 1000:.0f}ms"
            print(
                f"  [{i}] {ev.command} at {pts_sec:.3f}s (id={ev.splice_event_id}{dur_str}{seg_str}{pre_roll_str})",
                file=sys.stderr,
            )

    if not args.video:
        if events:
            print("\nSchedule is valid.", file=sys.stderr)
        else:
            print("Error: provide a schedule file, a video file, or both", file=sys.stderr)
            sys.exit(1)
        return

    info = probe_video_detailed(args.video)
    seg_dur = args.seg_duration
    tolerance = _frame_duration(info.frame_rate)

    print(f"\nVideo: {args.video}", file=sys.stderr)
    print(f"  Codec: {info.codec_name} ({info.width}x{info.height})", file=sys.stderr)
    print(f"  Frame rate: {info.frame_rate}", file=sys.stderr)
    print(f"  Duration: {_format_time(info.duration)}", file=sys.stderr)
    print(f"  Timescale: {info.timescale}", file=sys.stderr)
    print(f"  Keyframes: {len(info.keyframe_pts)} found", file=sys.stderr)
    print(file=sys.stderr)

    print("Keyframe positions (valid splice points):", file=sys.stderr)
    for kf in info.keyframe_pts:
        print(f"  {_format_time(kf)}", file=sys.stderr)
    print(file=sys.stderr)

    problems: list[str] = []
    required_keyframe_times: list[float] = []

    if events:
        splice_boundaries = collect_splice_boundaries(events, info.duration)

        print("Splice point alignment:", file=sys.stderr)
        for boundary_sec, label in splice_boundaries:
            nearest, delta = find_nearest_keyframe(info.keyframe_pts, boundary_sec)
            if nearest is not None and delta <= tolerance:
                print(f"  {_format_time(boundary_sec):>12s}  OK  {label} — keyframe at {_format_time(nearest)} (delta: {delta*1000:.0f}ms)", file=sys.stderr)
            else:
                required_keyframe_times.append(boundary_sec)
                if nearest is not None:
                    problems.append(f"{label} at {_format_time(boundary_sec)}: nearest keyframe is {delta*1000:.0f}ms away")
                    print(f"  {_format_time(boundary_sec):>12s}  !!  {label} — nearest keyframe at {_format_time(nearest)} (delta: {delta*1000:.0f}ms)", file=sys.stderr)
                else:
                    problems.append(f"{label} at {_format_time(boundary_sec)}: no keyframes found")
                    print(f"  {_format_time(boundary_sec):>12s}  !!  {label} — no keyframes found", file=sys.stderr)
        print(file=sys.stderr)

    print(f"Segment boundary alignment (seg_duration={seg_dur}s):", file=sys.stderr)
    num_segs = int(info.duration / seg_dur) + 1
    for i in range(num_segs):
        boundary = i * seg_dur
        if boundary > info.duration:
            break
        nearest, delta = find_nearest_keyframe(info.keyframe_pts, boundary)
        if nearest is not None and delta <= tolerance:
            print(f"  {_format_time(boundary):>12s}  OK  keyframe at {_format_time(nearest)} (delta: {delta*1000:.0f}ms)", file=sys.stderr)
        else:
            required_keyframe_times.append(boundary)
            if nearest is not None:
                problems.append(f"Segment at {_format_time(boundary)}: nearest keyframe is {delta*1000:.0f}ms away")
                print(f"  {_format_time(boundary):>12s}  !!  nearest keyframe at {_format_time(nearest)} (delta: {delta*1000:.0f}ms)", file=sys.stderr)
            else:
                problems.append(f"Segment at {_format_time(boundary)}: no keyframes found")
                print(f"  {_format_time(boundary):>12s}  !!  no keyframes found", file=sys.stderr)

    print(file=sys.stderr)
    if not problems:
        print("All splice points and segment boundaries are aligned with keyframes.", file=sys.stderr)
        if args.schedule and args.video:
            nhml_out = args.video.rsplit(".", 1)[0] + "_events.nhml"
            parts = ['uv', 'run', sys.argv[0], "generate", args.schedule, args.video]
            if seg_dur != 6.0:
                parts.extend(["-s", str(seg_dur)])
            parts.extend(["-o", nhml_out])
            print(f"\nGenerate NHML:\n  {' '.join(parts)}", file=sys.stderr)
    else:
        print(f"Found {len(problems)} alignment issue(s):", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        print(file=sys.stderr)

        seg_boundaries = [i * seg_dur for i in range(num_segs) if i * seg_dur <= info.duration]
        splice_times = [t for t, _ in splice_boundaries]
        all_forced = sorted(set(required_keyframe_times + seg_boundaries + splice_times))
        cmd = build_ffmpeg_command(args.video, info, all_forced)
        print("Video needs re-encoding with forced keyframes. Suggested command:", file=sys.stderr)
        print(f"  {cmd}", file=sys.stderr)
        sys.exit(1)


def cmd_schema(_args: argparse.Namespace) -> None:
    print(json.dumps(SCHEDULE_SCHEMA, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    gen = subparsers.add_parser(
        "generate",
        help="Generate NHML from JSON schedule and video file",
        description="Generate SCTE-35 event message track (NHML) from a JSON schedule.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    gen.add_argument("schedule", help="Path to JSON schedule file")
    gen.add_argument("video", help="Input video file (probed for timescale + duration)")
    gen.add_argument("-s", "--seg-duration", type=float, default=6.0, help="Segment duration in seconds (default: 6)")
    gen.add_argument("-t", "--track-id", type=int, default=3, help="Track ID for the event track (default: 3)")
    gen.add_argument("-o", type=str, default=None, help="Output NHML file path (default: stdout)")
    gen.set_defaults(func=cmd_generate)

    val = subparsers.add_parser(
        "validate",
        help="Validate schedule and/or video keyframe alignment",
        description=(
            "Validate a JSON schedule, a video file, or both.\n\n"
            "  Video only:    validate video.mp4 -s 6    (analyze keyframes)\n"
            "  Schedule only: validate -i events.json     (parse check)\n"
            "  Both:          validate -i events.json video.mp4 -s 6"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    val.add_argument("video", nargs="?", default=None, help="Video file to check keyframe alignment")
    val.add_argument("-i", "--schedule", default=None, dest="schedule", help="JSON schedule file to validate")
    val.add_argument("-s", "--seg-duration", type=float, default=6.0, help="Segment duration in seconds (default: 6)")
    val.set_defaults(func=cmd_validate)

    sch = subparsers.add_parser(
        "schema",
        help="Print the JSON schema for the schedule format",
        description="Output the JSON Schema (draft 2020-12) for the schedule file format.",
    )
    sch.set_defaults(func=cmd_schema)

    # Backwards compat: if first arg isn't a subcommand, treat as "generate"
    known_commands = {"generate", "validate", "schema", "-h", "--help"}
    if len(sys.argv) > 1 and sys.argv[1] not in known_commands:
        sys.argv.insert(1, "generate")

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
