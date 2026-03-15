# /// script
# requires-python = ">=3.12"
# dependencies = ["pytest"]
# ///
from __future__ import annotations

import json
import struct
import xml.etree.ElementTree as ET

import pytest

from scte35_nhml_gen import (
    CODEC_TO_ENCODER,
    BitWriter,
    NHMLSample,
    SegmentationInfo,
    SpliceEvent,
    VideoInfo,
    build_ffmpeg_command,
    collect_splice_boundaries,
    compute_samples,
    crc32_mpeg2,
    encode_break_duration,
    encode_segmentation_descriptor,
    encode_splice_info_section,
    encode_splice_insert,
    encode_splice_time,
    encode_time_signal,
    find_nearest_keyframe,
    generate_nhml,
    parse_duration_field,
    parse_schedule,
    parse_time_string,
)


# ─── BitWriter Tests ───


class TestBitWriter:
    def test_write_full_bytes(self):
        w = BitWriter()
        w.write_bits(0xFC, 8)
        assert w.flush() == bytes([0xFC])

    def test_write_partial_bits(self):
        w = BitWriter()
        w.write_bits(0b101, 3)
        result = w.flush()
        assert result == bytes([0b10100000])

    def test_write_33_bit_value(self):
        w = BitWriter()
        w.write_bits(1, 1)
        w.write_bits(0x3F, 6)
        w.write_bits(900000, 33)
        result = w.flush()
        assert len(result) == 5

    def test_write_mixed_bits_and_bytes(self):
        w = BitWriter()
        w.write_bits(0xFF, 8)
        w.write_bytes(b"\xAB\xCD")
        result = w.flush()
        assert result == bytes([0xFF, 0xAB, 0xCD])

    def test_write_bytes_unaligned_raises(self):
        w = BitWriter()
        w.write_bits(1, 1)
        with pytest.raises(AssertionError):
            w.write_bytes(b"\x00")

    def test_byte_length(self):
        w = BitWriter()
        assert w.byte_length == 0
        w.write_bits(0xFF, 8)
        assert w.byte_length == 1
        w.write_bits(1, 1)
        assert w.byte_length == 2

    def test_12_bit_field(self):
        w = BitWriter()
        w.write_bits(0xABC, 12)
        result = w.flush()
        assert result == bytes([0xAB, 0xC0])


# ─── CRC-32 Tests ───


class TestCRC32:
    def test_empty(self):
        assert crc32_mpeg2(b"") == 0xFFFFFFFF

    def test_known_value(self):
        result = crc32_mpeg2(b"\xFC")
        assert isinstance(result, int)
        assert 0 <= result <= 0xFFFFFFFF

    def test_deterministic(self):
        data = b"\xFC\x30\x11\x00\x00\x00\x00\x00\x00\x00\xFF\xFF\xFF\x00\x00"
        assert crc32_mpeg2(data) == crc32_mpeg2(data)

    def test_different_inputs_differ(self):
        assert crc32_mpeg2(b"\x00") != crc32_mpeg2(b"\x01")


# ─── splice_time() Tests ───


class TestSpliceTime:
    def test_basic_pts(self):
        w = BitWriter()
        encode_splice_time(w, 900000)
        result = w.flush()
        assert len(result) == 5
        assert result[0] & 0x80 == 0x80  # time_specified_flag = 1
        assert result[0] & 0x7E == 0x7E  # reserved bits = 0x3F

    def test_zero_pts(self):
        w = BitWriter()
        encode_splice_time(w, 0)
        result = w.flush()
        assert len(result) == 5

    def test_max_pts(self):
        w = BitWriter()
        encode_splice_time(w, 0x1FFFFFFFF)
        result = w.flush()
        assert len(result) == 5


# ─── break_duration() Tests ───


class TestBreakDuration:
    def test_with_auto_return(self):
        w = BitWriter()
        encode_break_duration(w, True, 2700000)
        result = w.flush()
        assert len(result) == 5
        assert result[0] & 0x80 == 0x80  # auto_return = 1

    def test_without_auto_return(self):
        w = BitWriter()
        encode_break_duration(w, False, 2700000)
        result = w.flush()
        assert len(result) == 5
        assert result[0] & 0x80 == 0x00  # auto_return = 0

    def test_reserved_bits(self):
        w = BitWriter()
        encode_break_duration(w, True, 0)
        result = w.flush()
        assert result[0] & 0x7E == 0x7E  # reserved = 0x3F


# ─── splice_insert() Tests ───


class TestSpliceInsert:
    def test_basic_splice_insert(self):
        event = SpliceEvent(
            pts_90khz=900000,
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=2700000,
            auto_return=True,
            segmentation=None,
        )
        result = encode_splice_insert(event)
        assert len(result) > 0
        assert result[0:4] == struct.pack(">I", 1)  # splice_event_id

    def test_splice_insert_no_duration(self):
        event = SpliceEvent(
            pts_90khz=900000,
            command="splice_insert",
            splice_event_id=42,
            out_of_network=False,
            duration_90khz=None,
            auto_return=False,
            segmentation=None,
        )
        result = encode_splice_insert(event)
        assert result[0:4] == struct.pack(">I", 42)

    def test_splice_insert_reserved_bits(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=None,
        )
        result = encode_splice_insert(event)
        assert result[4] == 0x7F  # cancel=0 + reserved 0x7F


# ─── time_signal() Tests ───


class TestTimeSignal:
    def test_basic(self):
        result = encode_time_signal(900000)
        assert len(result) == 5


# ─── segmentation_descriptor() Tests ───


class TestSegmentationDescriptor:
    def test_basic_descriptor(self):
        event = SpliceEvent(
            pts_90khz=900000,
            command="time_signal",
            splice_event_id=100,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=100,
                segmentation_type_id=52,
                segmentation_duration_90khz=2700000,
                upid_type=9,
                upid="SIGNAL:abc123",
            ),
        )
        result = encode_segmentation_descriptor(event)
        assert result[0] == 0x02  # splice_descriptor_tag
        assert result[2:6] == b"CUEI"  # identifier

    def test_no_segmentation(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="time_signal",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=None,
        )
        assert encode_segmentation_descriptor(event) == b""

    def test_sub_segment_type_ids(self):
        for type_id in [0x30, 0x32, 0x34, 0x36, 0x38, 0x3A, 0x44, 0x46]:
            event = SpliceEvent(
                pts_90khz=0,
                command="time_signal",
                splice_event_id=1,
                out_of_network=True,
                duration_90khz=None,
                auto_return=False,
                segmentation=SegmentationInfo(
                    segmentation_event_id=1,
                    segmentation_type_id=type_id,
                    segmentation_duration_90khz=None,
                    upid_type=0,
                    upid=None,
                ),
            )
            result = encode_segmentation_descriptor(event)
            assert len(result) > 0

    def test_no_duration(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="time_signal",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=1,
                segmentation_type_id=48,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        result = encode_segmentation_descriptor(event)
        assert len(result) > 0

    def test_with_upid(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="time_signal",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=1,
                segmentation_type_id=52,
                segmentation_duration_90khz=None,
                upid_type=9,
                upid="provider.com/MOVE1234567890123456",
            ),
        )
        result = encode_segmentation_descriptor(event)
        assert b"provider.com/MOVE1234567890123456" in result


# ─── splice_info_section() Tests ───


class TestSpliceInfoSection:
    def test_splice_insert_section(self):
        event = SpliceEvent(
            pts_90khz=900000,
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=2700000,
            auto_return=True,
            segmentation=None,
        )
        result = encode_splice_info_section(event)
        assert result[0] == 0xFC  # table_id
        crc_in_data = struct.unpack(">I", result[-4:])[0]
        crc_computed = crc32_mpeg2(result[:-4])
        assert crc_in_data == crc_computed

    def test_time_signal_section(self):
        event = SpliceEvent(
            pts_90khz=900000,
            command="time_signal",
            splice_event_id=100,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=100,
                segmentation_type_id=52,
                segmentation_duration_90khz=2700000,
                upid_type=9,
                upid="SIGNAL:abc123",
            ),
        )
        result = encode_splice_info_section(event)
        assert result[0] == 0xFC
        crc_in_data = struct.unpack(">I", result[-4:])[0]
        crc_computed = crc32_mpeg2(result[:-4])
        assert crc_in_data == crc_computed

    def test_section_length_field(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=None,
        )
        result = encode_splice_info_section(event)
        section_length_bits = ((result[1] & 0x0F) << 8) | result[2]
        assert section_length_bits == len(result) - 3

    def test_reserved_bits_in_header(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=None,
        )
        result = encode_splice_info_section(event)
        # sap_type = 0x3 in bits [2:3] of byte 1
        assert (result[1] >> 4) & 0x03 == 0x03

    def test_unknown_command_raises(self):
        event = SpliceEvent(
            pts_90khz=0,
            command="unknown",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=None,
        )
        with pytest.raises(ValueError, match="Unknown command"):
            encode_splice_info_section(event)

    def test_crc_validates_whole_section(self):
        event = SpliceEvent(
            pts_90khz=12345,
            command="splice_insert",
            splice_event_id=7,
            out_of_network=True,
            duration_90khz=900000,
            auto_return=True,
            segmentation=None,
        )
        result = encode_splice_info_section(event)
        full_crc = crc32_mpeg2(result)
        assert full_crc == 0


# ─── Time Parsing Tests ───


class TestParseTimeString:
    def test_plain_seconds(self):
        assert parse_time_string("30") == 30.0
        assert parse_time_string("30.5") == 30.5
        assert parse_time_string("0.2") == pytest.approx(0.2)

    def test_hhmmss(self):
        assert parse_time_string("1:30:45") == 5445.0
        assert parse_time_string("0:00:30") == 30.0

    def test_mmss(self):
        assert parse_time_string("30:45") == 1845.0

    def test_suffixed(self):
        assert parse_time_string("200ms") == pytest.approx(0.2)
        assert parse_time_string("30s") == 30.0
        assert parse_time_string("1000us") == pytest.approx(0.001)

    def test_zero(self):
        assert parse_time_string("0") == 0.0
        assert parse_time_string("0.0") == 0.0

    def test_whitespace_stripped(self):
        assert parse_time_string("  30  ") == 30.0

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            parse_time_string("")

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_time_string("abc")

    def test_too_many_colons_raises(self):
        with pytest.raises(ValueError):
            parse_time_string("1:2:3:4")


class TestParseDurationField:
    def test_numeric_int(self):
        assert parse_duration_field(2700000) == 2700000

    def test_numeric_float(self):
        assert parse_duration_field(2700000.0) == 2700000

    def test_string_seconds(self):
        assert parse_duration_field("30s") == 2700000

    def test_string_plain(self):
        assert parse_duration_field("30") == 2700000

    def test_invalid_type(self):
        with pytest.raises(ValueError):
            parse_duration_field([1, 2, 3])


# ─── JSON Schedule Parsing Tests ───


class TestParseSchedule:
    def test_basic_splice_insert(self):
        data = {
            "version": 1,
            "events": [
                {
                    "time": "30",
                    "command": "splice_insert",
                    "splice_event_id": 1,
                    "out_of_network": True,
                    "duration": "30s",
                    "auto_return": True,
                }
            ],
        }
        events = parse_schedule(data)
        assert len(events) == 1
        assert events[0].pts_90khz == 2700000
        assert events[0].command == "splice_insert"
        assert events[0].splice_event_id == 1
        assert events[0].duration_90khz == 2700000

    def test_pts_addressing(self):
        data = {
            "version": 1,
            "events": [{"pts": 720000, "command": "splice_insert", "splice_event_id": 1}],
        }
        events = parse_schedule(data)
        assert events[0].pts_90khz == 720000

    def test_auto_increment_id(self):
        data = {
            "version": 1,
            "events": [
                {"time": "10", "command": "splice_insert"},
                {"time": "20", "command": "splice_insert"},
                {"time": "30", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(data)
        assert [e.splice_event_id for e in events] == [1, 2, 3]

    def test_time_signal_with_segmentation(self):
        data = {
            "version": 1,
            "events": [
                {
                    "time": "60",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 100,
                        "segmentation_type_id": 52,
                        "segmentation_duration": "30s",
                        "upid_type": 9,
                        "upid": "SIGNAL:abc123",
                    },
                }
            ],
        }
        events = parse_schedule(data)
        assert events[0].segmentation is not None
        assert events[0].segmentation.segmentation_event_id == 100
        assert events[0].segmentation.segmentation_type_id == 52

    def test_time_signal_without_segmentation_warns(self, capsys):
        data = {
            "version": 1,
            "events": [{"time": "10", "command": "time_signal"}],
        }
        events = parse_schedule(data)
        assert len(events) == 1
        captured = capsys.readouterr()
        assert "Warning" in captured.err

    def test_sorted_by_pts(self):
        data = {
            "version": 1,
            "events": [
                {"time": "30", "command": "splice_insert"},
                {"time": "10", "command": "splice_insert"},
                {"time": "20", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(data)
        assert events[0].pts_90khz < events[1].pts_90khz < events[2].pts_90khz

    def test_both_time_and_pts_raises(self):
        data = {
            "version": 1,
            "events": [{"time": "10", "pts": 900000, "command": "splice_insert"}],
        }
        with pytest.raises(ValueError, match="mutually exclusive"):
            parse_schedule(data)

    def test_neither_time_nor_pts_raises(self):
        data = {
            "version": 1,
            "events": [{"command": "splice_insert"}],
        }
        with pytest.raises(ValueError, match="must have"):
            parse_schedule(data)

    def test_unknown_command_raises(self):
        data = {
            "version": 1,
            "events": [{"time": "10", "command": "splice_null"}],
        }
        with pytest.raises(ValueError, match="unknown command"):
            parse_schedule(data)

    def test_wrong_version_raises(self):
        data = {"version": 2, "events": []}
        with pytest.raises(ValueError, match="version"):
            parse_schedule(data)

    def test_missing_events_raises(self):
        data = {"version": 1}
        with pytest.raises(ValueError, match="events"):
            parse_schedule(data)

    def test_segmentation_event_id_defaults_to_splice_event_id(self):
        data = {
            "version": 1,
            "events": [
                {
                    "time": "10",
                    "command": "time_signal",
                    "splice_event_id": 42,
                    "segmentation": {"segmentation_type_id": 48},
                }
            ],
        }
        events = parse_schedule(data)
        assert events[0].segmentation.segmentation_event_id == 42

    def test_duration_as_ticks(self):
        data = {
            "version": 1,
            "events": [
                {
                    "time": "10",
                    "command": "splice_insert",
                    "duration": 2700000,
                }
            ],
        }
        events = parse_schedule(data)
        assert events[0].duration_90khz == 2700000

    def test_empty_events_list(self):
        data = {"version": 1, "events": []}
        events = parse_schedule(data)
        assert len(events) == 0


# ─── Sample Boundary Algorithm Tests ───


class TestComputeSamples:
    def _make_event(self, pts_90khz=0, duration_90khz=None, event_id=1, command="splice_insert"):
        return SpliceEvent(
            pts_90khz=pts_90khz,
            command=command,
            splice_event_id=event_id,
            out_of_network=True,
            duration_90khz=duration_90khz,
            auto_return=True,
            segmentation=None,
        )

    def test_no_events(self):
        samples = compute_samples(0, 540000, [], 90000)
        assert len(samples) == 1
        assert samples[0].dts == 0
        assert samples[0].duration == 540000
        assert samples[0].emib_list == []

    def test_one_event_mid_segment(self):
        ev = self._make_event(pts_90khz=900000, duration_90khz=2700000)
        samples = compute_samples(540000, 1080000, [ev], 90000)

        assert len(samples) == 2
        assert samples[0].dts == 540000
        assert samples[0].duration == 360000  # 900000 - 540000
        assert samples[0].emib_list == []

        assert samples[1].dts == 900000
        assert samples[1].duration == 180000  # 1080000 - 900000
        assert len(samples[1].emib_list) == 1
        assert samples[1].emib_list[0]["presentation_time_delta"] == 0

    def test_event_carryover(self):
        ev = self._make_event(pts_90khz=900000, duration_90khz=2700000)
        samples = compute_samples(1080000, 1620000, [ev], 90000)

        assert len(samples) == 1
        assert samples[0].dts == 1080000
        assert len(samples[0].emib_list) == 1
        assert samples[0].emib_list[0]["presentation_time_delta"] == -180000

    def test_two_overlapping_events(self):
        ev_a = self._make_event(pts_90khz=180000, duration_90khz=450000, event_id=1)
        ev_b = self._make_event(pts_90khz=360000, duration_90khz=270000, event_id=2)
        samples = compute_samples(0, 900000, [ev_a, ev_b], 90000)

        assert len(samples) == 4
        # Sample 0: [0, 180000) → emeb
        assert samples[0].emib_list == []
        # Sample 1: [180000, 360000) → emib(A)
        assert len(samples[1].emib_list) == 1
        assert samples[1].emib_list[0]["event_id"] == 1
        # Sample 2: [360000, 630000) → emib(A) + emib(B)
        assert len(samples[2].emib_list) == 2
        # Sample 3: [630000, 900000) → emeb
        assert samples[3].emib_list == []

    def test_event_at_segment_start(self):
        ev = self._make_event(pts_90khz=540000, duration_90khz=270000)
        samples = compute_samples(540000, 1080000, [ev], 90000)

        assert samples[0].dts == 540000
        assert len(samples[0].emib_list) == 1
        assert samples[0].emib_list[0]["presentation_time_delta"] == 0

    def test_indefinite_duration(self):
        ev = self._make_event(pts_90khz=180000, duration_90khz=None)
        samples = compute_samples(0, 540000, [ev], 90000)

        assert len(samples) == 2
        # [0, 180000) → emeb
        assert samples[0].emib_list == []
        # [180000, 540000) → emib with event_duration=0
        assert len(samples[1].emib_list) == 1
        assert samples[1].emib_list[0]["event_duration"] == 0

    def test_event_ending_mid_segment(self):
        ev = self._make_event(pts_90khz=0, duration_90khz=270000)
        samples = compute_samples(0, 540000, [ev], 90000)

        assert len(samples) == 2
        # [0, 270000) → emib
        assert len(samples[0].emib_list) == 1
        # [270000, 540000) → emeb
        assert samples[1].emib_list == []

    def test_different_timescale(self):
        ev = self._make_event(pts_90khz=900000, duration_90khz=2700000)
        samples = compute_samples(0, 144000, [ev], 24000)

        # At 24000 timescale: event_pts = 900000 * 24000 / 90000 = 240000
        # seg_end = 144000, so event starts after segment ends
        assert len(samples) == 1
        assert samples[0].emib_list == []

    def test_empty_presentation(self):
        samples = compute_samples(0, 540000, [], 90000)
        assert len(samples) == 1
        assert samples[0].emib_list == []
        assert samples[0].duration == 540000

    def test_program_start_end_pairing(self):
        """ProgramStart(0x10) at 2s closed by ProgramEnd(0x11) at 7s, indefinite."""
        prog_start = SpliceEvent(
            pts_90khz=180000,
            command="time_signal",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=42,
                segmentation_type_id=0x10,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        prog_end = SpliceEvent(
            pts_90khz=630000,
            command="time_signal",
            splice_event_id=2,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=42,
                segmentation_type_id=0x11,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        samples = compute_samples(0, 900000, [prog_start, prog_end], 90000)

        # Boundaries: {0, 180000, 630000, 900000}
        assert len(samples) == 3

        # [0, 180000) → emeb
        assert samples[0].emib_list == []
        # [180000, 630000) → emib(prog_start active, indefinite until closed)
        assert len(samples[1].emib_list) == 1
        assert samples[1].emib_list[0]["event_id"] == 1
        # [630000, 900000) → prog_start deactivated by prog_end; prog_end active
        ids_at_630 = {e["event_id"] for e in samples[2].emib_list}
        assert 1 not in ids_at_630  # prog_start gone
        assert 2 in ids_at_630  # prog_end present

    def test_provider_placement_opportunity_pairing(self):
        """POStart(0x34) closed by POEnd(0x35)."""
        po_start = SpliceEvent(
            pts_90khz=0,
            command="time_signal",
            splice_event_id=10,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=99,
                segmentation_type_id=0x34,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        po_end = SpliceEvent(
            pts_90khz=270000,
            command="time_signal",
            splice_event_id=11,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=99,
                segmentation_type_id=0x35,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        samples = compute_samples(0, 540000, [po_start, po_end], 90000)

        # Boundaries: {0, 270000, 540000}
        assert len(samples) == 2
        # [0, 270000) → emib(po_start active)
        assert len(samples[0].emib_list) == 1
        assert samples[0].emib_list[0]["event_id"] == 10
        # [270000, 540000) → po_start deactivated, po_end is active
        assert all(e["event_id"] != 10 for e in samples[1].emib_list)

    def test_pairing_does_not_affect_unrelated_events(self):
        """A closing type only deactivates its matching open event."""
        splice = self._make_event(pts_90khz=0, duration_90khz=None, event_id=50)
        prog_end = SpliceEvent(
            pts_90khz=270000,
            command="time_signal",
            splice_event_id=2,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=42,
                segmentation_type_id=0x11,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        samples = compute_samples(0, 540000, [splice, prog_end], 90000)
        # splice has no segmentation so it won't be paired — it stays active forever
        # [270000, 540000) should still have splice active
        last = samples[-1]
        assert any(e["event_id"] == 50 for e in last.emib_list)

    def test_mismatched_seg_event_id_no_pairing(self):
        """Open and close with different segmentation_event_id don't pair."""
        prog_start = SpliceEvent(
            pts_90khz=0,
            command="time_signal",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=42,
                segmentation_type_id=0x10,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        prog_end = SpliceEvent(
            pts_90khz=270000,
            command="time_signal",
            splice_event_id=2,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=99,
                segmentation_type_id=0x11,
                segmentation_duration_90khz=None,
                upid_type=0,
                upid=None,
            ),
        )
        samples = compute_samples(0, 540000, [prog_start, prog_end], 90000)
        # prog_start should still be active after prog_end (different seg_event_id)
        last = samples[-1]
        assert any(e["event_id"] == 1 for e in last.emib_list)

    def test_program_start_ad_break_program_end(self):
        """Full scenario: ProgramStart → POStart → POEnd → ProgramEnd."""
        prog_start = SpliceEvent(
            pts_90khz=0, command="time_signal", splice_event_id=1,
            out_of_network=True, duration_90khz=None, auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=1, segmentation_type_id=0x10,
                segmentation_duration_90khz=None, upid_type=0, upid=None,
            ),
        )
        po_start = SpliceEvent(
            pts_90khz=270000, command="time_signal", splice_event_id=2,
            out_of_network=True, duration_90khz=None, auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=2, segmentation_type_id=0x34,
                segmentation_duration_90khz=None, upid_type=0, upid=None,
            ),
        )
        po_end = SpliceEvent(
            pts_90khz=540000, command="time_signal", splice_event_id=3,
            out_of_network=True, duration_90khz=None, auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=2, segmentation_type_id=0x35,
                segmentation_duration_90khz=None, upid_type=0, upid=None,
            ),
        )
        prog_end = SpliceEvent(
            pts_90khz=900000, command="time_signal", splice_event_id=4,
            out_of_network=True, duration_90khz=None, auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=1, segmentation_type_id=0x11,
                segmentation_duration_90khz=None, upid_type=0, upid=None,
            ),
        )
        events = [prog_start, po_start, po_end, prog_end]
        samples = compute_samples(0, 1080000, events, 90000)

        # At [0, 270000): prog_start active
        assert any(e["event_id"] == 1 for e in samples[0].emib_list)
        assert all(e["event_id"] != 2 for e in samples[0].emib_list)

        # At [270000, 540000): prog_start + po_start active
        s_at_270 = next(s for s in samples if s.dts == 270000)
        ids_at_270 = {e["event_id"] for e in s_at_270.emib_list}
        assert 1 in ids_at_270  # prog_start
        assert 2 in ids_at_270  # po_start

        # At [540000, 900000): po_start deactivated, prog_start still active
        s_at_540 = next(s for s in samples if s.dts == 540000)
        ids_at_540 = {e["event_id"] for e in s_at_540.emib_list}
        assert 2 not in ids_at_540  # po_start gone
        assert 1 in ids_at_540  # prog_start still there

        # At [900000, ...): prog_start deactivated by prog_end
        s_at_900 = next(s for s in samples if s.dts == 900000)
        ids_at_900 = {e["event_id"] for e in s_at_900.emib_list}
        assert 1 not in ids_at_900  # prog_start gone


# ─── NHML Output Tests ───


class TestGenerateNHML:
    def test_valid_xml(self):
        nhml = generate_nhml([], 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        assert root.tag == "NHNTStream"

    def test_stream_attributes(self):
        nhml = generate_nhml([], 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        assert root.get("timeScale") == "90000"
        assert root.get("streamType") == "Metadata"
        assert root.get("codecID") == "evte"
        assert root.get("trackID") == "3"

    def test_empty_presentation_single_emeb(self):
        nhml = generate_nhml([], 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        samples = root.findall("NHNTSample")
        assert len(samples) == 1
        assert samples[0].find("EventMessageEmptyBox") is not None
        assert samples[0].get("isRAP") == "yes"

    def test_all_samples_have_isRAP(self):
        events = [
            SpliceEvent(
                pts_90khz=270000,
                command="splice_insert",
                splice_event_id=1,
                out_of_network=True,
                duration_90khz=180000,
                auto_return=True,
                segmentation=None,
            )
        ]
        nhml = generate_nhml(events, 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        for sample in root.findall("NHNTSample"):
            assert sample.get("isRAP") == "yes"

    def test_dts_monotonically_increasing(self):
        events = [
            SpliceEvent(
                pts_90khz=270000,
                command="splice_insert",
                splice_event_id=1,
                out_of_network=True,
                duration_90khz=180000,
                auto_return=True,
                segmentation=None,
            )
        ]
        nhml = generate_nhml(events, 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        dts_values = [int(s.get("DTS")) for s in root.findall("NHNTSample")]
        for i in range(1, len(dts_values)):
            assert dts_values[i] > dts_values[i - 1]

    def test_emib_attributes(self):
        events = [
            SpliceEvent(
                pts_90khz=0,
                command="splice_insert",
                splice_event_id=42,
                out_of_network=True,
                duration_90khz=270000,
                auto_return=True,
                segmentation=None,
            )
        ]
        nhml = generate_nhml(events, 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        emib = root.find(".//EventMessageInstanceBox")
        assert emib is not None
        assert emib.get("scheme_id_uri") == "urn:scte:scte35:2013:bin"
        assert emib.get("value") == ""
        assert emib.get("event_id") == "42"
        assert emib.get("message_data").startswith("0x")

    def test_emeb_has_no_attributes(self):
        nhml = generate_nhml([], 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        emeb = root.find(".//EventMessageEmptyBox")
        assert emeb is not None
        assert len(emeb.attrib) == 0

    def test_message_data_hex_encoding(self):
        events = [
            SpliceEvent(
                pts_90khz=0,
                command="splice_insert",
                splice_event_id=1,
                out_of_network=True,
                duration_90khz=None,
                auto_return=False,
                segmentation=None,
            )
        ]
        nhml = generate_nhml(events, 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        emib = root.find(".//EventMessageInstanceBox")
        hex_str = emib.get("message_data")
        assert hex_str.startswith("0xFC")

    def test_multi_segment_output(self):
        nhml = generate_nhml([], 90000, 1080000, 6.0, 3)
        root = ET.fromstring(nhml)
        samples = root.findall("NHNTSample")
        assert len(samples) == 2
        assert int(samples[0].get("DTS")) == 0
        assert int(samples[0].get("duration")) == 540000
        assert int(samples[1].get("DTS")) == 540000
        assert int(samples[1].get("duration")) == 540000

    def test_durations_sum_correctly(self):
        events = [
            SpliceEvent(
                pts_90khz=270000,
                command="splice_insert",
                splice_event_id=1,
                out_of_network=True,
                duration_90khz=180000,
                auto_return=True,
                segmentation=None,
            )
        ]
        nhml = generate_nhml(events, 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)
        total = sum(int(s.get("duration")) for s in root.findall("NHNTSample"))
        assert total == 540000


# ─── Integration-Style Tests ───


class TestEndToEnd:
    def test_splice_insert_full_pipeline(self):
        schedule = {
            "version": 1,
            "events": [
                {
                    "time": "10",
                    "command": "splice_insert",
                    "splice_event_id": 1,
                    "out_of_network": True,
                    "duration": "30s",
                    "auto_return": True,
                }
            ],
        }
        events = parse_schedule(schedule)
        nhml = generate_nhml(events, 90000, 2700000, 6.0, 3)
        root = ET.fromstring(nhml)

        all_samples = root.findall("NHNTSample")
        assert len(all_samples) > 0

        has_emib = any(s.find("EventMessageInstanceBox") is not None for s in all_samples)
        has_emeb = any(s.find("EventMessageEmptyBox") is not None for s in all_samples)
        assert has_emib
        assert has_emeb

    def test_time_signal_full_pipeline(self):
        schedule = {
            "version": 1,
            "events": [
                {
                    "time": "5",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 100,
                        "segmentation_type_id": 52,
                        "segmentation_duration": "10s",
                        "upid_type": 9,
                        "upid": "SIGNAL:test",
                    },
                }
            ],
        }
        events = parse_schedule(schedule)
        nhml = generate_nhml(events, 90000, 1080000, 6.0, 3)
        root = ET.fromstring(nhml)

        emib_elements = root.findall(".//EventMessageInstanceBox")
        assert len(emib_elements) > 0
        hex_data = emib_elements[0].get("message_data")
        raw = bytes.fromhex(hex_data[2:])
        assert raw[0] == 0xFC

    def test_overlapping_events_pipeline(self):
        schedule = {
            "version": 1,
            "events": [
                {"time": "2", "command": "splice_insert", "splice_event_id": 1, "duration": "5s"},
                {"time": "4", "command": "splice_insert", "splice_event_id": 2, "duration": "3s"},
            ],
        }
        events = parse_schedule(schedule)
        nhml = generate_nhml(events, 90000, 900000, 10.0, 3)
        root = ET.fromstring(nhml)

        multi_emib_samples = [
            s for s in root.findall("NHNTSample") if len(s.findall("EventMessageInstanceBox")) > 1
        ]
        assert len(multi_emib_samples) > 0

    def test_crc_valid_in_output(self):
        schedule = {
            "version": 1,
            "events": [{"time": "3", "command": "splice_insert", "splice_event_id": 1, "duration": "5s"}],
        }
        events = parse_schedule(schedule)
        nhml = generate_nhml(events, 90000, 540000, 6.0, 3)
        root = ET.fromstring(nhml)

        for emib in root.findall(".//EventMessageInstanceBox"):
            hex_data = emib.get("message_data")
            raw = bytes.fromhex(hex_data[2:])
            full_crc = crc32_mpeg2(raw)
            assert full_crc == 0, f"CRC validation failed for emib event_id={emib.get('event_id')}"

    def test_program_boundaries_with_ad_break_pipeline(self):
        """Full JSON → NHML: ProgramStart, ad break, ProgramEnd."""
        schedule = {
            "version": 1,
            "events": [
                {
                    "time": "0",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 1,
                        "segmentation_type_id": 16,
                    },
                },
                {
                    "time": "3",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 2,
                        "segmentation_type_id": 52,
                    },
                },
                {
                    "time": "6",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 2,
                        "segmentation_type_id": 53,
                    },
                },
                {
                    "time": "10",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 1,
                        "segmentation_type_id": 17,
                    },
                },
            ],
        }
        events = parse_schedule(schedule)
        nhml = generate_nhml(events, 90000, 1080000, 12.0, 3)
        root = ET.fromstring(nhml)

        all_samples = root.findall("NHNTSample")
        # After ProgramEnd at 10s, ProgramStart should not be active
        for s in all_samples:
            dts = int(s.get("DTS"))
            if dts >= 900000:  # 10s in 90kHz
                emib_ids = [
                    int(e.get("event_id"))
                    for e in s.findall("EventMessageInstanceBox")
                ]
                # splice_event_id=1 is ProgramStart — should not be present
                assert 1 not in emib_ids, (
                    f"ProgramStart still active at DTS={dts} after ProgramEnd"
                )

    def test_spec_example_two_overlapping(self):
        """Matches the two-event overlap example from the CMAF design spec."""
        ev_a = SpliceEvent(
            pts_90khz=180000,
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=450000,
            auto_return=True,
            segmentation=None,
        )
        ev_b = SpliceEvent(
            pts_90khz=360000,
            command="splice_insert",
            splice_event_id=2,
            out_of_network=True,
            duration_90khz=270000,
            auto_return=True,
            segmentation=None,
        )

        samples = compute_samples(0, 900000, [ev_a, ev_b], 90000)

        assert len(samples) == 4

        assert samples[0].dts == 0
        assert samples[0].duration == 180000
        assert samples[0].emib_list == []

        assert samples[1].dts == 180000
        assert samples[1].duration == 180000
        assert len(samples[1].emib_list) == 1
        assert samples[1].emib_list[0]["event_id"] == 1
        assert samples[1].emib_list[0]["presentation_time_delta"] == 0

        assert samples[2].dts == 360000
        assert samples[2].duration == 270000
        assert len(samples[2].emib_list) == 2

        assert samples[3].dts == 630000
        assert samples[3].duration == 270000
        assert samples[3].emib_list == []


# ─── Keyframe Analysis Tests ───


class TestFindNearestKeyframe:
    def test_exact_match(self):
        kf, delta = find_nearest_keyframe([0.0, 6.0, 12.0], 6.0)
        assert kf == 6.0
        assert delta == 0.0

    def test_closest_before(self):
        kf, delta = find_nearest_keyframe([0.0, 5.5, 12.0], 6.0)
        assert kf == 5.5
        assert delta == pytest.approx(0.5)

    def test_closest_after(self):
        kf, delta = find_nearest_keyframe([0.0, 6.5, 12.0], 6.0)
        assert kf == 6.5
        assert delta == pytest.approx(0.5)

    def test_empty_list(self):
        kf, delta = find_nearest_keyframe([], 6.0)
        assert kf is None
        assert delta == float("inf")

    def test_single_keyframe(self):
        kf, delta = find_nearest_keyframe([3.0], 6.0)
        assert kf == 3.0
        assert delta == pytest.approx(3.0)

    def test_target_at_zero(self):
        kf, delta = find_nearest_keyframe([0.0, 5.0], 0.0)
        assert kf == 0.0
        assert delta == 0.0


class TestBuildFfmpegCommand:
    def _make_info(self, codec="h264"):
        return VideoInfo(
            codec_name=codec,
            width=1920, height=1080,
            duration=60.0,
            frame_rate="24000/1001",
            keyframe_pts=[0.0],
            streams=[
                {"index": 0, "codec_name": codec, "codec_type": "video"},
                {"index": 1, "codec_name": "aac", "codec_type": "audio"},
            ],
            timescale=24000,
            duration_ticks=1440000,
        )

    def test_h264_encoder(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info("h264"), [0, 6, 12])
        assert "-c:v libx264" in cmd
        assert "-c:a copy" in cmd

    def test_hevc_encoder(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info("hevc"), [0, 6])
        assert "-c:v libx265" in cmd

    def test_h265_alias(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info("h265"), [0, 6])
        assert "-c:v libx265" in cmd

    def test_av1_encoder(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info("av1"), [0, 6])
        assert "-c:v libsvtav1" in cmd

    def test_unknown_codec_defaults_to_libx264(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info("vp9"), [0, 6])
        assert "-c:v libx264" in cmd

    def test_force_key_frames_included(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info(), [0, 6, 10, 12])
        assert "-force_key_frames" in cmd
        assert "0.000" in cmd
        assert "6.000" in cmd
        assert "10.000" in cmd
        assert "12.000" in cmd

    def test_deduplicates_times(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info(), [6, 6, 6, 12])
        kf_part = cmd.split("-force_key_frames")[1].split("-c:a")[0]
        assert kf_part.count("6.000") == 1

    def test_custom_output_path(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info(), [0], "output.mp4")
        assert cmd.endswith("output.mp4")

    def test_default_output_path(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info(), [0])
        assert "input_spliceready.mp4" in cmd

    def test_audio_always_copy(self):
        cmd = build_ffmpeg_command("input.mp4", self._make_info(), [0])
        assert "-c:a copy" in cmd


class TestCollectSpliceBoundaries:
    """Test that collect_splice_boundaries checks both start AND end (auto-return)."""

    def _make_event(self, pts_sec, duration_sec=None, auto_return=False, event_id=1):
        return SpliceEvent(
            pts_90khz=int(pts_sec * 90000),
            command="splice_insert",
            splice_event_id=event_id,
            out_of_network=True,
            duration_90khz=int(duration_sec * 90000) if duration_sec else None,
            auto_return=auto_return,
            segmentation=None,
        )

    def test_event_with_duration_has_two_boundaries(self):
        ev = self._make_event(pts_sec=10.0, duration_sec=30.0)
        boundaries = collect_splice_boundaries([ev], video_duration=60.0)
        assert len(boundaries) == 2
        assert boundaries[0][0] == 10.0
        assert "start" in boundaries[0][1]
        assert boundaries[1][0] == 40.0
        assert "end" in boundaries[1][1]

    def test_event_with_duration_no_auto_return_still_has_end(self):
        ev = self._make_event(pts_sec=10.0, duration_sec=30.0, auto_return=False)
        boundaries = collect_splice_boundaries([ev], video_duration=60.0)
        assert len(boundaries) == 2
        assert boundaries[1][0] == 40.0

    def test_no_duration_event_has_one_boundary(self):
        ev = self._make_event(pts_sec=10.0)
        boundaries = collect_splice_boundaries([ev], video_duration=60.0)
        assert len(boundaries) == 1

    def test_end_beyond_video_duration_excluded(self):
        ev = self._make_event(pts_sec=30.0, duration_sec=30.0)
        boundaries = collect_splice_boundaries([ev], video_duration=50.0)
        assert len(boundaries) == 1

    def test_multiple_events_with_duration(self):
        events = [
            self._make_event(pts_sec=10.0, duration_sec=15.0, event_id=1),
            self._make_event(pts_sec=30.0, duration_sec=20.0, event_id=2),
        ]
        boundaries = collect_splice_boundaries(events, video_duration=60.0)
        assert len(boundaries) == 4
        times = [b[0] for b in boundaries]
        assert times == [10.0, 25.0, 30.0, 50.0]

    def test_mixed_with_and_without_duration(self):
        events = [
            self._make_event(pts_sec=10.0, duration_sec=15.0, event_id=1),
            self._make_event(pts_sec=30.0, event_id=2),
        ]
        boundaries = collect_splice_boundaries(events, video_duration=60.0)
        assert len(boundaries) == 3
        times = [b[0] for b in boundaries]
        assert times == [10.0, 25.0, 30.0]

    def test_zero_duration_no_end_boundary(self):
        ev = self._make_event(pts_sec=10.0, duration_sec=0)
        boundaries = collect_splice_boundaries([ev], video_duration=60.0)
        assert len(boundaries) == 1

    def test_time_signal_segmentation_duration_has_end_boundary(self):
        ev = SpliceEvent(
            pts_90khz=int(60.0 * 90000),
            command="time_signal",
            splice_event_id=100,
            out_of_network=True,
            duration_90khz=None,
            auto_return=False,
            segmentation=SegmentationInfo(
                segmentation_event_id=100,
                segmentation_type_id=0x34,
                segmentation_duration_90khz=int(30.0 * 90000),
                upid_type=0,
                upid=None,
            ),
        )
        boundaries = collect_splice_boundaries([ev], video_duration=120.0)
        assert len(boundaries) == 2
        assert boundaries[0][0] == 60.0
        assert boundaries[1][0] == 90.0
        assert "end" in boundaries[1][1]

    def test_splice_insert_duration_takes_precedence_over_segmentation(self):
        ev = SpliceEvent(
            pts_90khz=int(10.0 * 90000),
            command="splice_insert",
            splice_event_id=1,
            out_of_network=True,
            duration_90khz=int(20.0 * 90000),
            auto_return=True,
            segmentation=SegmentationInfo(
                segmentation_event_id=1,
                segmentation_type_id=0x34,
                segmentation_duration_90khz=int(30.0 * 90000),
                upid_type=0,
                upid=None,
            ),
        )
        boundaries = collect_splice_boundaries([ev], video_duration=60.0)
        assert len(boundaries) == 2
        assert boundaries[1][0] == 30.0


class TestPreRoll:
    TIMESCALE = 90000

    def _make_event(self, pts_sec, pre_roll_ms=0, duration_sec=None, event_id=1):
        return SpliceEvent(
            pts_90khz=int(pts_sec * 90000),
            command="splice_insert",
            splice_event_id=event_id,
            out_of_network=True,
            duration_90khz=int(duration_sec * 90000) if duration_sec else None,
            auto_return=False,
            segmentation=None,
            pre_roll_90khz=int(pre_roll_ms / 1000 * 90000),
        )

    def test_no_pre_roll_no_extra_boundary(self):
        ev = self._make_event(pts_sec=5.0)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        boundaries = [s.dts for s in samples]
        assert 0 in boundaries
        assert 5 * self.TIMESCALE in boundaries
        assert len(samples) == 2

    def test_pre_roll_creates_extra_boundary(self):
        ev = self._make_event(pts_sec=5.0, pre_roll_ms=500)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        boundaries = [s.dts for s in samples]
        pre_roll_ticks = int(4.5 * self.TIMESCALE)
        assert pre_roll_ticks in boundaries
        assert 5 * self.TIMESCALE in boundaries
        assert len(samples) == 3

    def test_pre_roll_sample_has_positive_presentation_time_delta(self):
        ev = self._make_event(pts_sec=5.0, pre_roll_ms=500)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        pre_roll_ticks = int(4.5 * self.TIMESCALE)
        pre_roll_sample = [s for s in samples if s.dts == pre_roll_ticks][0]
        assert len(pre_roll_sample.emib_list) == 1
        ptd = pre_roll_sample.emib_list[0]["presentation_time_delta"]
        expected_delta = 5 * self.TIMESCALE - pre_roll_ticks
        assert ptd == expected_delta
        assert ptd > 0

    def test_splice_sample_has_zero_presentation_time_delta(self):
        ev = self._make_event(pts_sec=5.0, pre_roll_ms=500)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        splice_sample = [s for s in samples if s.dts == 5 * self.TIMESCALE][0]
        assert len(splice_sample.emib_list) == 1
        assert splice_sample.emib_list[0]["presentation_time_delta"] == 0

    def test_pre_roll_clamped_to_segment_start(self):
        ev = self._make_event(pts_sec=0.2, pre_roll_ms=500)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        assert samples[0].dts == 0
        assert len(samples[0].emib_list) == 1
        ptd = samples[0].emib_list[0]["presentation_time_delta"]
        assert ptd == int(0.2 * self.TIMESCALE)

    def test_pre_roll_clamped_to_zero(self):
        ev = self._make_event(pts_sec=0.1, pre_roll_ms=300)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        assert samples[0].dts == 0
        assert len(samples[0].emib_list) == 1

    def test_pre_roll_does_not_affect_event_end_boundary(self):
        ev = self._make_event(pts_sec=5.0, pre_roll_ms=500, duration_sec=3.0)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        boundaries = [s.dts for s in samples]
        assert int(4.5 * self.TIMESCALE) in boundaries
        assert 5 * self.TIMESCALE in boundaries
        assert 8 * self.TIMESCALE in boundaries

    def test_multiple_events_with_different_pre_rolls(self):
        ev1 = self._make_event(pts_sec=3.0, pre_roll_ms=200, event_id=1)
        ev2 = self._make_event(pts_sec=7.0, pre_roll_ms=500, event_id=2)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev1, ev2], self.TIMESCALE)
        boundaries = [s.dts for s in samples]
        assert 3 * self.TIMESCALE - int(0.2 * self.TIMESCALE) in boundaries
        assert 3 * self.TIMESCALE in boundaries
        assert 7 * self.TIMESCALE - int(0.5 * self.TIMESCALE) in boundaries
        assert 7 * self.TIMESCALE in boundaries

    def test_pre_roll_event_not_in_sample_before_pre_roll_window(self):
        ev = self._make_event(pts_sec=5.0, pre_roll_ms=500)
        samples = compute_samples(0, 10 * self.TIMESCALE, [ev], self.TIMESCALE)
        first_sample = samples[0]
        assert first_sample.dts == 0
        assert len(first_sample.emib_list) == 0


class TestPreRollParsing:
    def test_global_pre_roll(self):
        schedule = {
            "version": 1,
            "pre_roll": "300ms",
            "events": [
                {"time": "10s", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(schedule)
        assert events[0].pre_roll_90khz == int(0.3 * 90000)

    def test_per_event_pre_roll_overrides_global(self):
        schedule = {
            "version": 1,
            "pre_roll": "500ms",
            "events": [
                {"time": "10s", "command": "splice_insert", "pre_roll": "200ms"},
                {"time": "20s", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(schedule)
        assert events[0].pre_roll_90khz == int(0.2 * 90000)
        assert events[1].pre_roll_90khz == int(0.5 * 90000)

    def test_no_pre_roll_defaults_to_zero(self):
        schedule = {
            "version": 1,
            "events": [
                {"time": "10s", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(schedule)
        assert events[0].pre_roll_90khz == 0

    def test_pre_roll_as_ticks(self):
        schedule = {
            "version": 1,
            "pre_roll": 27000,
            "events": [
                {"time": "10s", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(schedule)
        assert events[0].pre_roll_90khz == 27000

    def test_per_event_pre_roll_without_global(self):
        schedule = {
            "version": 1,
            "events": [
                {"time": "10s", "command": "splice_insert", "pre_roll": "100ms"},
                {"time": "20s", "command": "splice_insert"},
            ],
        }
        events = parse_schedule(schedule)
        assert events[0].pre_roll_90khz == int(0.1 * 90000)
        assert events[1].pre_roll_90khz == 0

    def test_negative_global_pre_roll_raises(self):
        schedule = {
            "version": 1,
            "pre_roll": -1000,
            "events": [{"time": "10s", "command": "splice_insert"}],
        }
        with pytest.raises(ValueError, match="pre_roll must not be negative"):
            parse_schedule(schedule)

    def test_negative_per_event_pre_roll_raises(self):
        schedule = {
            "version": 1,
            "events": [
                {"time": "10s", "command": "splice_insert", "pre_roll": -500},
            ],
        }
        with pytest.raises(ValueError, match="pre_roll must not be negative"):
            parse_schedule(schedule)

    def test_time_signal_with_pre_roll(self):
        schedule = {
            "version": 1,
            "pre_roll": "300ms",
            "events": [
                {
                    "time": "10s",
                    "command": "time_signal",
                    "segmentation": {
                        "segmentation_event_id": 1,
                        "segmentation_type_id": 52,
                    },
                },
            ],
        }
        events = parse_schedule(schedule)
        assert events[0].pre_roll_90khz == int(0.3 * 90000)
        samples = compute_samples(0, 20 * 90000, events, 90000)
        pre_roll_boundary = 10 * 90000 - int(0.3 * 90000)
        boundaries = [s.dts for s in samples]
        assert pre_roll_boundary in boundaries


class TestPreRollEndToEnd:
    TIMESCALE = 90000

    def test_pre_roll_nhml_has_positive_ptd(self):
        schedule = {
            "version": 1,
            "pre_roll": "500ms",
            "events": [
                {"time": "6s", "command": "splice_insert", "duration": "30s"},
            ],
        }
        events = parse_schedule(schedule)
        nhml = generate_nhml(
            events=events,
            timescale=self.TIMESCALE,
            total_duration_ticks=60 * self.TIMESCALE,
            seg_duration_seconds=6.0,
            track_id=3,
        )
        root = ET.fromstring(nhml)
        all_samples = root.findall("NHNTSample")
        pre_roll_sample = None
        for s in all_samples:
            dts = int(s.get("DTS", "0"))
            if dts == int(5.5 * self.TIMESCALE):
                pre_roll_sample = s
                break
        assert pre_roll_sample is not None
        emib = pre_roll_sample.find("EventMessageInstanceBox")
        assert emib is not None
        ptd = int(emib.get("presentation_time_delta", "0"))
        assert ptd == int(0.5 * self.TIMESCALE)
