# scte35-nhml-gen

Generate SCTE-35 event message tracks (NHML) for use with GPAC.

Takes a JSON schedule of splice events and an input video file, probes
the video for timescale and duration, encodes SCTE-35 binary sections
per ANSI/SCTE 35 2023r1, computes sample boundaries per ISO 23001-18 §8c,
and outputs an NHML file with `emib`/`emeb` samples ready to mux alongside
video using GPAC.

```
events.json ──→ scte35_nhml_gen.py ──→ events.nhml ──→ gpac ──→ DASH/CMAF
                     │
                     └─ probes video.mp4 for timescale + duration
```

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or plain Python
- [GPAC](https://gpac.io) (for video probing and final muxing)
- [FFmpeg](https://ffmpeg.org) (for `validate` keyframe inspection via ffprobe)

## Quick Start

```bash
# 1. Validate your schedule and video (checks keyframe alignment)
uv run scte35_nhml_gen.py validate -i events.json input.mp4 -s 6

# 2. Generate the NHML event track
uv run scte35_nhml_gen.py events.json input.mp4 -o events.nhml

# 3. Mux with GPAC into DASH/CMAF
gpac -i input.mp4 -i events.nhml:#ID=3 -o output/manifest.mpd:profile=live:cmaf=cmf2:segdur=6
```

If `validate` reports misaligned keyframes, re-encode first with the
suggested ffmpeg command, then repeat from step 1.

## Commands

### generate (default)

Generate NHML from a JSON schedule and video file.

```bash
uv run scte35_nhml_gen.py generate events.json input.mp4 -o events.nhml

# The "generate" subcommand is optional — these are equivalent:
uv run scte35_nhml_gen.py events.json input.mp4 -o events.nhml
```

| Argument | Description |
|----------|-------------|
| `schedule` | Path to JSON schedule file |
| `video` | Input video file (probed for timescale + duration) |
| `-s`, `--seg-duration` | Segment duration in seconds (default: 6) |
| `-t`, `--track-id` | Track ID for the event track (default: 3) |
| `-o` | Output NHML file path (default: stdout) |

### validate

Validate a schedule, a video, or both. Three modes:

```bash
# Video only — analyze keyframes and segment boundary alignment
uv run scte35_nhml_gen.py validate input.mp4 -s 6

# Schedule only — parse check, no video needed
uv run scte35_nhml_gen.py validate -i events.json

# Both — check splice points + segment boundaries against keyframes
uv run scte35_nhml_gen.py validate -i events.json input.mp4 -s 6
```

**Video-only** lists all keyframe positions (valid splice points) and checks
segment boundary alignment:

```
Video: input.mp4
  Codec: h264 (640x360)
  Frame rate: 24000/1001
  Duration: 4:40.530
  Timescale: 24000
  Keyframes: 61 found

Keyframe positions (valid splice points):
  0.000s
  5.339s
  9.801s
  ...

Segment boundary alignment (seg_duration=6.0s):
        0.000s  OK  keyframe at 0.000s (delta: 0ms)
        6.000s  !!  nearest keyframe at 5.339s (delta: 661ms)
       ...
```

**With schedule + video** also checks each splice point:

```
Splice point alignment:
       10.000s  OK  keyframe at 10.010s (delta: 10ms)

Segment boundary alignment (seg_duration=6.0s):
        0.000s  OK  keyframe at 0.000s (delta: 0ms)
        6.000s  OK  keyframe at 6.006s (delta: 6ms)
       ...

All splice points and segment boundaries are aligned with keyframes.
```

**When alignment fails** (exit code 1), suggests an ffmpeg command that
auto-detects the video codec:

```
Video needs re-encoding with forced keyframes. Suggested command:
  ffmpeg -i input.mp4 -c:v libx264 -force_key_frames "0.000,6.000,10.000,12.000,..." -c:a copy input_spliceready.mp4
```

| Video Codec | Suggested Encoder |
|-------------|-------------------|
| H.264 (AVC) | `libx264` |
| H.265 (HEVC) | `libx265` |
| AV1 | `libsvtav1` |

Audio streams are always copied (`-c:a copy`).

The keyframe alignment tolerance is computed from the video's actual frame
rate (e.g. ~41.7ms for 24fps, ~33.3ms for 30fps).

| Argument | Description |
|----------|-------------|
| `video` | Video file to check keyframe alignment |
| `-i`, `--schedule` | JSON schedule file to validate |
| `-s`, `--seg-duration` | Segment duration in seconds (default: 6) |

### schema

Print the embedded JSON Schema (draft 2020-12) for the schedule format.
Useful for editor autocompletion or external validation tools.

```bash
# Print to stdout
uv run scte35_nhml_gen.py schema

# Save to file
uv run scte35_nhml_gen.py schema > scte35_schedule.schema.json
```

## JSON Schedule Format

The schedule uses a simple JSON format with two SCTE-35 command types:
`splice_insert` and `time_signal` with `segmentation_descriptor`.

### Time Strings

Any time field (`time`, `duration`, `segmentation_duration`) accepts flexible
human-readable strings:

| Format | Example | Meaning |
|--------|---------|---------|
| Plain seconds | `"30"`, `"30.5"` | 30s, 30.5s |
| HH:MM:SS | `"1:30:45"` | 1h 30m 45s |
| MM:SS | `"30:45"` | 30m 45s |
| With suffix | `"200ms"`, `"30s"`, `"1000us"` | Explicit unit |

The `pts` field is always numeric (90kHz ticks). The `duration` and
`segmentation_duration` fields also accept a numeric value interpreted as
90kHz ticks.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | int | yes | Must be `1` |
| `pre_roll` | int/string | no | Default pre-roll for all events (e.g. `"500ms"`) |
| `events` | array | yes | List of splice events |
| `events[].time` | string | one of time/pts | Splice point as time string |
| `events[].pts` | int | one of time/pts | Splice point in 90kHz ticks |
| `events[].command` | string | yes | `"splice_insert"` or `"time_signal"` |
| `events[].splice_event_id` | int | no | Auto-increments from 1 if omitted |
| `events[].out_of_network` | bool | no | true = ad start (default: true) |
| `events[].duration` | int/string | no | Break duration |
| `events[].auto_return` | bool | no | Auto-return after duration |
| `events[].pre_roll` | int/string | no | Per-event pre-roll, overrides global |
| `events[].segmentation` | object | conditional | Required for `time_signal` |
| `segmentation.segmentation_event_id` | int | no | Defaults to `splice_event_id` |
| `segmentation.segmentation_type_id` | int | no | SCTE-35 Table 23 value |
| `segmentation.segmentation_duration` | int/string | no | Segmentation duration |
| `segmentation.upid_type` | int | no | UPID type (e.g. 9 = ADI) |
| `segmentation.upid` | string | no | UPID value |

## Examples

### Simple Ad Break with splice_insert

A 30-second ad break starting at the 2-minute mark:

```json
{
  "version": 1,
  "events": [
    {
      "time": "2:00",
      "command": "splice_insert",
      "splice_event_id": 1,
      "out_of_network": true,
      "duration": "30s",
      "auto_return": true
    }
  ]
}
```

```bash
uv run scte35_nhml_gen.py validate ad_break.json movie.mp4 -s 6
uv run scte35_nhml_gen.py ad_break.json movie.mp4 -s 6 -o events.nhml
gpac -i movie.mp4 -i events.nhml:#ID=3 \
  -o output/manifest.mpd:profile=live:cmaf=cmf2:segdur=6
```

### Program Boundaries with time_signal

Mark a program starting at 10s and ending at 5 minutes, with a 30-second ad
break in the middle at the 2:30 mark. Uses `segmentation_type_id` values from
SCTE-35 Table 23:

- `16` (`0x10`) — Program Start
- `17` (`0x11`) — Program End
- `52` (`0x34`) — Provider Placement Opportunity Start
- `53` (`0x35`) — Provider Placement Opportunity End

Open/close pairs are matched by `segmentation_event_id`. When a closing type
arrives, the corresponding open event is automatically deactivated in the
event message track.

```json
{
  "version": 1,
  "events": [
    {
      "time": "10",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 1,
        "segmentation_type_id": 16,
        "upid_type": 9,
        "upid": "SIGNAL:program_start"
      }
    },
    {
      "time": "2:30",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 2,
        "segmentation_type_id": 52,
        "segmentation_duration": "30s",
        "upid_type": 9,
        "upid": "SIGNAL:ad_break_1"
      }
    },
    {
      "time": "3:00",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 2,
        "segmentation_type_id": 53,
        "upid_type": 9,
        "upid": "SIGNAL:ad_break_1"
      }
    },
    {
      "time": "5:00",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 1,
        "segmentation_type_id": 17,
        "upid_type": 9,
        "upid": "SIGNAL:program_end"
      }
    }
  ]
}
```

This produces the following timeline:

```
0s        10s                    2:30      3:00                5:00
|---------|----------------------|---------|-------------------|
          ↑ ProgramStart(0x10)   ↑ POStart ↑ POEnd(0x35)       ↑ ProgramEnd(0x11)
          seg_event_id=1         (0x34)      seg_event_id=2      seg_event_id=1
                                 seg_event_id=2
                                 dur=30s
```

The event message track samples look like:

```
[0s,  10s)    → emeb                      (nothing active)
[10s, 2:30)   → emib(ProgramStart)        (program running)
[2:30, 3:00)  → emib(ProgramStart)        (program + ad break)
                + emib(POStart)
[3:00, 5:00)  → emib(ProgramStart)        (POStart deactivated by POEnd)
                + emib(POEnd)
[5:00, end)   → emib(ProgramEnd)          (ProgramStart deactivated by ProgramEnd)
```

### Live Sports Broadcast with Multiple Ad Breaks

A 90-minute sports broadcast with a halftime break and two in-game ad
opportunities:

```json
{
  "version": 1,
  "events": [
    {
      "time": "0",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 1,
        "segmentation_type_id": 16,
        "upid_type": 9,
        "upid": "SPORTS:match_start"
      }
    },
    {
      "time": "22:00",
      "command": "splice_insert",
      "splice_event_id": 100,
      "out_of_network": true,
      "duration": "60s",
      "auto_return": true
    },
    {
      "time": "45:00",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 2,
        "segmentation_type_id": 52,
        "segmentation_duration": "15:00",
        "upid_type": 9,
        "upid": "SPORTS:halftime"
      }
    },
    {
      "time": "1:00:00",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 2,
        "segmentation_type_id": 53,
        "upid_type": 9,
        "upid": "SPORTS:halftime"
      }
    },
    {
      "time": "1:12:00",
      "command": "splice_insert",
      "splice_event_id": 101,
      "out_of_network": true,
      "duration": "60s",
      "auto_return": true
    },
    {
      "time": "1:30:00",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 1,
        "segmentation_type_id": 17,
        "upid_type": 9,
        "upid": "SPORTS:match_end"
      }
    }
  ]
}
```

### Mixing splice_insert and time_signal

Combine simple ad breaks (`splice_insert`) with structured segmentation
markers (`time_signal`). Ad breaks use `splice_insert` for straightforward
out/return signaling; program boundaries use `time_signal` with descriptors
for richer metadata:

```json
{
  "version": 1,
  "events": [
    {
      "time": "0",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 1,
        "segmentation_type_id": 16,
        "upid_type": 9,
        "upid": "SHOW:episode_42"
      }
    },
    {
      "time": "8:00",
      "command": "splice_insert",
      "splice_event_id": 10,
      "out_of_network": true,
      "duration": "2:00",
      "auto_return": true
    },
    {
      "time": "20:00",
      "command": "splice_insert",
      "splice_event_id": 11,
      "out_of_network": true,
      "duration": "2:30",
      "auto_return": true
    },
    {
      "time": "30:00",
      "command": "time_signal",
      "segmentation": {
        "segmentation_event_id": 1,
        "segmentation_type_id": 17,
        "upid_type": 9,
        "upid": "SHOW:episode_42"
      }
    }
  ]
}
```

### Using PTS Addressing

For precise control, use 90kHz PTS ticks directly instead of time strings:

```json
{
  "version": 1,
  "events": [
    {
      "pts": 2700000,
      "command": "splice_insert",
      "splice_event_id": 1,
      "out_of_network": true,
      "duration": 2700000,
      "auto_return": true
    }
  ]
}
```

`pts: 2700000` = 30 seconds at 90kHz. `duration: 2700000` = 30 seconds.

### Pre-Roll

Pre-roll places the event in samples *before* the splice point, giving CMAF
clients advance notice via a positive `presentation_time_delta` (per ISO
23001-18 §7.4). This is useful for low-latency live streams where the client
needs time to fetch ad content before the splice occurs.

There is no pre-roll by default. Set it globally or per-event:

```json
{
  "version": 1,
  "pre_roll": "500ms",
  "events": [
    {
      "time": "30s",
      "command": "splice_insert",
      "duration": "30s",
      "auto_return": true
    },
    {
      "time": "2:00",
      "command": "splice_insert",
      "pre_roll": "300ms",
      "duration": "30s",
      "auto_return": true
    }
  ]
}
```

The first event inherits the global 500ms pre-roll. The second event overrides
it with 300ms. The resulting event message track contains:

```
[29.5s, 30s)  → emib with presentation_time_delta = +0.5s  (pre-roll)
[30s,   60s)  → emib with presentation_time_delta = 0      (splice active)
...
[1:59.7, 2:00) → emib with presentation_time_delta = +0.3s (pre-roll)
[2:00,  2:30)  → emib with presentation_time_delta = 0     (splice active)
```

Pre-roll points do not require video keyframes — they are metadata-only
boundaries within the event track. Keyframes are only needed at the actual
splice point where video content switches.

## Keyframe Alignment

For clean ad insertion, the video must have I-frames (keyframes) at splice
points and segment boundaries. The `validate` command checks this and
suggests an ffmpeg re-encode command when keyframes are missing.

The keyframe alignment tolerance is computed from the video's actual frame
rate — a keyframe within one frame duration of the target is considered
aligned (e.g. ~41.7ms for 24fps, ~33.3ms for 30fps).

### Typical workflow

```bash
# 1. Check the source video for keyframe alignment
uv run scte35_nhml_gen.py validate source.mp4 -s 6

# 2. If misaligned, re-encode with the suggested command
ffmpeg -i source.mp4 -c:v libx264 \
  -force_key_frames "0,6,10,12,18,24,30,..." \
  -c:a copy source_ready.mp4

# 3. Validate schedule + video together
uv run scte35_nhml_gen.py validate -i events.json source_ready.mp4 -s 6

# 4. Generate NHML and mux
uv run scte35_nhml_gen.py events.json source_ready.mp4 -o events.nhml
gpac -i source_ready.mp4 -i events.nhml:#ID=3 \
  -o output/manifest.mpd:profile=live:cmaf=cmf2:segdur=6
```

## Output

The script produces an NHML file that GPAC reads via its `nhmlr` filter. After
muxing with GPAC, the output directory contains:

```
output/
├── manifest.mpd                    (DASH manifest)
├── video_dashinit.mp4              (video init segment)
├── video_dash1.m4s                 (video segment 1)
├── video_dash2.m4s                 (video segment 2)
├── ...
├── events_dashinit.mp4             (event track init segment)
├── events_dash1.m4s                (event segment 1)
├── events_dash2.m4s                (event segment 2)
└── ...
```

The event track appears in the MPD as:

```xml
<AdaptationSet segmentAlignment="true" mimeType="application/mp4" startWithSAP="1">
  <Representation id="3" codecs="evte" bandwidth="27">
    <SegmentTemplate timescale="24000" .../>
  </Representation>
</AdaptationSet>
```

## Segmentation Type Pairing

Open/close segmentation type pairs are automatically handled per SCTE-35
§10.3.3. When a closing type arrives, the matching open event (same
`segmentation_event_id`) is deactivated in the event message track.

| Open (start) | Close (end) | Description |
|---|---|---|
| `16` (`0x10`) | `17` (`0x11`) | Program Start / End |
| `32` (`0x20`) | `33` (`0x21`) | Chapter Start / End |
| `48` (`0x30`) | `49` (`0x31`) | Provider Ad Start / End |
| `50` (`0x32`) | `51` (`0x33`) | Distributor Ad Start / End |
| `52` (`0x34`) | `53` (`0x35`) | Provider Placement Opportunity Start / End |
| `54` (`0x36`) | `55` (`0x37`) | Distributor Placement Opportunity Start / End |
| `64` (`0x40`) | `65` (`0x41`) | Unscheduled Event Start / End |
| `68` (`0x44`) | `69` (`0x45`) | Network Start / End |

## SCTE-35 Segmentation Type Reference

All `segmentation_type_id` values from SCTE-35 Table 23:

| Type ID | Hex | Description |
|---------|-----|-------------|
| 0 | `0x00` | Not Indicated |
| 1 | `0x01` | Content Identification |
| 2 | `0x02` | Private |
| 16 | `0x10` | Program Start |
| 17 | `0x11` | Program End |
| 18 | `0x12` | Program Early Termination |
| 19 | `0x13` | Program Breakaway |
| 20 | `0x14` | Program Resumption |
| 21 | `0x15` | Program Runover Planned |
| 22 | `0x16` | Program Runover Unplanned |
| 23 | `0x17` | Program Overlap Start |
| 24 | `0x18` | Program Blackout Override |
| 25 | `0x19` | Program Join |
| 32 | `0x20` | Chapter Start |
| 33 | `0x21` | Chapter End |
| 34 | `0x22` | Break Start |
| 35 | `0x23` | Break End |
| 48 | `0x30` | Provider Ad Start |
| 49 | `0x31` | Provider Ad End |
| 50 | `0x32` | Distributor Ad Start |
| 51 | `0x33` | Distributor Ad End |
| 52 | `0x34` | Provider Placement Opportunity Start |
| 53 | `0x35` | Provider Placement Opportunity End |
| 54 | `0x36` | Distributor Placement Opportunity Start |
| 55 | `0x37` | Distributor Placement Opportunity End |
| 64 | `0x40` | Unscheduled Event Start |
| 65 | `0x41` | Unscheduled Event End |
| 68 | `0x44` | Network Start |
| 69 | `0x45` | Network End |
| 80 | `0x50` | Provider Ad Block Start |
| 81 | `0x51` | Provider Ad Block End |

## How It Works

1. **Probes the video** with `ffprobe` for timescale, duration, keyframe
   positions, and codec detection
2. **Parses the JSON schedule** and converts all times to 90kHz PTS values
3. **Encodes SCTE-35 binary** `splice_info_section()` for each event with
   proper CRC-32 (MPEG-2 polynomial)
4. **Runs the sample boundary algorithm** (ISO 23001-18 §8c) per segment:
   boundaries at event activation/deactivation (and pre-roll points when
   configured), emib for active events, emeb for gaps
5. **Handles segmentation type pairing** — closing types (ProgramEnd,
   POEnd, etc.) deactivate their matching open event per SCTE-35 §10.3.3
6. **Writes NHML** with `EventMessageInstanceBox` (emib) and
   `EventMessageEmptyBox` (emeb) samples

Events that span multiple segments are carried over with negative
`presentation_time_delta`, which is valid per ISO 23001-18 §8b. Events with
pre-roll appear in samples before their splice time with a positive
`presentation_time_delta`, which is the ISO 23001-18 §7.4 "future events"
mechanism.

## Testing

```bash
uv run --with pytest pytest test_scte35_nhml_gen.py -v
```

Tests cover: SCTE-35 binary encoding, CRC-32, time parsing, JSON schedule
parsing, sample boundary algorithm, segmentation type pairing, pre-roll,
keyframe analysis, ffmpeg command generation, NHML output, and end-to-end
integration.

## Standards

- **ANSI/SCTE 35 2023r1** — Digital Program Insertion Cueing Message
- **ISO/IEC 23001-18:2022** — Event message track format (emib/emeb)
- **ISO/IEC 14496-12:2022** — ISO base media file format (ISOBMFF)
- **SCTE-214** — DASH/CMAF signaling of SCTE-35

## License

MIT
