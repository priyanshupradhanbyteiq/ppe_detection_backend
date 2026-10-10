# Camera recognition backend

Backend for registering construction-site cameras, authenticating users, validating
streams, and controlling a separate GPU worker that detects personal protective
equipment (PPE). FastAPI handles HTTP requests, PostgreSQL stores application and
runtime data, and RabbitMQ carries camera commands to NVIDIA DeepStream.

The worker builds one dynamic pipeline with **up to five simultaneous H265 RTSP
cameras**, PPE inference, object tracking, and a local tiled display. A queued
decision worker associates PPE with tracked people and confirms violations over
time. It logs event creation/resolution and saves events in PostgreSQL `ppe_events`.
Selected evidence frames upload to Cloudinary when credentials are configured.
The separate `detection_events` table and frontend event notifications remain
unconnected.

This guide describes the current implementation, including startup recovery and
the PPE decision processor. Follow the GPU setup below for live camera processing.

## Contents

- [Technology stack and dependencies](#technology-stack-and-dependencies)
- [Project structure](#project-structure)
- [Full application flow](#full-application-flow)
- [PPE association and decision events](#ppe-association-and-decision-events)
- [Database tables](#database-tables)
- [How to run](#how-to-run)
- [Try the API and camera lifecycle](#try-the-api-and-camera-lifecycle)
- [API reference](#api-reference)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [Current limitations](#current-limitations)

## Technology stack and dependencies

| Technology | Role in this project |
| --- | --- |
| Python 3.10+ | Application language; use Python 3.10 for the DeepStream 7.1 setup below |
| FastAPI + Uvicorn | HTTP API, ASGI server, Swagger/ReDoc documentation |
| Pydantic + Pydantic Settings | Request/response validation and `.env` configuration |
| SQLAlchemy + asyncpg | Async PostgreSQL database access |
| Alembic | Versioned database migrations |
| PostgreSQL | Users, cameras, sessions, worker records, runtime/status history |
| RabbitMQ + aio-pika | Camera commands over a durable topic exchange |
| python-jose + cryptography | JWT access and refresh tokens |
| Passlib + bcrypt | Password hashing and verification |
| OpenCV + NumPy | Stream validation, frame handling, JPEG thumbnail encoding |
| Cloudinary | Optional registration thumbnails and confirmed PPE evidence snapshots |
| NVIDIA DeepStream + GStreamer | GPU decoding, batching, inference, tracking, display |
| CUDA + TensorRT | GPU runtime and optimized model inference |
| PyGObject (`gi`) + `pyds` | Python access to GStreamer/GLib and DeepStream metadata |
| ONNX model + custom C++ YOLO parser | PPE model and conversion of model output into detection boxes |
| NvSORT tracker | Associates detections across frames and supplies track IDs |
| Python queue/threading and SQLite | Bounded frame submission, PPE decisions, and a durable local outbox feeding async PostgreSQL writes |

The backend consumes an exported ONNX model. Training frameworks are not required
to run the API or this inference worker.

### Python packages

[requirements.txt](requirements.txt) contains:

```text
fastapi                  uvicorn
sqlalchemy               asyncpg
pydantic-settings        alembic
email-validator          python-multipart
aiofiles                 python-jose[cryptography]
passlib[bcrypt]           opencv-python-headless
numpy                    cloudinary
aio-pika                 python-dotenv[cli]
```

`python-multipart` and `aiofiles` support upload/file-handling code. An upload
service exists, but no video-upload HTTP route is currently registered.

**The requirements file does not install the NVIDIA SDK, GStreamer plugins,
`gi`, or `pyds`.** These are additional worker dependencies. Its Python package
versions are unpinned, so it is not a locked, reproducible environment.

### System and hardware requirements

| Run mode / feature | Requirements |
| --- | --- |
| API only | Python 3.10+, PostgreSQL, RabbitMQ; no NVIDIA GPU or DeepStream required |
| GPU video worker | Linux, supported NVIDIA GPU, compatible driver/CUDA/TensorRT, DeepStream, GStreamer, `gi`, `pyds`, and graphical display access, plus API services/packages |
| Compile custom parser | `g++`, `make`, DeepStream headers, CUDA headers/runtime libraries |
| Real camera processing | Network access, correct RTSP credentials/path, H265 video |
| Registration thumbnails | All three Cloudinary credentials and access to Cloudinary |

The parser Makefile targets `/opt/nvidia/deepstream/deepstream-7.1`. For that
version, NVIDIA documents Ubuntu 22.04, CUDA 12.6, and TensorRT 10.3. Install the
matching driver and SDK components for your GPU using the
[DeepStream 7.1 installation guide](https://docs.nvidia.com/metropolis/deepstream/7.1/text/DS_Installation.html).
The matching Python bindings release is `pyds` 1.2.0 for Python 3.10, with x86
and Jetson wheels. That release does not support NumPy 2.x. See the
[NVIDIA Python bindings release](https://github.com/NVIDIA-AI-IOT/deepstream_python_apps/releases/tag/v1.2.0).

Docker is optional and used below for PostgreSQL and RabbitMQ. There is no
backend Dockerfile or Compose configuration for the API/worker.

## Project structure

```text
camera_recognization_backend/
├── main.py                         # FastAPI app, startup/shutdown, CORS
├── requirements.txt                # Python application dependencies
├── .env.example                    # Configuration template
├── alembic.ini
├── alembic/versions/                # Migration history
├── app/
│   ├── api/route.py                 # Mounts /auth, /users, /cameras
│   ├── api/v1/                     # HTTP endpoints
│   ├── core/                       # Settings, DB engine, JWT, admin seed
│   ├── dependecies/auth.py          # Current-user authentication
│   ├── schemas/                    # Request/response contracts
│   ├── models/                     # SQLAlchemy tables
│   ├── repositories/               # Database reads/writes
│   ├── services/                   # Auth, cameras, validation, storage
│   │   ├── ppe_event_persistence.py # Durable outbox, PostgreSQL and snapshot tasks
│   │   └── ppe_snapshot_service.py  # JPEG encoding and Cloudinary uploads
│   ├── messaging/rabbitmq.py        # API command publisher
│   └── deepstream_worker/
│       ├── main.py                 # Separate worker entry point
│       ├── camera_manager.py       # Source/session lifecycle and callbacks
│       ├── camera_source.py        # H265 RTSP source bin
│       ├── pipeline.py             # Shared inference/tracking/display
│       ├── ppe_processor.py        # Frame queue and per-camera decision sessions
│       ├── ppe_association.py      # Assigns PPE evidence to tracked people
│       ├── ppe_decision_engine.py  # In-memory temporal violations and recovery
│       ├── worker_lock.py          # PostgreSQL advisory ownership lock
│       ├── messaging/              # Camera command consumer
│       └── model_configs/
│           ├── best.onnx
│           ├── config_pgie_ppe.txt
│           ├── labels_ppe.txt
│           ├── config_tracker_NvSORT.yml
│           └── parser/             # C++ parser and Makefile
├── data/                           # Runtime SQLite outbox and staged JPEG evidence
└── tests/                          # PPE, callback, recovery, and DB lifecycle tests
```

Compiled `.so` files are ignored by Git. A fresh checkout needs the parser built
and the tracker library resolved from the installed SDK even if those binaries
already exist on a developer's machine.

## Full application flow

### Architecture

```mermaid
flowchart TD
    Client[Frontend / Swagger / curl] --> API[FastAPI: /api/v1]
    API --> Auth[JWT authentication and Pydantic validation]
    Auth --> Service[User / camera services]
    Service --> Repo[SQLAlchemy repositories]
    Repo <--> DB[(PostgreSQL)]
    Service --> Probe[OpenCV stream probe]
    Probe --> Cloud[Optional Cloudinary thumbnail]
    Service --> Publisher[aio-pika publisher]
    Publisher --> Broker[RabbitMQ: camera.commands]
    Broker --> Consumer[DeepStream command consumer]
    Consumer --> Manager[CameraManager]
    Manager <--> DB
    Manager --> Pipeline[Dynamic GPU pipeline]
    Cameras[H265 RTSP cameras] --> Pipeline
    Pipeline --> Display[Local tiled display]
    Pipeline --> Logs[PPE boxes and tracker IDs in logs]
    Pipeline --> Frames[Tracker probe: complete frames]
    Frames --> Queue[Bounded frame queue: 2000 entries]
    Queue --> Association[PPE association worker]
    Association --> Decisions[Per-camera PPE event engines]
    Decisions --> Outbox[(Durable SQLite action outbox)]
    Outbox --> Writer[Async PostgreSQL writer with ordered retries]
    Writer --> DB
    Decisions --> Requests[Request matching evidence frame]
    Requests --> Capture[RGBA probe: owned CPU image copy]
    Capture --> Snapshots[Separate snapshot task and staged JPEGs]
    Writer --> Snapshots
    Snapshots --> CloudEvidence[Cloudinary violation snapshots]
    CloudEvidence --> DB
    Decisions --> EventLogs[CREATED / RESOLVED event logs]
```

There are **two Python processes**. Uvicorn does not start DeepStream. Both read
the same `.env` and use the same database/broker. HTTP handling follows
`route → service → repository → PostgreSQL`; video runs independently.

### 1. API startup and authentication

1. `main.py` loads settings, creates FastAPI, registers `/api/v1` routes and CORS.
2. Startup attempts to create the target database if missing, checks PostgreSQL,
   and creates missing model tables with `create_all()`. Apply Alembic first to
   keep the schema versioned.
3. It attempts to seed `admin@example.com` / `Password123` if that email is absent,
   then opens the RabbitMQ publisher connection and command exchange.
4. Registration hashes passwords. Login verifies the hash and returns access and
   refresh JWTs. Refresh exchanges a valid refresh token for a new token pair.
5. Camera routes require an access token. The authentication dependency checks
   signature, expiry, token type, email, and user ID against the database.

Startup catches initialization failures and can still serve HTTP. `GET /` checks
HTTP availability, not database/broker health. The seeded account is an ordinary
user record; there is no implemented admin-role authorization layer.

### 2. Camera validation and registration

1. The client sends `POST /api/v1/cameras` with a name and stream URL.
2. By default, an `RTSP_STREAM` is validated before saving. The validator checks
   scheme/host, probes TCP reachability, and runs OpenCV/FFmpeg in a background
   thread to read video for about five seconds.
3. It reports resolution/FPS and retains a frame. With Cloudinary configured,
   the service JPEG-encodes that frame, uploads it to `camera_thumbnails`, and
   stores the HTTPS thumbnail URL.
4. The camera is committed with runtime `status=OFFLINE` and, by default,
   `desired_state=STOPPED`.
5. The API publishes `camera.created`. The current consumer logs this as an
   unknown event. Registration does not start video; call `/start` next.

`?verify_stream=false` skips the registration probe/thumbnail. It allows API
checks with an unavailable stream; actual processing still needs H265 RTSP.

### 3. Start command and session assignment

1. `POST /api/v1/cameras/{camera_id}/start` saves `desired_state=RUNNING` and
   publishes `camera.start`. The response shows the saved request and latest
   runtime status; execution is asynchronous.
2. The API uses durable topic exchange `camera.commands` by default. The worker
   declares durable queue `deepstream.camera.commands`, bound with `camera.*`.
   Messages use persistent delivery and contain identifiers:

   ```json
   {
     "event_id": "<event UUID>",
     "event_type": "camera.start",
     "camera_id": "<camera UUID>"
   }
   ```

3. The consumer dispatches to `CameraManager`. An asyncio lock serializes
   start/stop operations. The manager ignores an already-running start, checks
   five-camera capacity, and reads the RTSP URL from PostgreSQL. A delayed start
   for a camera whose saved desired state is `STOPPED` is ignored.
4. It generates a session UUID, attaches a source to the lowest free slot
   (`0`–`4`), creates a `camera_sessions` row, increments worker camera count,
   and records `CONNECTING` status/runtime history.
5. A failed session creation removes the source. Camera/session identity is
   preserved in callbacks so late notifications from older sessions are ignored.
6. The pipeline registers a PPE decision engine with the same camera/session
   identity and a separate tracking-session UUID. Decisions stay inactive until
   the database camera session commits, avoiding a foreign-key race. If source
   attachment fails, it closes the decision session as part of rollback.

### 4. Video, inference, tracking, and display

Each camera uses this source chain:

```text
rtspsrc (TCP, latency=200 ms)
  → queue → rtph265depay → queue → h265parse → queue → nvv4l2decoder
  → source-bin output → nvstreammux.sink_<source_index>
```

All sources feed one permanent shared chain:

```text
nvstreammux → nvinfer → nvtracker
  → evidence nvvideoconvert → RGBA capsfilter → nvmultistreamtiler
  → display nvvideoconvert → RGBA capsfilter → nvdsosd → nveglglessink
```

| Stage | Behavior here |
| --- | --- |
| `nvstreammux` | Legacy mux, five-camera capacity, 1280×720 output, live sources, 40,000 µs push timeout |
| `nvinfer` / PGIE | Loads the PPE model/config and attaches detection metadata |
| Custom YOLO parser | Decodes outputs, thresholds proposals, performs class-wise non-maximum suppression |
| PGIE probe | Maps source slots to camera IDs; logs class, label, confidence and `(left, top, width, height)` boxes |
| `nvtracker` | NvSORT config and SDK tracking library; tracker size 640×384 on GPU 0 |
| Tracker metadata probe | Runs after the evidence RGBA converter and before tiling; submits complete frames and copies requested evidence; unassigned ID becomes `None` |
| PPE decision worker | Associates equipment with tracked people, checks temporal evidence, logs transitions, and submits UTC payloads to the durable outbox |
| Persistence tasks | Write ordered lifecycle actions to PostgreSQL, throttle updates, and upload staged snapshots separately |
| Tiler/converter/OSD | Makes a 1280×720 grid, converts to RGBA, draws overlays |
| `nveglglessink` | Local display on the worker machine; requires display access |

The grid changes between 1×1, 1×2, 2×2, and 2×3 based on the highest occupied
slot. Gaps can remain after removing a camera.

The tracker probe advances through every object, including filtered components,
and timestamps each submitted frame with `time.monotonic()`. Submission is
nonblocking: a full PPE queue drops the incoming frame and logs a warning.
The PPE worker starts after the pipeline's `PLAYING` request passes the immediate
failure check. An immediate failure returns the pipeline to `NULL` and cleans up
the GLib loop; later asynchronous errors go through the bus callback.

The supplied [PPE config](app/deepstream_worker/model_configs/config_pgie_ppe.txt)
uses batch size 5, FP16 (`network-mode=2`), ten classes, every-frame inference
(`interval=0`), and threshold 0.25. The parser performs NMS, so `cluster-mode=4`.
Class order in [labels_ppe.txt](app/deepstream_worker/model_configs/labels_ppe.txt):

| Class ID | Label | Class ID | Label |
| --- | --- | --- | --- |
| 0 | Hardhat | 5 | Person |
| 1 | Mask | 6 | Safety Cone |
| 2 | NO-Hardhat | 7 | Safety Vest |
| 3 | NO-Mask | 8 | machinery |
| 4 | NO-Safety Vest | 9 | vehicle |

The parser header defines ten classes, fourteen output attributes (four box
values plus ten scores), and 8,400 prediction locations for a 640×640 model.
The model output, header, labels, and config must agree. Source comments still
mention an older fourteen-class layout; header constants determine compilation.

### 5. Frame status, errors, stop, update, and delete

The manager's frame callback moves notifications from streaming threads onto
asyncio. When invoked, it counts frames and persists the first immediately,
then at most once per second. The repository updates session count/FPS, camera
timestamps and `ONLINE` status. The decoded-frame probe invokes that callback
for each buffer and returns `Gst.PadProbeReturn.OK` to keep video flowing.

```text
Registration:  desired_state=STOPPED, status=OFFLINE
Start saved:   desired_state=RUNNING, status may still be OFFLINE
Source added:  status=CONNECTING
Frame saved:   status=ONLINE
Stop saved:    desired_state=STOPPED
Stop handled:  status=OFFLINE, session=STOPPED
Source error:  status=ERROR, session=ERROR
```

- **Stop:** saves `STOPPED`, publishes `camera.stop`, removes the source, releases
  its mux pad/slot, closes the PPE decision session, persists final metrics, and
  closes database session/history. Pending frames for that removed decision
  session are ignored.
- **Update:** saves fields and publishes `camera.updated`. The worker restarts an
  active camera to reload its URL. Inactive cameras stay inactive. Use `/start`
  and `/stop` for execution; a desired-state `PATCH` does not reliably control it.
- **Restart:** `camera.restart` closes the old session and creates another. The
  consumer supports it, but there is no HTTP `/restart` route.
- **Delete:** deletes the camera and publishes `camera.deleted` to remove its
  source. Related records use cascading foreign keys.
- **Error:** GStreamer source errors reach the matching camera/session; shared
  element failures affect active cameras. Sources are removed and errors saved.
  There is no automatic retry.
- **Shutdown:** closes the consumer, sets the shared pipeline to `NULL`, stops
  accepting PPE frames, and drains accepted frames while decision sessions remain
  available. It then closes the PPE sessions, removes sources, stops the GLib
  loop, and closes managed database sessions. Persistence drains its action and
  snapshot jobs before the worker is marked offline; failed jobs stay in the
  local outbox for replay.
  Desired states remain saved. Startup closes this worker's abandoned database
  sessions as `ERROR` and restores cameras whose desired state is `RUNNING`
  before consuming commands.

## PPE association and decision events

The PPE path separates temporal decisions from persistence:

| Module | Responsibility |
| --- | --- |
| [ppe_processor.py](app/deepstream_worker/ppe_processor.py) | One shared bounded frame queue and worker thread; a separate engine for each camera/session |
| [ppe_association.py](app/deepstream_worker/ppe_association.py) | Matches helmet, mask, and vest evidence to person boxes and produces `PRESENT`, `MISSING`, or `UNKNOWN` observations |
| [ppe_decision_engine.py](app/deepstream_worker/ppe_decision_engine.py) | Maintains evidence windows, creates/updates/resolves violations, and expires tracks; performs no file or database writes |
| [ppe_event_persistence.py](app/services/ppe_event_persistence.py) | Accepts thread-safe durable actions, writes PostgreSQL transactions, and runs a separate snapshot consumer |
| [ppe_event_repository.py](app/repositories/ppe_event_repository.py) | Idempotent lifecycle writes, final statistics, snapshot references, and worker-scoped recovery |
| [ppe_snapshot_service.py](app/services/ppe_snapshot_service.py) | Encodes owned BGR frames as JPEG and uploads evidence to Cloudinary |

### Frame queue and session isolation

`MultiCameraPPEProcessor` receives camera ID, session ID, monotonic timestamp, and
the complete frame's object list. It copies the object dictionaries into a queued
tuple, keeping the submitted list independent of later caller changes. The queue
holds at most **2,000 frames across all cameras**. A full queue rejects the new
frame and increments the in-memory `dropped_frames` counter.

The worker ignores frames whose camera is no longer registered or whose session
ID differs from the current decision session. Each camera's evidence history and
open events are isolated in its own engine; PPE association is shared and
stateless. Registering a new session for an existing camera closes the old engine
with `SESSION_REPLACED` before installing the replacement.

Track expiration runs periodically using the same monotonic clock as frame
timestamps. It pauses while the queue is backlogged and while shutdown drains
frames. This avoids expiring tracks ahead of queued evidence, but continuous
backlog can delay expiration for all cameras.

### Association and confidence policy

Person detections need class ID `5`, a valid track ID, and a finite box with
positive width/height. Fresh person detections and PPE evidence require detector
confidence between **0.40 and 1.0**. With the default
`allow_tracker_only_persons=True`, a tracked person whose detector confidence is
the DeepStream `-0.1` sentinel can also anchor fresh PPE evidence. This fallback
requires a valid track and box; negative-confidence PPE detections are rejected.
Set the association option to `False` to require fresh person detector confidence.

The matcher checks PPE-box containment in the person box and expected body
regions: helmet near the head, mask near the face, and vest near the torso.
Its defaults are minimum containment **0.50**, minimum match score **0.65**,
ownership ambiguity margin **0.08**, and conflicting-evidence margin **0.15**.
Scores combine geometry and detector confidence; they are heuristic evidence
strengths. Similar matches between overlapping people are discarded. Agreeing
detections use the strongest evidence; conflicting states without a clear winner
produce `UNKNOWN`.

No associated PPE evidence means `UNKNOWN`; it does not imply missing equipment.
Explicit `NO-Hardhat`, `NO-Mask`, and `NO-Safety Vest` detections provide `MISSING`
evidence. Safety cones, machinery, and vehicles do not contribute to PPE decisions.

### Temporal decision settings

The pipeline uses these defaults, configured in `register_camera()` in
`ppe_processor.py`:

| Setting | Default | Behavior |
| --- | --- | --- |
| `window_seconds` | 1.5 s | Recent evidence considered for a new violation |
| `min_observations` | 5 | Minimum known-state observations for violation or recovery |
| `min_evidence_seconds` | 0.8 s | Minimum span between first and last known-state samples |
| `violation_threshold` | 0.80 | At least 80% of known states must be `MISSING` to open a violation |
| `recovery_window_seconds` | 2.0 s | Recent evidence considered for compliance recovery |
| `recovery_threshold` | 0.80 | At least 80% of known states must be `PRESENT` to resolve for compliance |
| `track_timeout_seconds` | 3.0 s | Time without a person observation before track expiration, once expiration is allowed |

Both violation and recovery require the minimum sample count and evidence span.
The two-second recovery window is a lookback window, rather than a mandatory
two-second wait. `UNKNOWN` samples are counted in event statistics but excluded
from the `MISSING`/`PRESENT` ratios and evidence requirements. Observations older
than a track's most recent timestamp are ignored.

At most one event is open for `(tracking_session_id, camera_id, person_track_id,
ppe_type)`. Each occurrence has its own event UUID. After compliance recovery,
a new violation for the same track gets a new UUID; the PostgreSQL unique index
applies only to `OPEN` events. Continued missing evidence updates the existing
open event. Violation types are
`NO_HARDHAT`, `NO_MASK`, and `NO_SAFETY_VEST`. The processor logs `CREATED` and
`RESOLVED` actions. `UPDATED` actions are admitted to persistence at most once
per event every five seconds, without per-frame event-transition logs. Creation
and resolution always include complete event payloads and bypass that throttle.

| Resolution reason | Trigger |
| --- | --- |
| `PPE_COMPLIANCE_RESTORED` | Sufficient recent `PRESENT` evidence |
| `PERSON_LEFT_CAMERA` | Person track exceeds the inactivity timeout |
| `STREAM_STOPPED` | Camera removed, including source-add rollback after decision registration |
| `SESSION_REPLACED` | Existing camera receives a different decision session |
| `PIPELINE_STOPPED` | Processor shutdown completes its queue drain |
| `SESSION_INTERRUPTED` | Startup recovery finds an open PostgreSQL event linked to this worker's inactive camera session |

### PostgreSQL persistence and UTC timestamps

[PPEEvent](app/models/ppe_event.py) links each occurrence to its camera and database
camera session. `session_id` is the `CameraSession.id` foreign key;
`tracking_session_id` is the independent UUID generated for the decision engine.
The repository uses event UUIDs for idempotent retries and ignores stale updates
to resolved events. Resolution saves the final confidence, evidence counts,
ratio, and observation times along with the status and reason.

Temporal windows still use `time.monotonic()`. The probe captures a UTC wall-clock
time alongside each frame's monotonic timestamp. The processor retains those
pairs for the evidence window, preserves the actual first missing observation
and confirmation times, and supplies timezone-aware UTC datetimes for database
fields. Later updates do not replace creation timestamps.

The decision engine performs no JSON, PostgreSQL, or Cloudinary operations.
Its action sink synchronously admits lifecycle payloads to a small SQLite outbox,
then an async task on the worker's existing event loop writes PostgreSQL
transactions. SQLite admission uses a thread lock, WAL, and `synchronous=FULL`;
this is a local disk write from the decision worker, rather than a database/network
call from the GStreamer probe. Updates are throttled before admission.

The outbox defaults to `data/ppe_outbox.sqlite3`, relative to the worker's working
directory; set `PPE_OUTBOX_PATH` to change it. Creation/resolution jobs are retained
until their PostgreSQL transaction commits. Database failures retry with capped
backoff, preserving FIFO action order so resolution cannot overtake creation.
Pending actions replay on restart. Preserve the outbox and its WAL files while
jobs are outstanding. Extended outages can grow the outbox; disk-capacity
monitoring remains necessary.

Existing `data/ppe_events_<camera_id>.json` files remain untouched as legacy
artifacts. The worker no longer reads or writes them. Their old monotonic times
cannot be converted reliably to historical UTC times, so automatic import is not
included.

Verify recent persisted events in PostgreSQL:

```sql
SELECT id, camera_id, person_track_id, ppe_type, violation_type, status,
       confidence, confirmed_at, resolved_at, snapshot_url, snapshot_captured_at
FROM ppe_events
ORDER BY confirmed_at DESC
LIMIT 20;
```

### Cloudinary evidence snapshots

Configure all three `CLOUDINARY_*` settings in `.env` to enable evidence uploads.
Without them, PostgreSQL events still persist and snapshot fields stay empty.
Credentials are read from application settings; uploads use JPEG quality 85 and
stable event-based IDs under `ppe_events/<camera_id>`.

When a violation is confirmed, the decision worker requests the **next frame that
still matches the same camera session, person track, and missing PPE type**. The
probe copies that frame from the individual RGBA batch surface before tiling and
converts the owned copy to BGR. It never sends a borrowed GPU surface to a
background task. On x86, the evidence converter uses unified CUDA memory; on
Jetson, the mapped surface is unmapped after copying. These requirements follow
[NVIDIA's surface access API](https://docs.nvidia.com/metropolis/deepstream/9.0/python-api/PYTHON_API/Methods/methodsdoc.html#get-nvds-buf-surface).

A bounded thread-safe queue accepts up to 32 CPU snapshots. If full, the capture
request stays pending for a later matching frame. Requests expire after five
seconds, or when the event resolves; if the person/PPE is no longer visible,
the event can remain without a snapshot. The selected frame is shortly after
confirmation, rather than a retained historical confirmation buffer.

A separate async task JPEG-encodes and stages images under `data/ppe_snapshots/`,
records snapshot jobs in the SQLite outbox, and waits for the event commit before
uploading. Uploads run outside the event loop. Failures retain staged images and
retry with backoff; staged jobs survive restart. A crash before the CPU image is
staged can lose that snapshot, while the event action remains durable. Successful
uploads save `snapshot_url`, `snapshot_public_id`, and the frame's actual UTC
`snapshot_captured_at` on the same event, then remove the local job/image.
`clip_url` remains empty; video clips are not captured.

### Recovery and shutdown

Startup first closes this worker's abandoned database camera sessions, then
replays its pending outbox actions. PPE recovery resolves `OPEN` events linked
to this worker's inactive camera sessions as `SESSION_INTERRUPTED`, using their
last persisted observation time. Events linked to other workers or still-active
sessions are preserved. New decision engines start with empty tracking history.

Graceful pipeline shutdown drains accepted frames before closing engines and
emitting final `PIPELINE_STOPPED` actions. New frame submissions are rejected once
processor shutdown begins. The decision-thread join has a five-second timeout.
The persistence tasks then have 30 seconds to drain; if they time out, shutdown
raises `PPE persistence drain timed out; pending jobs remain in the outbox`.
Durable pending jobs replay on the next successful startup.

## Database tables

| Table | Stored information / current use |
| --- | --- |
| `users` | Profile and password hash |
| `cameras` | URL, location, flags, thumbnail, desired/runtime state, timestamps |
| `deepstream_workers` | Identity, status, capacity, active count, heartbeat timestamp |
| `camera_sessions` | Camera/worker/pipeline/slot, start/stop, state, count/FPS, errors |
| `camera_status_logs` | Time intervals for camera status changes |
| `camera_runtime_logs` | Lifecycle events with session/worker links and slot metadata |
| `detection_events` | Earlier generic detection schema; worker writes are not connected |
| `ppe_events` | Confirmed PPE occurrences, camera/session/track identity, evidence statistics, UTC lifecycle times, resolutions, and Cloudinary snapshot references |

Partial unique indexes prevent duplicate open sessions for a camera and duplicate
open worker/pipeline/slot assignments. Startup recovery releases abandoned slots
while preserving session history. A PostgreSQL advisory lock keeps one worker
process active for the deployment's shared camera command queue.

PPE violations use `ppe_events`; the generic `detection_events` table remains
unconnected. The authenticated `GET /api/v1/cameras/summary` response includes
`ppe_event_count`: the total persisted PPE occurrences across all cameras,
including both `OPEN` and `RESOLVED` events. It counts event rows, rather than
people or individual detection frames, and returns `0` when the table is empty.
The dashboard's **Total PPE Events** card reads this field and refreshes every
ten seconds while Live Updates is enabled. This total covers all stored history;
the dashboard time filter does not restrict it. Event listings and charts do not
yet read persisted PPE history.

## How to run

Run commands from the backend directory:

```bash
cd camera_recognization_backend
```

For **API-only development**, follow steps 1–4 and 7, then try the HTTP examples.
For **GPU camera processing**, complete all steps, including the model/parser setup.

### 1. Install system prerequisites

For the API on Ubuntu:

```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip
```

For the worker, install the compatible NVIDIA driver, CUDA, TensorRT and
DeepStream using the version-specific NVIDIA guide above. Also install OS
bindings/tools and the parser compiler:

```bash
sudo apt install python3.10-venv python3-dev python3-gi python3-gst-1.0 \
  gir1.2-gstreamer-1.0 gir1.2-gst-plugins-base-1.0 \
  gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
  gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav \
  build-essential
```

This command installs ordinary OS packages, not NVIDIA plugins or the SDK.

### 2. Create an environment and install Python packages

Choose one setup. For API only:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

For DeepStream 7.1, use Python 3.10 and expose OS GStreamer bindings:

```bash
python3.10 -m venv --system-site-packages .venv
source .venv/bin/activate
python -m pip install --upgrade pip
cat > /tmp/bput-deepstream-constraints.txt <<'EOF'
numpy<2
EOF
python -m pip install -r requirements.txt -c /tmp/bput-deepstream-constraints.txt
```

The NumPy constraint follows `pyds` 1.2.0 requirements and lets pip select a
compatible OpenCV version. If `.venv` already exists, check its Python version and
system-package visibility; choose a separate compatible environment if needed.

If `pyds` is not available to this interpreter, download the Python 3.10 wheel for
your architecture from NVIDIA's `v1.2.0` release and install it in the environment.
Replace this example path with the actual wheel filename:

```bash
python -m pip install /path/to/downloaded/pyds-1.2.0-compatible-wheel.whl
```

Verify consistency and worker imports:

```bash
python -m pip check
python -c "import gi, pyds; gi.require_version('Gst', '1.0'); from gi.repository import Gst; Gst.init(None); print(Gst.version_string())"
```

Activate the environment in every terminal below. `gi` and `pyds` must import in
the interpreter running the worker. API-only setups can skip the worker import.

### 3. Configure `.env`

Copy the example without overwriting existing configuration and generate a secret:

```bash
cp -n .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"
```

Edit `.env` with service URLs and use the generated value for `SECRET_KEY`.
Do not commit real credentials.

| Variable | Purpose | Example/default |
| --- | --- | --- |
| `DATABASE_URL` | Async PostgreSQL URL including database name | `postgresql+asyncpg://postgres:change-me@localhost:5432/ppe_detection` |
| `SECRET_KEY` | JWT signing secret | Generated random value |
| `ALGORITHM` | JWT algorithm | `HS256` |
| `EXPIRE` | Access-token lifetime, minutes | `1440` |
| `REFRESH_EXPIRE` | Refresh-token lifetime, minutes | `10080` |
| `RABBITMQ_URL` | Shared API/worker AMQP connection | `amqp://guest:guest@localhost:5672/` |
| `CAMERA_COMMAND_EXCHANGE` | Shared topic exchange | `camera.commands` |
| `DEEPSTREAM_WORKER_NAME` | Worker database identity | `deepstream-default` |
| `PPE_OUTBOX_PATH` | Durable action/snapshot outbox; relative paths use the worker working directory | `data/ppe_outbox.sqlite3` |
| `CAMERA_RECONCILE_SECONDS` | Defined setting; no reconciliation scheduled in current worker | `10` |
| `CAMERA_FRAME_TIMEOUT_SECONDS` | Defined setting; no no-frame timeout enforced in current worker | `30` |
| `CLOUDINARY_CLOUD_NAME` | Optional Cloudinary cloud name | Empty |
| `CLOUDINARY_API_KEY` | Optional Cloudinary API key | Empty |
| `CLOUDINARY_API_SECRET` | Optional Cloudinary API secret | Empty |

Set all three Cloudinary values for registration thumbnails and PPE evidence; otherwise uploads
are skipped. URL-encode special characters in connection URL credentials, for
example `@` as `%40`.

Run both processes from the backend directory: `.env` is resolved relative to the
working directory. Model/tracker paths are code/config values, not `.env` settings.

### 4. Start PostgreSQL/RabbitMQ and migrate

Skip container creation if services already exist and use their actual URLs.
For a fresh Docker setup, these values match `.env.example`:

```bash
docker run -d --name bput-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=change-me \
  -e POSTGRES_DB=ppe_detection \
  -p 127.0.0.1:5432:5432 \
  -v bput-postgres-data:/var/lib/postgresql/data \
  postgres:16

docker run -d --name bput-rabbitmq --hostname bput-rabbitmq \
  -p 127.0.0.1:5672:5672 \
  -p 127.0.0.1:15672:15672 \
  -v bput-rabbitmq-data:/var/lib/rabbitmq \
  rabbitmq:3-management
```

Check readiness; repeat if still starting:

```bash
docker exec bput-postgres pg_isready -U postgres -d ppe_detection
docker exec bput-rabbitmq rabbitmq-diagnostics -q ping
```

RabbitMQ management: <http://localhost:15672>, `guest` / `guest` for this local
setup. Restart existing containers with:

```bash
docker start bput-postgres bput-rabbitmq
```

Once the target database exists, apply migrations **before starting API/worker**:

```bash
python -m alembic upgrade head
python -m alembic current
```

Current migration chain:

```text
a1b2c3 (users) → d4e5f6 (cameras) → g7h8i9 (detection events)
  → j1k2l3 (status logs) → m4n5o6 (worker/session/runtime persistence)
  → n7p8q9 (PPE events and snapshot references)
```

API `create_all()` does not record a migration revision or alter existing tables.
See troubleshooting for unversioned databases. For future model changes, generate
and review an Alembic migration before applying it.

### 5. Prepare model paths, parser, tracker, and frame callbacks

**Verify inference/tracker files.**
[pipeline.py](app/deepstream_worker/pipeline.py) resolves the supplied
`config_pgie_ppe.txt` and `config_tracker_NvSORT.yml` relative to its own
`model_configs` directory. It uses the installed SDK tracker library and reports
missing config/library files before constructing the pipeline.

Confirm the SDK tracker library exists:

```bash
ls /opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so
```

In `config_pgie_ppe.txt`, these paths resolve relative to the config directory:

- `onnx-file`: `best.onnx` or another compatible PPE model.
- `model-engine-file`: writable TensorRT engine output/cache path.
- `labelfile-path`: `labels_ppe.txt`.
- `custom-lib-path`: the parser `.so` built below.

Keep model class count/order consistent with config/labels/header and batch size
5 consistent with the pipeline. An engine filename containing `b5` does not prove
ONNX batch compatibility; verify the export supports batch 5 or a compatible
dynamic batch. Without a usable engine, `nvinfer` attempts to build from ONNX.
Allow extra first-start time and build for the target GPU/runtime.

The supplied ONNX model has dynamic batch, height and width dimensions. Keep
`infer-dims=3;640;640` in the inference config so DeepStream supplies concrete
spatial dimensions when building the engine; `batch-size=5` sets its maximum
batch. A missing engine cache warning on the first run is expected while the
engine is built and saved.

**Build the parser.** The Makefile uses DeepStream headers, `/usr/local/cuda/include`
and `/usr/local/cuda/lib64`:

```bash
make -B -C app/deepstream_worker/model_configs/parser \
  DS_PATH=/opt/nvidia/deepstream/deepstream-7.1
ls app/deepstream_worker/model_configs/parser/libnvdsinfer_custom_yolov11.so
ldd app/deepstream_worker/model_configs/parser/libnvdsinfer_custom_yolov11.so
```

If CUDA is elsewhere, update the Makefile include/library paths. Its `CUDA_VER`
variable currently does not change those paths. Rebuild after header/parser or
SDK/toolchain changes.

**Check tracker property support.** `pipeline.py` sets `enable-batch-process` and
`enable-past-frame` only when the installed plugin exposes them. They are absent
from the local DeepStream 7.1 plugin. Inspect available properties with:

```bash
gst-inspect-1.0 nvtracker
```

**Decoded-frame callback.** In
[camera_source.py](app/deepstream_worker/camera_source.py), `_on_frame()` counts
buffers, records arrival time, and notifies the manager. It returns
`Gst.PadProbeReturn.OK` even if the notification raises an exception.
`DynamicPipeline.add_camera()` wraps this callback with camera/session identity
so the manager can ignore late notifications from an older session.

### 6. Verify and start the DeepStream worker

Use a Linux graphical session with GPU/display access. In a worker terminal:

```bash
source .venv/bin/activate
nvidia-smi
for element in rtspsrc rtph265depay h265parse nvv4l2decoder nvstreammux \
  nvinfer nvtracker nvmultistreamtiler nvvideoconvert nvdsosd nveglglessink; do
  gst-inspect-1.0 "$element" > /dev/null || break
done
printenv DISPLAY
python -m app.deepstream_worker.main
```

`nvidia-smi` applies to discrete-GPU setups; use platform NVIDIA tools for Jetson.
A nonempty `DISPLAY` does not alone prove access. An unset/inaccessible display
can prevent EGL startup. Headless operation needs an intentional sink change in
`pipeline.py`; there is no environment setting for that.

Apply migration `n7p8q9` before starting the updated API/worker:

```bash
python3 -m alembic upgrade n7p8q9
```

If startup reports that `ppe_events.snapshot_captured_at` is missing or that the
PPE database schema is outdated, run this command from the backend directory
using the same `.env` as the worker, then restart the worker. `create_all()`
cannot add this column to an existing table.

The worker
acquires its PostgreSQL advisory lock, starts the pipeline, closes abandoned
camera sessions, replays PPE outbox actions, recovers interrupted PPE events,
connects to RabbitMQ, restores requested cameras, and listens indefinitely. Look for
`RabbitMQ connected`, `DeepStream worker ready`, and
`Listening for camera commands...`. Start it before issuing camera commands.

Use one worker for this setup. A second worker connected to the same database
exits with `Another DeepStream worker is already running for this database`.
Stop an older worker before launching the updated entry point so it participates
in the same ownership lock.

### 7. Start the API in another terminal

From the backend directory:

```bash
source .venv/bin/activate
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

| URL | Purpose |
| --- | --- |
| <http://127.0.0.1:8000/> | Basic HTTP availability |
| <http://127.0.0.1:8000/docs> | Interactive Swagger UI |
| <http://127.0.0.1:8000/redoc> | ReDoc documentation |
| <http://127.0.0.1:8000/openapi.json> | Generated API schema |

```bash
curl http://127.0.0.1:8000/
```

Expected: `{"message":"Hello, World!"}`. Check startup logs separately for
PostgreSQL, admin-seeding, or RabbitMQ errors. `--reload` is for development.
The CORS list includes local frontend origins on ports 3000, 5173 and 5174;
check the exact allowlist in `main.py` for your frontend origin.

### 8. Stop processes and services

Stop API/worker with `Ctrl+C`, then stop the example containers:

```bash
docker stop bput-postgres bput-rabbitmq
```

Named volumes retain service data. Graceful worker shutdown drains queued PPE
frames, closes decision engines and database sessions, and drains persistence
tasks on `Ctrl+C` or `SIGTERM`. Preserve the local outbox and staged JPEGs in
`data/` to replay pending work; event history lives in PostgreSQL. On restart, cameras with desired state
`RUNNING` are restored automatically. Sessions left open by abrupt termination
are closed as errors before this worker's source slots are reused.
`Ctrl+Z` suspends the process and retains its database lock; use `fg` in the
original terminal, then `Ctrl+C`, to shut down a suspended worker.

## Try the API and camera lifecycle

These examples use Bash and the local API. Replace the camera URL/UUID and keep
API/worker logs visible.

### 1. Register or log in

Use the default account if startup seeded it:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@example.com","password":"Password123"}'
```

Or register your own account, then log in with its credentials:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"name":"Site Operator","email":"operator@example.com","password":"ExamplePassword123","phone":"+919876543210"}'
```

Login returns `access_token`, `refresh_token`, `token_type`, and `expires_in`.
The current implementation returns `expires_in` in minutes. Copy the access token:

```bash
ACCESS_TOKEN='paste-the-access-token-here'
```

In Swagger, click **Authorize** and supply the access token for camera requests.

### 2. Validate a real stream

```bash
curl -X POST http://127.0.0.1:8000/api/v1/cameras/validate-rtsp \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"rtsp_url":"rtsp://camera-user:camera-password@192.168.1.100:554/stream","timeout_seconds":5}'
```

Success includes `is_valid`, URL, resolution, dimensions, FPS, and a message;
failure returns HTTP 400. OpenCV success does not prove H265 worker compatibility.

### 3. Register the camera

```bash
curl -X POST http://127.0.0.1:8000/api/v1/cameras \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Main Gate","rtsp_url":"rtsp://camera-user:camera-password@192.168.1.100:554/stream","location_name":"Construction Site A","ppe_enabled":true,"fire_enabled":false,"smoke_enabled":false,"desired_state":"STOPPED","source_type":"RTSP_STREAM"}'
```

Copy `id` from the response:

```bash
CAMERA_ID='paste-the-camera-uuid-here'
```

For API-only checks with an unavailable camera, use the same request with URL
`http://127.0.0.1:8000/api/v1/cameras?verify_stream=false`.

### 4. Start and inspect

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/cameras/$CAMERA_ID/start" \
  -H "Authorization: Bearer $ACCESS_TOKEN"

curl "http://127.0.0.1:8000/api/v1/cameras/$CAMERA_ID" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

Repeat GET after worker handling. With setup corrections applied, expected
progress is `OFFLINE → CONNECTING → ONLINE`. Logs show RTSP linking, first mux/
display buffers, PPE boxes and track IDs, and `PPE camera registered`. With
sufficient explicit missing-equipment evidence, look for `PPE event:
action=CREATED` and the corresponding row in PostgreSQL `ppe_events`.
The display opens on the worker machine. Camera API responses do not expose PPE
event history.

In API-only mode, start saves state but cannot process video. A worker started
later restores cameras whose saved desired state is `RUNNING`. Repeated start/stop
requests publish another command even when the saved state already matches.

### 5. Stop

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/cameras/$CAMERA_ID/stop" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

After handling, the camera becomes `OFFLINE`, its session becomes `STOPPED`,
and its source slot is available again. Any open PPE events for the removed
decision session resolve with `STREAM_STOPPED` and are queued for PostgreSQL persistence.

## API reference

Paths below are relative to `/api/v1`.

| Method | Path | Purpose | Access token required |
| --- | --- | --- | --- |
| POST | `/auth/register` | Register user | No |
| POST | `/auth/login` | Get token pair | No |
| POST | `/auth/refresh` | Refresh via refresh-token JSON body | No access token; valid refresh token required |
| POST | `/auth/me` | Confirm authentication | Yes |
| GET | `/users` | List users | No, in current code |
| GET | `/users/{user_id}` | Get user | No, in current code |
| PATCH | `/users/{user_id}` | Update user | No, in current code |
| DELETE | `/users/{user_id}` | Delete user | No, in current code |
| POST | `/cameras/validate-rtsp` | Probe stream | Yes |
| POST | `/cameras` | Register camera | Yes |
| GET | `/cameras` | List cameras with `skip`/`limit` | Yes |
| GET | `/cameras/{camera_id}` | Camera config/latest status | Yes |
| PATCH | `/cameras/{camera_id}` | Update fields | Yes |
| DELETE | `/cameras/{camera_id}` | Delete camera | Yes |
| POST | `/cameras/{camera_id}/start` | Save RUNNING, publish start | Yes |
| POST | `/cameras/{camera_id}/stop` | Save STOPPED, publish stop | Yes |

The worker maintains runtime `status`; clients cannot supply it in camera PATCH.
Desired states are `RUNNING` and `STOPPED`. There are no registered routes for
live video, worker/session history, detection events, uploads, or restart.

## Verification

Run the PPE regression tests without GPU dependencies:

```bash
python -m unittest discover -s ./tests -p 'test_ppe_processing.py' -v
```

These cover complete frame submission, tracked-person confidence handling,
camera cleanup, startup failure, backlog expiration, shutdown draining, and
UTC timestamp preservation, session activation, and owned evidence-image copies.
The processing suite has 17 tests and requires no live camera, database, broker,
`gi`, or `pyds`; the image-copy test skips if NumPy/OpenCV are absent.

Run SQL lifecycle and durable-outbox tests:

```bash
python -m unittest discover -s ./tests -p 'test_ppe_persistence.py' -v
```

These eleven tests use SQLite transactions and temporary outboxes/staged JPEGs,
with mocked Cloudinary uploads. They check idempotence, repeat occurrences, UTC
times, final statistics, worker-scoped recovery, throttling, ordered retries,
snapshot commit ordering, restart replay, and actionable missing-schema errors.
They require application packages
and NumPy/OpenCV, but no external database/broker or Cloudinary credentials.
The migration's PostgreSQL DDL can be reviewed without applying it:

```bash
python -m alembic upgrade m4n5o6:n7p8q9 --sql
```

Run tests targeting the current manager interface:

```bash
python -m unittest discover -s ./tests -p 'test_camera_start_callbacks.py' -v
```

These use SQLite transactions and fake sources for session assignment, concurrent
starts, rollback, frame persistence, stale callbacks and errors. Pipeline tests
also check source/pad cleanup and need `gi`/`pyds`; they skip if imports are
unavailable. The decoded-frame probe test feeds real GStreamer buffers and
checks persisted frame counts and ONLINE status. These tests do not run real
inference or validate live camera/GPU/display.

Run the camera summary API tests:

```bash
python -m unittest discover -s ./tests -p 'test_camera_summary.py' -v
```

These two tests exercise the HTTP response with temporary SQLite rows and mocked
authentication. They verify numeric zero counts and counting open/resolved PPE
occurrences across multiple cameras, including repeat occurrences for a track.

Run the worker startup recovery and ownership/cleanup tests:

```bash
python -m unittest discover -s ./tests -p 'test_worker_recovery.py' -v
```

These check abandoned-session recovery, restoring requested cameras, worker
ownership locking, and cleanup after startup/service failures. Hardware-dependent
worker cleanup tests skip if `gi`/`pyds` cannot be imported.

`test_camera_lifecycle.py` targets an older reconciliation-oriented interface.
Its 17 tests currently fail in setup because `CameraManager` no longer accepts
`retry_delay`. Update it alongside any restoration of that implementation; it is
not a passing integration check for the current worker. Full discovery on system
Python ran 68 tests: 51 passed, including all 28 processing/persistence tests and
the current callback/recovery suites; the 17 older lifecycle tests failed on the
obsolete manager interface. The existing
`.venv` lacked application packages; install requirements before running the
database-backed suites in that environment.

For live integration, check services, migrate, run both processes, log in,
register/start an H265 camera, inspect display/logs/status, then stop and confirm
database session closure and `STREAM_STOPPED` PPE resolutions. Also check graceful
worker shutdown produces `PIPELINE_STOPPED` resolutions for remaining open PPE
events. Recorded test results do not verify live GPU inference or decision
accuracy on site footage.

## Troubleshooting

| Symptom | Check / action |
| --- | --- |
| Missing application Python modules | Activate intended environment, install requirements, use `python -m ...` from backend directory |
| Missing `gi` / `pyds` | Install OS bindings and matching NVIDIA wheel; confirm environment visibility |
| NumPy incompatibility with `pyds` 1.2.0 | Use the `numpy<2` constraint and `python -m pip check` |
| PostgreSQL connection/auth error | Check readiness, URL host/port/credentials, and database name |
| `Another DeepStream worker is already running` | Stop the existing worker; for a suspended shell job, use `fg` then `Ctrl+C`. The lock is released when that process exits |
| Existing tables but no Alembic revision | Inspect `python -m alembic current` / `heads`; do not stamp an unverified schema |
| Missing PGIE config/parser | Correct paths in step 5, build parser, inspect `ldd` |
| Unsupported tracker properties | Inspect `gst-inspect-1.0 nvtracker`; guard/remove the two calls in step 5 |
| Engine build/deserialization failure | Check ONNX batch/output, GPU/runtime versions, parser, writable cache; rebuild incompatible engine |
| `uq_camera_sessions_active_source` after a worker crash | Stop the old process and restart the updated worker; startup recovery closes its abandoned sessions before cameras are restored |
| Segmentation fault while building the dynamic ONNX engine | Ensure `infer-dims=3;640;640` supplies concrete spatial dimensions; check the DeepStream/TensorRT runtime if it still fails |
| EGL/display failure | Run with graphical display/GPU access; headless sink needs a code change |
| Unsupported RTSP codec | Configure H265; this source bin does not support H264 |
| Stuck CONNECTING despite frames | Check frame callback/database errors and confirm buffers reach the decoder output probe |
| `Expected a Gst.PadProbeReturn, but got NoneType` | Restart with the updated source probe, which returns `Gst.PadProbeReturn.OK` on every buffer |
| Registered camera does not start | Consumer does not handle `camera.created`; call `/start` |
| Saved command has no effect | Check shared broker/exchange, queue/consumer/logs; resend request because no reconciliation loop exists |
| Five-camera capacity reached | Stop another camera and resend `/start`; no automatic waiting-camera scheduler |
| Duplicate open-session/slot violation after crash | Confirm the old worker is stopped and the restarted worker uses the same configured worker identity; its startup recovery closes its abandoned database sessions |
| `PPE observation queue full` | The shared 2,000-frame queue is saturated; incoming frames are dropped. Inspect frame volume, processor errors, and local outbox disk latency |
| Person boxes but no PPE events | Check valid track IDs, association confidence/body regions, explicit `NO-*` detections, and minimum evidence count/span; missing detections produce `UNKNOWN` |
| No PostgreSQL PPE rows | Apply migration `n7p8q9`, check event-transition/outbox retry logs, database connectivity and camera session activation; no event is created without sufficient missing-PPE evidence |
| Saved `OPEN` event becomes `SESSION_INTERRUPTED` | Expected recovery for this worker's inactive database camera session; old tracking state is not restored |
| Snapshot URL remains empty | Check all Cloudinary settings, matching-frame availability, surface-copy/upload logs, and staged outbox jobs; snapshots can be absent if the person leaves before capture |
| `PPE persistence drain timed out` | PostgreSQL/Cloudinary work did not drain within 30 seconds. Preserve the local outbox and staged images, restore connectivity, and restart for replay |
| `PPE worker did not stop cleanly` | The queue drain exceeded the five-second worker join; inspect processor errors, backlog, and storage latency before retrying shutdown |
| Seeded account login unavailable | Check seeding/DB logs, account existence, and whether password changed |

For a database previously initialized by `create_all()`, only after verifying
that all existing tables, columns, constraints and indexes match revision
`j1k2l3`, record that baseline with `python -m alembic stamp j1k2l3`, then upgrade
head. Stamping records a version; it does not create missing schema objects.
Reconcile mismatches before stamping.

## Current limitations

These are implementation gaps, not features enabled by changing `.env`:

- The consumer handles start, stop, restart, update and delete; it does not
  handle `camera.created`.
- Commands dispatch directly. Start checks the current desired state, but there
  is no recurring reconciliation to recover every delayed command. After failed
  publication, the API log mentions reconciliation, but that fallback is absent
  in the current worker; resend the endpoint request.
- There is no recurring heartbeat, no-frame timeout or automatic camera retry.
  Startup recovery/restoration runs once, and the worker status is updated on
  startup and shutdown.
- Dispatch failures are logged inside the message processing context and normally
  acknowledged. There is no declared failed-command queue/application-level retry.
- Detection flags are stored but do not control per-camera pipeline branches.
  Fire/smoke inference, the generic `detection_events` table, frontend alerts,
  video uploads and browser video output are not connected to this runtime.
- Outbox admission is a synchronous local SQLite commit from the decision thread.
  Database/network writes are asynchronous, but disk failure can prevent durable
  admission and sustained outages can grow the outbox. Monitor storage capacity.
- Snapshots select a matching frame after confirmation. A person who leaves or
  recovers before capture may have no image; un-staged CPU images do not survive
  an abrupt process exit. Video clips and restricted image delivery are not implemented.
- One shared FIFO frame queue has no per-camera fairness or bounded latency.
  Queue saturation drops incoming frames, and sustained backlog delays track
  expiration for every camera.
- PPE geometry and temporal thresholds are starting defaults that need validation
  against site footage. `UNKNOWN` evidence alone does not resolve an open event
  while the person remains observed; `unknown_grace_seconds` is currently unused.
- Evidence windows use a monotonic clock; PostgreSQL events use UTC datetimes.
  Frontend event delivery and legacy JSON import are not implemented.
- User CRUD routes currently have no authentication dependency. Camera routes
  require authentication but do not enforce per-user ownership.

Use the explicit HTTP start/stop flow and inspect worker logs/session records
when commands or sources fail.
