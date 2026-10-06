# Camera recognition backend

FastAPI backend for users, authentication, camera registration, RTSP validation,
and camera control. PostgreSQL stores application data. The API publishes camera
commands to RabbitMQ, and a separate worker consumes them.

The worker runs one dynamic DeepStream pipeline with five camera slots. It reads
camera configuration from PostgreSQL and stores sessions, runtime events, status
history, frame timestamps/counts, and worker heartbeats. The current source chain
supports H265 RTSP streams; inference is not yet connected.

Camera creation defaults to `desired_state=STOPPED` and `status=OFFLINE`. Start
and stop endpoints save `RUNNING` or `STOPPED` and publish a RabbitMQ wake-up.
The worker reads the latest database state for each command, so delayed events
do not reverse a newer request. A camera becomes `ONLINE` only after decoded
frames arrive. Stop removes its source, closes its session, and sets `OFFLINE`.

Every ten seconds the worker reconciles saved desired states, including after a
restart or broker delivery failure. Source errors or thirty seconds without
frames close the session with `ERROR`; requested running cameras retry after
thirty seconds. Frame metrics are flushed every two seconds. Shutdown closes
sessions while preserving desired state for the next startup.

Only one worker process is supported per database. A PostgreSQL advisory lock
prevents a second process from taking commands or changing camera ownership.
Failed commands are preserved in `deepstream.camera.commands.failed`; database
reconciliation retries the associated camera state.

## Requirements

- Python 3.10 or newer.
- PostgreSQL and RabbitMQ, either installed locally or running in Docker.
- Linux with the NVIDIA DeepStream SDK, GStreamer/PyGObject bindings, and a compatible GPU for the worker.
- Optional Cloudinary credentials for thumbnail/image uploads.

Run the following commands from `camera_recognization_backend/`.

## 1. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Activate the virtual environment in each terminal used below. Using `python -m`
keeps commands attached to the active environment.

## 2. Configure the environment

For a new setup, copy the example file:

```bash
cp -n .env.example .env
```

This preserves an existing `.env`. Edit `.env` to match your services, and replace
`SECRET_KEY` with the output of:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

| Variable | Purpose | Example/default |
| --- | --- | --- |
| `DATABASE_URL` | Async PostgreSQL connection, including the database name | `postgresql+asyncpg://postgres:change-me@localhost:5432/ppe_detection` |
| `SECRET_KEY` | JWT signing secret | Generate a random value using the command above |
| `ALGORITHM` | JWT signing algorithm | `HS256` |
| `EXPIRE` | Access token lifetime in minutes | `1440` |
| `REFRESH_EXPIRE` | Refresh token lifetime in minutes | `10080` |
| `RABBITMQ_URL` | AMQP connection shared by the API and worker | `amqp://guest:guest@localhost:5672/` |
| `CAMERA_COMMAND_EXCHANGE` | Shared RabbitMQ topic exchange | `camera.commands` |
| `DEEPSTREAM_WORKER_NAME` | Stable worker identity | `deepstream-default` |
| `CAMERA_RECONCILE_SECONDS` | Desired-state reconciliation interval | `10` |
| `CAMERA_FRAME_TIMEOUT_SECONDS` | Maximum interval without decoded frames | `30` |
| `CLOUDINARY_CLOUD_NAME` | Optional Cloudinary cloud name | Empty |
| `CLOUDINARY_API_KEY` | Optional Cloudinary API key | Empty |
| `CLOUDINARY_API_SECRET` | Optional Cloudinary API secret | Empty |

The API and worker load the same `.env` configuration. Set all three Cloudinary values to enable image uploads;
otherwise uploads are skipped. URL-encode special characters in connection URL
usernames/passwords, such as `@` → `%40`.

Run both processes from the backend directory so they resolve the same `.env`.

## 3. Start PostgreSQL and RabbitMQ

Skip container creation if these services are already running, and configure
`.env` with their actual credentials and ports. For a fresh local Docker setup,
these containers match `.env.example`:

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

Wait for both services to become ready. Check with:

```bash
docker exec bput-postgres pg_isready -U postgres -d ppe_detection
docker exec bput-rabbitmq rabbitmq-diagnostics -q ping
```

Repeat the readiness checks if either service is still starting. The RabbitMQ
management UI is at <http://localhost:15672>, using `guest` / `guest` for this local
setup. To restart containers you already created:

```bash
docker start bput-postgres bput-rabbitmq
```

## 4. Apply database migrations

With the target database available, apply migrations **before starting the API**:

```bash
python -m alembic upgrade head
python -m alembic current
```

The current migration head is `m4n5o6`. This revision creates runtime tables,
adds indexes preventing duplicate active sessions/source assignments, and
normalizes legacy `ACTIVE` desired states to `RUNNING`. It also adopts runtime
tables already created by an earlier API startup without dropping their data. API startup also calls
`Base.metadata.create_all()`, which does not record an Alembic revision. Applying
the migration history first prevents an unversioned schema on a fresh database.

For future model changes, use:

```bash
python -m alembic upgrade head
python -m alembic revision --autogenerate -m "describe the model change"
# Review the generated migration before applying it.
python -m alembic upgrade head
```

## 5. Start the camera command worker

In one terminal, from the backend directory with the virtual environment active:

```bash
python -m app.deepstream_worker.main
```

Use the module command above so package imports resolve correctly. The worker
starts its DeepStream pipeline, registers its database identity, recovers stale
sessions, and restores requested cameras before accepting new commands. It
declares `deepstream.camera.commands` and binds it to the configured exchange
with `camera.*`. Start it before issuing commands for immediate processing.

Supported events are `camera.created`, `camera.start`, `camera.stop`,
`camera.updated`, `camera.restart`, and `camera.deleted`. Press `Ctrl+C` to stop
the worker. Session/status history stays in PostgreSQL and running requests
are restored on the next startup.

## 6. Start the API

In a second terminal, activate `.venv` and run from the backend directory:

```bash
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

- Swagger UI: <http://127.0.0.1:8000/docs>
- ReDoc: <http://127.0.0.1:8000/redoc>
- API prefix: `/api/v1`

On successful database initialization, startup attempts to seed a local admin
account with email `admin@example.com` and password `Password123`.

Check the HTTP server:

```bash
curl http://127.0.0.1:8000/
```

Expected response: `{"message":"Hello, World!"}`. This endpoint checks HTTP
availability; inspect startup logs for PostgreSQL/RabbitMQ connection failures.

To sign in using the seeded account:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@example.com","password":"Password123"}'
```

Use Swagger UI to register cameras and call
`POST /api/v1/cameras/{camera_id}/start` or `/stop`. Camera registration validates
the supplied stream by default; `?verify_stream=false` skips that probe for
testing command delivery without an accessible camera.

## Troubleshooting

- **`ModuleNotFoundError`**: activate `.venv` and reinstall with
  `python -m pip install -r requirements.txt`. `aio-pika` is needed by both the API
  and worker. Both load configuration through Pydantic settings.
- **Database connection/authentication errors**: check PostgreSQL readiness and
  the complete `DATABASE_URL`, including `/ppe_detection` (or your chosen database
  name). For an existing database, its credentials must match `.env`.
- **`relation "users" already exists` / `Target database is not up to date`**:
  inspect `python -m alembic current` and `python -m alembic heads`. A database
  created by `create_all()` may contain tables without an Alembic revision. Only
  after verifying that all existing columns, constraints, and indexes match
  revision `j1k2l3`, record that baseline with
  `python -m alembic stamp j1k2l3`, then run `python -m alembic upgrade head`.
  Stamping only records a revision; missing columns/tables must be reconciled
  before stamping.
- **RabbitMQ connection errors or no worker messages**: check broker readiness,
  load the same `RABBITMQ_URL` and `CAMERA_COMMAND_EXCHANGE` in both processes.
  If a publish fails, the saved desired state is picked up by reconciliation.
- **Camera never becomes `ONLINE`**: inspect `camera_sessions.error_message` and
  `camera_runtime_logs`. Ensure the camera serves H265 RTSP video and that the
  GPU/DeepStream plugins can decode it. An OpenCV validation success does not
  guarantee compatibility with the current H265-only worker.

Stop the API and worker with `Ctrl+C`. To stop the example service containers:

```bash
docker stop bput-postgres bput-rabbitmq
```

Their named volumes retain the PostgreSQL and RabbitMQ data.

## Verify the lifecycle

```bash
python -m unittest discover -s ./tests -p 'test_camera_lifecycle.py' -v
```

These tests exercise SQL transactions using SQLite and fake streaming sources;
they do not require a running camera, broker, or GPU. Start/stop API responses
show the saved request and latest observed status; streaming changes happen
asynchronously in the worker. `PATCH` can change the validated desired state,
but runtime status is maintained by the worker and cannot be edited by clients.
