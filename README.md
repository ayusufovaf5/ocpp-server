# opencpo-core

CSMS for EV charging stations (OCPP 1.6j).

Python 3.11+ · FastAPI · PostgreSQL 16 · layered architecture inspired by EvPointOCPP.

## How it fits together

```
Charge Point ──WS──► ocpp16/     (protocol + mapping)
                         │
                         ▼
                    services/    (business rules)
                         │
                         ▼
                 repositories/  (SQL)
                         │
                         ▼
                       db/       (models + engine)

REST clients ──HTTP──► api/  (/health, /version)
```

| Process | Port | Role |
| --- | --- | --- |
| `api` | 8000 | REST |
| `ocpp16` | 9000 | OCPP WebSocket + heartbeat monitor |

OCPP URL: `ws://host:9000/ocpp/{charge_point_id}` · subprotocol `ocpp1.6`

## Layout

```
api/             REST routes
ocpp16/          OCPP-J protocol, Pydantic messages, WS handler
services/        ChargerService, SessionService
repositories/    DB access
tasks/           heartbeat timeout monitor (runs inside ocpp16 process)
db/              SQLAlchemy models + engine
migrations/      Alembic
tests/
docs/adr/
```

## Behaviour (EvPoint-compatible)

- Boot → Accepted, interval 60s
- Authorize → always Accepted (`id_tag` stored as string on the session)
- No heartbeat for 120s → charger status `Unavailable`
- MeterValues: kWh→Wh, kW→W
- Status aliases: Finishing→Available, SuspendedEVSE→SuspendedEV

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

- REST: http://localhost:8000/health  
- OCPP: ws://localhost:9000/ocpp/CP_1  

Local dev (tests / lint):

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest -q
```

## License

Apache-2.0
