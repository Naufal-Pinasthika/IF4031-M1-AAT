# Tugas Besar IF4031-M1-AAT
To run:
```bash
cp .env.example .env
docker compose up --build
```

Set different values for `BMKG_API_KEY`, `PVMBG_TOKEN`, `AGGREGATOR_API_KEY`,
and `CLIENT_API_KEY` in `.env` before running.

P2 client API runs on port 8080. Read events with `GET /events?source=BMKG` or
`GET /events?source=PVMBG` and header `X-Client-Key`. Responses include source
status: `UNKNOWN`, `UP`, `STALE`, or `UNAVAILABLE`. `GET /source-status` returns both source
statuses. The status changes after the next polling cycle.

`AGGREGATOR_MAX_IN_FLIGHT` and `CLIENT_API_MAX_IN_FLIGHT` limit active data
requests. Extra requests receive HTTP 429. `PVMBG_DELAY_MIN_MS` and
`PVMBG_DELAY_MAX_MS` set the mock delay; use `3000` for both to fix it at 3 s.
