# uns-productivity — Production Run Logger

A Go HTTP function that tracks production runs by reading [UNS Framework](https://www.unsframework.com) `/program` topics from the shared Valkey cache (populated by [uns-framework](../uns-framework/)) and logging completed runs with throughput metrics to PostgreSQL.

## How It Works

```
POST /uns-productivity (via gateway or cron)
    │
    ▼
┌─────────────────────────────────────────────┐
│  uns-productivity (Go HTTP function)        │
│                                             │
│  1. Fetch config from Valkey (cached 30s)   │
│     → topic list, table name                │
│                                             │
│  2. Read /program topics from Valkey cache  │
│     → program_id, parts, progress, status   │
│                                             │
│  3. Track active runs per machine           │
│     → if run COMPLETED or CHANGED:          │
│       • calculate throughput metrics        │
│       • log run to Postgres                 │
│       • reset tracker for new run           │
│                                             │
│  4. INSERT completed run to Postgres        │
│     → parts, target %, duration, parts/hr   │
│                                             │
│  5. Return JSON summary                     │
└─────────────────────────────────────────────┘
         │              │
         ▼              ▼
   fnkit-cache      PostgreSQL
   (Valkey)         (uns_productivity table)
```

## What Gets Logged

A production run is logged when any of these occur:

| Trigger | Description |
| ------- | ----------- |
| `completed` | Parts completed reached parts target |
| `program_changed` | Machine switched to a different program |
| `stopped` | Program status went from RUNNING → STOPPED |

Each logged run includes:

```
cnc-01: O1001 FLANGE-A — 50/50 parts (100.0%) in 9000s (20.0 parts/hr) [completed]
cnc-02: O2001 AXLE-A — 28/40 parts (70.0%) in 12000s (8.4 parts/hr) [program_changed]
```

## Config in Valkey

Config is stored in the shared Valkey cache using `FUNCTION_TARGET` as the key:

```
FUNCTION_TARGET=uns-productivity  →  reads fnkit:config:uns-productivity
```

### Config format

```json
{
  "table": "uns_productivity",
  "topics": [
    "v1.0/enterprise/site1/area1/cnc-01/program",
    "v1.0/enterprise/site1/area1/cnc-02/program",
    "v1.0/enterprise/site1/area2/cnc-03/program",
    "v1.0/enterprise/site1/area2/cnc-04/program"
  ]
}
```

Set config with valkey-cli:

```bash
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-productivity '{"table":"uns_productivity","topics":["v1.0/enterprise/site1/area1/cnc-01/program","v1.0/enterprise/site1/area1/cnc-02/program","v1.0/enterprise/site1/area2/cnc-03/program","v1.0/enterprise/site1/area2/cnc-04/program"]}'
```

## PostgreSQL Table

Auto-created on first run:

```sql
CREATE TABLE IF NOT EXISTS uns_productivity (
    id                   BIGSERIAL    PRIMARY KEY,
    logged_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    enterprise           TEXT         NOT NULL,
    site                 TEXT         NOT NULL,
    area                 TEXT         NOT NULL,
    machine              TEXT         NOT NULL,
    program_id           TEXT         NOT NULL,
    program_name         TEXT         NOT NULL,
    parts_completed      INT          NOT NULL,
    parts_target         INT          NOT NULL,
    target_pct           NUMERIC      NOT NULL,
    cycle_time_planned_s INT          NOT NULL,
    started_at           TIMESTAMPTZ  NOT NULL,
    ended_at             TIMESTAMPTZ  NOT NULL,
    run_duration_s       NUMERIC      NOT NULL,
    parts_per_hour       NUMERIC      NOT NULL,
    reason               TEXT         NOT NULL DEFAULT ''
);
```

### Example rows

```
id | machine | program_id | program_name | parts_completed | parts_target | target_pct | run_duration_s | parts_per_hour | reason
1  | cnc-01  | O1001      | FLANGE-A     | 50              | 50           | 100.0      | 9000.0         | 20.0           | completed
2  | cnc-02  | O2001      | AXLE-A       | 28              | 40           | 70.0       | 12000.0        | 8.4            | program_changed
3  | cnc-04  | O4002      | SPACER-B     | 800             | 1000         | 80.0       | 36000.0        | 80.0           | stopped
```

## Quick Start

```bash
# Ensure fnkit-network, cache, postgres, and uns-framework are running
docker network create fnkit-network 2>/dev/null || true
fnkit cache start

# Set config in Valkey
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-productivity '{"table":"uns_productivity","topics":["v1.0/enterprise/site1/area1/cnc-01/program","v1.0/enterprise/site1/area1/cnc-02/program","v1.0/enterprise/site1/area2/cnc-03/program","v1.0/enterprise/site1/area2/cnc-04/program"]}'

# Build and start
docker compose up -d

# Check logs
docker logs -f uns-productivity

# Trigger a check
curl http://localhost:8080/uns-productivity
```

## Calling on an Interval

Like uns-state, this function is designed to be called repeatedly:

```bash
# Every 10s
while true; do curl -s http://localhost:8080/uns-productivity | jq; sleep 10; done
```

## API Response

### Run completed (logged)

```json
{
  "logged_runs": 1,
  "machines": [
    {
      "machine": "cnc-01",
      "area": "area1",
      "program_id": "O1001",
      "program_name": "FLANGE-A",
      "status": "RUNNING",
      "parts_completed": 12,
      "parts_target": 50,
      "progress_pct": 24.0,
      "run_seconds": 45
    }
  ],
  "completed": [
    {
      "machine": "cnc-01",
      "area": "area1",
      "program_id": "O1004",
      "program_name": "SHAFT-D",
      "parts_completed": 100,
      "parts_target": 100,
      "target_pct": 100.0,
      "cycle_time_planned_s": 120,
      "run_duration_s": 12000.0,
      "parts_per_hour": 30.0,
      "started_at": "2026-02-26T12:00:00Z",
      "ended_at": "2026-02-26T15:20:00Z",
      "reason": "completed"
    }
  ],
  "checked_at": "2026-02-26T15:20:00Z"
}
```

### No completions

```json
{
  "logged_runs": 0,
  "machines": [...],
  "completed": [],
  "checked_at": "2026-02-26T15:20:05Z"
}
```

## Useful Queries

### Throughput per machine (last 24h)

```sql
SELECT machine,
       COUNT(*) AS runs,
       SUM(parts_completed) AS total_parts,
       ROUND(AVG(parts_per_hour), 1) AS avg_parts_per_hour,
       ROUND(AVG(target_pct), 1) AS avg_target_pct
FROM uns_productivity
WHERE started_at >= NOW() - INTERVAL '24 hours'
GROUP BY machine
ORDER BY total_parts DESC;
```

### Target attainment by program

```sql
SELECT program_name,
       COUNT(*) AS runs,
       ROUND(AVG(target_pct), 1) AS avg_target_pct,
       SUM(parts_completed) AS total_parts,
       SUM(parts_target) AS total_target
FROM uns_productivity
WHERE started_at >= NOW() - INTERVAL '7 days'
GROUP BY program_name
ORDER BY avg_target_pct DESC;
```

### Incomplete runs (stopped early)

```sql
SELECT machine, program_name, parts_completed, parts_target,
       target_pct, reason, started_at
FROM uns_productivity
WHERE reason != 'completed'
  AND started_at >= NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;
```

### Average cycle time vs planned

```sql
SELECT machine, program_name,
       cycle_time_planned_s,
       ROUND(AVG(run_duration_s / NULLIF(parts_completed, 0)), 1) AS actual_cycle_s
FROM uns_productivity
WHERE parts_completed > 0
  AND started_at >= NOW() - INTERVAL '7 days'
GROUP BY machine, program_name, cycle_time_planned_s
ORDER BY machine;
```

## Configuration

| Variable           | Default                                                            | Description                            |
| ------------------ | ------------------------------------------------------------------ | -------------------------------------- |
| `FUNCTION_TARGET`  | `uns-productivity`                                                 | Function name = Valkey config key      |
| `DATABASE_URL`     | `postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable` | PostgreSQL connection string           |
| `CACHE_URL`        | `redis://fnkit-cache:6379`                                         | Valkey/Redis connection                |
| `CACHE_KEY_PREFIX` | `uns`                                                              | Cache key prefix (match uns-framework) |

## Built With

- [fnkit](https://github.com/functionkit/fnkit) — scaffolded with `fnkit go uns-productivity`
- [functions-framework-go](https://github.com/GoogleCloudPlatform/functions-framework-go) — HTTP function framework
- [go-redis](https://github.com/redis/go-redis) — Valkey/Redis client
- [pgx](https://github.com/jackc/pgx) — PostgreSQL driver
