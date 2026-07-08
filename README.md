# Forge AGI

Decentralized, distributed AI research platform with autonomous multi-agent collaboration, persistent learning memory, distributed task orchestration, and production-grade infrastructure.

## Quick Start

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="sk-..."
python forge_ai.py
```

Server starts at `http://localhost:8000`. API docs at `http://localhost:8000/docs`.

## Architecture

```
User → API (FastAPI) → Multi-Agent Pipeline → Memory (SQLite / PostgreSQL)
                       ├─ Thinker (plans approach)
                       ├─ Coder (writes code)
                       ├─ Critic (reviews quality)
                       └─ Learner (extracts patterns)
                              ↓
                    Vector Search (pgvector / numpy)
                    Redis / RabbitMQ (job distribution)
                    Prometheus + Grafana (monitoring)
```

Tasks can be distributed via RabbitMQ or direct DB polling, with Prometheus metrics and a built-in dashboard.

## API Overview

| Endpoint | Description |
|----------|-------------|
| `POST /research/task` | Create a research task |
| `POST /research/solve/{id}` | Solve task (streaming SSE) |
| `GET /research/history` | Past experiments |
| `GET /research/discoveries` | System discoveries |
| `POST /workers/register` | Register a compute worker |
| `POST /workers/heartbeat` | Worker keep-alive |
| `GET /workers/stats` | Cluster statistics |
| `GET /tasks/pending` | Pending tasks for workers |
| `POST /tasks/{id}/complete` | Submit task result |
| `GET /metrics` | Prometheus metrics |
| `GET /dashboard` | Web monitoring UI |
| `GET /api/stats` | JSON stats for dashboard |

## Example

```bash
curl -X POST http://localhost:8000/research/task \
  -H "Content-Type: application/json" \
  -d '{"task_name": "sort-list", "description": "Write a function to sort a list"}'

curl -X POST http://localhost:8000/research/solve/1
```

## Docker Compose (full stack)

```bash
docker compose up
```

Starts all services:
- **forge-agi** — app on port 8000
- **rabbitmq** — message broker on port 5672 (management UI on 15672)
- **prometheus** — metrics store on port 9090
- **grafana** — dashboards on port 3000 (admin/admin)

## Project Status

- **Phase 1 (Core Platform):** Complete — agents, memory, streaming API, SQLite
- **Phase 2 (Distributed Compute):** Complete — task queue, worker registration, heartbeat, orphan reassignment
- **Phase 3 (Knowledge Sharing):** Complete — patterns, trending, cross-domain, recommendations
- **Post-MVP Infra:** Complete — PostgreSQL, vector search, Prometheus/Grafana, RabbitMQ
- **Phase 4 (Incentives):** Removed

## Stack

- **Python** — `3.14`
- **FastAPI** — REST API + SSE streaming
- **Anthropic Claude** — agent reasoning
- **SQLite / PostgreSQL** — data storage
- **sentence-transformers** — semantic embeddings
- **pgvector / numpy** — vector similarity search
- **RabbitMQ** — reliable job distribution
- **Prometheus + Grafana** — monitoring & dashboards

## Tests

```bash
python -m pytest tests/ -v
```
