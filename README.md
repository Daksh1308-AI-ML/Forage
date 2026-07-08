# Forge AGI

Decentralized, distributed AI research platform with autonomous multi-agent collaboration, persistent learning memory, and distributed task orchestration.

## Quick Start

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="sk-..."
python forge_ai.py
```

Server starts at `http://localhost:8000`. API docs at `http://localhost:8000/docs`.

## Architecture

```
User â†’ API (FastAPI) â†’ Multi-Agent Pipeline â†’ Memory (SQLite)
                       â”œâ”€ Thinker (plans approach)
                       â”œâ”€ Coder (writes code)
                       â”œâ”€ Critic (reviews quality)
                       â””â”€ Learner (extracts patterns)
```

Tasks can be distributed across worker nodes via the task queue.

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

## Example

```bash
curl -X POST http://localhost:8000/research/task \
  -H "Content-Type: application/json" \
  -d '{"task_name": "sort-list", "description": "Write a function to sort a list"}'

curl -X POST http://localhost:8000/research/solve/1
```

## Project Status

- **Phase 1 (Core Platform):** Complete â€” agents, memory, streaming API, SQLite
- **Phase 2 (Distributed Compute):** Complete â€” task queue, worker registration, heartbeat, orphan reassignment
- **Phase 3 (Knowledge Sharing):** Not started
- **Phase 4 (Incentives):** Not started

## Stack

- **Python** â€” `3.14`
- **FastAPI** â€” REST API + SSE streaming
- **Anthropic Claude** â€” agent reasoning
- **SQLite** â€” experiments, patterns, discoveries, task queue

## Tests

```bash
python -m pytest tests/ -v
```

<!-- pin -->
