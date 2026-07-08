# Forge AGI – Technical Architecture & API Reference

---

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         USER INTERFACE LAYER                     │
│  (Web Dashboard, CLI, API Clients, Mobile Apps)                 │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│                         API GATEWAY (FastAPI)                    │
│  - Authentication & Rate Limiting                               │
│  - Request Validation                                           │
│  - Response Formatting                                          │
│  - Streaming Support                                            │
└────────┬──────────────────┬──────────────────┬──────────────────┘
         │                  │                  │
         ↓                  ↓                  ↓
    ┌─────────────┐    ┌─────────────┐   ┌──────────────┐
    │  Research   │    │   Worker    │   │   Utility    │
    │  Endpoints  │    │  Endpoints  │   │  Endpoints   │
    └──────┬──────┘    └──────┬──────┘   └──────┬───────┘
           │                  │                  │
           ↓                  ↓                  ↓
     ┌──────────────────────────────────────────────────┐
     │         ORCHESTRATION LAYER (Python)             │
     │  ┌────────────────────────────────────────────┐  │
     │  │  Multi-Agent Collaboration Engine          │  │
     │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐   │  │
     │  │  │ Thinker  │→│  Coder   │→│ Critic   │   │  │
     │  │  │  Agent   │ │  Agent   │ │  Agent   │   │  │
     │  │  └──────────┘ └──────────┘ └──────┬───┘   │  │
     │  │                                    ↓       │  │
     │  │                            ┌───────────────┐ │
     │  │                            │ Learner Agent │ │
     │  │                            └───────────────┘ │
     │  └────────────────────────────────────────────┘  │
     │  ┌────────────────────────────────────────────┐  │
     │  │  Task Queue Manager                        │  │
     │  │  - Task Assignment                         │  │
     │  │  - Worker Heartbeat                        │  │
     │  │  - Result Aggregation                      │  │
     │  └────────────────────────────────────────────┘  │
     └──────────────────────────────────────────────────┘
           │                    │                │
           ↓                    ↓                ↓
    ┌─────────────┐      ┌─────────────┐  ┌──────────────┐
    │   Memory    │      │  Task Queue │  │   Worker     │
    │   System    │      │   Backend   │  │  Registry    │
    │             │      │             │  │              │
    │ - Exper.    │      │ - Pending   │  │ - Worker IDs │
    │ - Patterns  │      │ - Assigned  │  │ - Status     │
    │ - Discovery │      │ - Complete  │  │ - Heartbeat  │
    └─────────────┘      └─────────────┘  └──────────────┘
           │                    │                │
           ↓                    ↓                ↓
    ┌──────────────────────────────────────────────────┐
    │            DATABASE LAYER (SQLite/PostgreSQL)    │
    │  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
    │  │ Exper.   │ │ Patterns │ │ Task Queue       │ │
    │  │ Table    │ │ Table    │ │ Tables           │ │
    │  └──────────┘ └──────────┘ └──────────────────┘ │
    │  ┌──────────┐ ┌──────────────────────────────┐  │
    │  │Discovery │ │ Worker Registry              │  │
    │  │ Table    │ │ Table                        │  │
    │  └──────────┘ └──────────────────────────────┘  │
    └──────────────────────────────────────────────────┘
           ↑                    ↑                ↑
           └────────────────────┴────────────────┘
                    │
                    ↓
         ┌────────────────────────┐
         │  DISTRIBUTED WORKERS   │
         │  (Multiple Machines)   │
         │                        │
         │  ┌───────────────────┐ │
         │  │ Worker Node 1     │ │
         │  │ - Poll tasks      │ │
         │  │ - Execute         │ │
         │  │ - Submit results  │ │
         │  └───────────────────┘ │
         │  ┌───────────────────┐ │
         │  │ Worker Node 2     │ │
         │  │ - Poll tasks      │ │
         │  │ - Execute         │ │
         │  │ - Submit results  │ │
         │  └───────────────────┘ │
         │  ┌───────────────────┐ │
         │  │ Worker Node N     │ │
         │  │ - Poll tasks      │ │
         │  │ - Execute         │ │
         │  │ - Submit results  │ │
         │  └───────────────────┘ │
         └────────────────────────┘
```

---

## Data Flow Diagram

### Research Task Flow

```
User submits: "Create image segmentation model"
        ↓
[API] POST /research/task
        ↓
System checks memory for similar solutions
        ├─ FOUND: Suggest reuse
        └─ NOT FOUND: Start full pipeline
        ↓
[Thinker] Analyzes problem → Generates plan
        ↓
[Coder] Implements solution based on plan
        ↓
[Critic] Reviews code → Provides score + feedback
        ↓
[Coder] (optional 2nd pass) Refines based on critique
        ↓
[Learner] Extracts patterns & insights
        ↓
Store in database:
  - experiments table (solution + metadata)
  - patterns table (reusable techniques)
  - discoveries table (insights)
        ↓
Return to user + stream results in real-time
```

### Worker Task Flow

```
Worker registers: POST /workers/register
        ↓
Worker polls: GET /tasks/pending
        ↓
Receive task list
        ↓
Claim task: GET /tasks/{id}
        ↓
Execute locally (may involve running agents)
        ↓
Submit result: POST /tasks/{id}/complete
        ↓
System validates and stores result
        ↓
Update experiment history
        ↓
Worker polls for next task
```

---

## Component Responsibilities

### API Gateway (FastAPI)
```
Responsibilities:
  - Route requests to correct handler
  - Validate input (Pydantic models)
  - Format JSON responses
  - Stream server-sent events
  - Handle CORS and security
  - Rate limiting (future)
  - Authentication (future)

Performance targets:
  - <100ms for simple endpoints
  - <2s for complex endpoints
  - <5s to first byte on streaming
```

### Orchestration Layer
```
Multi-Agent Collaboration:
  - Thinker: Plan generation (specialized prompts)
  - Coder: Implementation (code-focused prompts)
  - Critic: Review & quality assurance
  - Learner: Knowledge extraction
  
Responsibilities:
  - Sequential execution with context passing
  - Memory consultation before each step
  - Error handling and retries
  - Streaming output to users
  - Conversation history management

Performance targets:
  - Agent response: 5-30 seconds each
  - Full pipeline: 15-60 seconds
  - Memory lookup: <500ms
```

### Memory System
```
Tables:
  - experiments: Completed solutions (searchable)
  - patterns: Reusable code templates
  - discoveries: Learned insights

Responsibilities:
  - Store solutions after completion
  - Search for similar past solutions
  - Track success rates of patterns
  - Provide memory context to agents
  - Archive old data (retention policy)

Performance targets:
  - Insert: <100ms
  - Query similar: <500ms
  - Return context: <1s
```

### Task Queue Manager
```
Responsibilities:
  - Create task entries
  - Assign to available workers
  - Track task status (pending→assigned→completed)
  - Detect abandoned tasks
  - Reassign on worker failure
  - Aggregate results

Performance targets:
  - Assign task: <100ms
  - Detect failure: <30s
  - Reassign: <1s
```

### Worker Registry
```
Responsibilities:
  - Register new workers
  - Track online/offline status
  - Store worker capabilities
  - Monitor heartbeat
  - Update task counts
  - Prevent task loss on failure

Performance targets:
  - Registration: <100ms
  - Heartbeat check: <1s
  - Failover: <30s
```

---

## API Reference

### Research Endpoints

#### 1. Create Research Task

```http
POST /research/task
Content-Type: application/json

{
  "task_name": "string (unique identifier)",
  "description": "string (problem description)"
}

Response:
{
  "task_id": 1,
  "status": "created",
  "similar_solution_found": false,
  "hint": "optional - if similar solution found"
}
```

**Example:**
```bash
curl -X POST http://localhost:8000/research/task \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "image-segmentation-v1",
    "description": "Create a CNN model for semantic image segmentation using PyTorch"
  }'
```

---

#### 2. Solve Research Task

```http
POST /research/solve/{task_id}

Response: Server-Sent Events (streaming)

Event format:
data: {"stage": "thinker", "output": "..."}
data: {"stage": "coder", "output": "..."}
data: {"stage": "critic", "output": "..."}
data: {"stage": "learner", "output": "..."}
data: {"status": "completed", "task_id": 1}
```

**Example:**
```bash
curl -X POST http://localhost:8000/research/solve/1 \
  --header "Content-Type: application/json" \
  --no-buffer

# Output streams in real-time
# [THINKER] Analyzing problem...
# [CODER] Writing implementation...
# [CRITIC] Reviewing solution...
# [LEARNER] Extracting insights...
```

---

#### 3. Get Research History

```http
GET /research/history?limit=20

Response:
{
  "experiments": [
    {
      "task": "task_name",
      "description": "...",
      "success": true
    },
    ...
  ]
}
```

---

#### 4. Get Discoveries

```http
GET /research/discoveries?limit=10

Response:
{
  "discoveries": [
    {
      "discovery": "pattern or insight",
      "from": "agent_name"
    },
    ...
  ]
}
```

---

### Worker Endpoints

#### 1. Register Worker

```http
POST /workers/register
Content-Type: application/json

{
  "worker_id": "string (unique machine identifier)"
}

Response:
{
  "worker_id": "worker-gpu-1",
  "status": "registered",
  "pending_tasks": [
    {
      "id": 1,
      "name": "solve-problem",
      "description": "..."
    },
    ...
  ]
}
```

---

#### 2. Get Worker Statistics

```http
GET /workers/stats

Response:
{
  "online_workers": 5,
  "completed_tasks": 42,
  "active_workers": ["worker-1", "worker-2", ...]
}
```

---

#### 3. Get Pending Tasks

```http
GET /tasks/pending?limit=10

Response:
{
  "tasks": [
    {
      "id": 1,
      "name": "task-name",
      "description": "..."
    },
    ...
  ]
}
```

---

#### 4. Complete Task

```http
POST /tasks/{task_id}/complete
Content-Type: application/json

{
  "result": "string (task solution or output)"
}

Response:
{
  "status": "completed",
  "task_id": 1
}
```

---

### Utility Endpoints

#### API Overview

```http
GET /

Response:
{
  "system": "Forge AGI",
  "version": "0.1.0",
  "features": [
    "Multi-Agent Autonomous Collaboration",
    "Persistent Learning Memory",
    "Distributed Task Orchestration"
  ],
  "endpoints": { ... }
}
```

---

## Database Schema

### experiments

```sql
CREATE TABLE experiments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_name TEXT UNIQUE NOT NULL,
  description TEXT NOT NULL,
  solution TEXT NOT NULL,
  success INTEGER NOT NULL,        -- 0 or 1
  timestamp DATETIME NOT NULL,
  agent_type TEXT NOT NULL         -- e.g., "multi-agent-ensemble"
);

-- Indexes
CREATE INDEX idx_task_name ON experiments(task_name);
CREATE INDEX idx_description ON experiments(description);
CREATE INDEX idx_success ON experiments(success);
```

**Usage:**
- Store solution after agents complete
- Search by task name or keywords in description
- Calculate success rate metrics

---

### patterns

```sql
CREATE TABLE patterns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pattern_type TEXT NOT NULL,      -- e.g., "data-cleaning", "api-design"
  description TEXT NOT NULL,
  code_template TEXT NOT NULL,     -- Reusable code snippet
  success_rate REAL NOT NULL,      -- 0.0-1.0
  timestamp DATETIME NOT NULL
);

-- Indexes
CREATE INDEX idx_pattern_type ON patterns(pattern_type);
CREATE INDEX idx_success_rate ON patterns(success_rate DESC);
```

**Usage:**
- Store reusable patterns discovered by Learner
- Update success_rate as pattern is reused
- Recommend top patterns to Coder agent

---

### discoveries

```sql
CREATE TABLE discoveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  discovery TEXT NOT NULL,         -- Insight or lesson learned
  relevance_score REAL NOT NULL,   -- 0.0-1.0
  agent_who_found TEXT NOT NULL,   -- e.g., "learner", "critic"
  timestamp DATETIME NOT NULL
);

-- Indexes
CREATE INDEX idx_relevance ON discoveries(relevance_score DESC);
CREATE INDEX idx_timestamp ON discoveries(timestamp DESC);
```

**Usage:**
- Store insights from Learner after each task
- Show recent discoveries to users
- Provide context to Thinker agent for future tasks

---

### tasks (for distributed queue)

```sql
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_name TEXT NOT NULL,
  description TEXT NOT NULL,
  status TEXT NOT NULL,            -- pending, assigned, completed
  assigned_to TEXT,                -- worker_id (NULL if pending)
  result TEXT,                     -- Solution or output
  created_at DATETIME NOT NULL,
  completed_at DATETIME            -- NULL until completed
);

-- Indexes
CREATE INDEX idx_status ON tasks(status);
CREATE INDEX idx_assigned_to ON tasks(assigned_to);
```

**Usage:**
- Track task lifecycle
- Distribute work to workers
- Detect orphaned tasks (assigned but not completed after 5 min)

---

### workers

```sql
CREATE TABLE workers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT UNIQUE NOT NULL,
  status TEXT NOT NULL,            -- online, offline, error
  last_heartbeat DATETIME NOT NULL,
  tasks_completed INTEGER NOT NULL
);

-- Indexes
CREATE INDEX idx_worker_id ON workers(worker_id);
CREATE INDEX idx_status ON workers(status);
```

**Usage:**
- Register workers
- Track availability
- Detect dead workers (no heartbeat for 30s)

---

## Error Handling

### HTTP Status Codes

| Status | Meaning | When to Return |
|--------|---------|----------------|
| 200 | OK | Request succeeded |
| 201 | Created | Resource created successfully |
| 400 | Bad Request | Invalid input (missing fields, bad format) |
| 404 | Not Found | Task/worker doesn't exist |
| 500 | Server Error | Unexpected error in agent or database |
| 503 | Unavailable | Service overloaded or down |

### Example Error Response

```json
{
  "detail": "Task not found",
  "status": 404,
  "timestamp": "2026-06-29T10:30:00Z"
}
```

### Retry Logic (for clients)

```python
import time

def call_api_with_retry(url, method="GET", data=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, json=data, timeout=30)
            if response.status_code < 500:
                return response  # Success or client error (don't retry)
            # Server error (5xx) - retry with backoff
        except requests.Timeout:
            pass  # Retry on timeout
        
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # Exponential backoff
            time.sleep(wait_time)
    
    raise Exception(f"Failed after {max_retries} attempts")
```

---

## Performance Characteristics

### Latency Breakdown (per typical task)

```
User submits task
  ├─ Thinker analysis:      5-15 seconds
  ├─ Coder implementation:  10-20 seconds
  ├─ Critic review:         3-8 seconds
  ├─ Coder refinement:      5-15 seconds (if needed)
  ├─ Learner extraction:    2-5 seconds
  └─ Storage:               <1 second

Total: 15-60 seconds (most tasks complete in 30-40 seconds)

With memory reuse:
  ├─ Memory lookup:         <500ms
  ├─ Suggestion to user:    <2 seconds
  └─ Adaptation:            5-10 seconds (if customization needed)

Total: 2-15 seconds for reused solutions
```

### Throughput

```
Single machine:
  - 2-3 concurrent tasks per agent
  - ~10-20 tasks per hour

10 distributed workers:
  - 20-30 concurrent tasks
  - ~100-200 tasks per hour

100 distributed workers:
  - 200-300 concurrent tasks
  - ~1000-2000 tasks per hour
```

### Resource Usage (per agent call)

```
Memory: ~50-100 MB per active agent
CPU: ~20-40% of single core per agent
API calls to Claude: 1 per agent per task (~6 calls per complete task)
```

---

## Security Considerations

### Current (MVP)
- No authentication (assume private network)
- No encryption (assume local deployment)
- No rate limiting
- All tasks stored unencrypted

### Phase 2 (Production)
- [ ] API key authentication
- [ ] Rate limiting per API key
- [ ] HTTPS/TLS encryption
- [ ] Audit logging
- [ ] Input validation (SQL injection prevention)

### Phase 3 (Enterprise)
- [ ] OAuth2/OIDC integration
- [ ] Field-level encryption for sensitive tasks
- [ ] GDPR compliance (data deletion)
- [ ] Compliance audit trail

---

## Configuration

### Environment Variables

```bash
# Required
ANTHROPIC_API_KEY=sk-...

# Optional
DATABASE_URL=sqlite:///forge_memory.db  # Default
TASK_QUEUE_DB=sqlite:///tasks.db            # Default
API_HOST=0.0.0.0                             # Default
API_PORT=8000                                # Default
LOG_LEVEL=INFO                               # Default
```

### Config File (future)

```yaml
# forge.config.yaml
server:
  host: 0.0.0.0
  port: 8000
  workers: 4

database:
  type: sqlite  # or postgresql
  path: ./forge_memory.db

memory:
  retention_days: 90
  archive_older_than: 365

agents:
  timeout_seconds: 120
  max_retries: 3
  
workers:
  heartbeat_interval: 30
  task_timeout: 3600
```

---

## Monitoring & Observability

### Logs to emit

```
INFO  [API] POST /research/task - task_id=1
INFO  [THINKER] Starting analysis for task_id=1
INFO  [THINKER] Completed in 12s - tokens_used=450
INFO  [CODER] Starting implementation for task_id=1
INFO  [CODER] Generated 250 lines of code
INFO  [CRITIC] Reviewing solution - quality_score=8.5
INFO  [LEARNER] Extracted 3 patterns - relevance=0.82
INFO  [MEMORY] Stored task_1 as experiment
INFO  [API] POST /research/solve/1 - completed in 45s
```

### Metrics to track

```
# Counters
  - tasks_created
  - tasks_completed
  - tasks_failed
  - agent_errors
  - memory_queries

# Gauges
  - active_tasks
  - active_workers
  - queue_size
  - database_size

# Histograms
  - task_duration_seconds
  - agent_duration_seconds
  - memory_query_ms
  - api_response_ms
```

---

## Troubleshooting

### Common Issues

**"Task stuck in assigned state"**
- Symptom: Task shows `status=assigned` for 10+ minutes
- Cause: Worker crashed before completing task
- Solution: Check worker heartbeat; reassign task
- Code: `UPDATE tasks SET status='pending', assigned_to=NULL WHERE status='assigned' AND assigned_to='dead-worker'`

**"Memory database growing too large"**
- Symptom: SQLite file >1GB, queries slow
- Cause: No retention policy; all experiments stored forever
- Solution: Archive old experiments or move to PostgreSQL
- Code: `DELETE FROM experiments WHERE timestamp < DATE('now', '-90 days')`

**"Agents producing low-quality solutions"**
- Symptom: Critic scores 4/10 or lower consistently
- Cause: Prompts are too vague or agents aren't referencing memory
- Solution: Refine agent prompts; add examples
- Action: Review recent experiments and improve prompts

**"Worker keeps going offline"**
- Symptom: Heartbeat timeout, tasks reassigned frequently
- Cause: Network issues or worker crash
- Solution: Check worker logs; increase timeout threshold; check connectivity
- Code: `SELECT * FROM workers WHERE status='offline' ORDER BY last_heartbeat DESC`

---

## Next Steps

1. Deploy this backend
2. Build a web UI on top (React/Vue)
3. Create worker client (Python script)
4. Test with 5-10 workers
5. Gather feedback and iterate

---

**Last Updated:** June 2026  
**Maintainer:** Platform Team
