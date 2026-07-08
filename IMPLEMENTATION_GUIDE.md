# Forge AGI – Implementation Guide & Quick Start

**For:** Developers building the platform  
**Estimated Time to MVP:** 2-4 weeks (full-time development)

---

## Quick Start (15 minutes)

### Prerequisites
```bash
# Python 3.9+
python --version

# Install dependencies
pip install fastapi uvicorn anthropic pydantic

# Get Anthropic API key
# Export as environment variable
export ANTHROPIC_API_KEY="sk-..."
```

### Run the Backend
```bash
# Clone or download forge_ai.py
python forge_ai.py

# Server starts at http://localhost:8000
# API docs at http://localhost:8000/docs
```

### Test a Task (in another terminal)
```bash
curl -X POST http://localhost:8000/research/task \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "sort-list",
    "description": "Write a Python function to sort a list of integers in ascending order"
  }'

# Returns: { "task_id": 1, "status": "created", ... }
```

### Solve the Task
```bash
curl -X POST http://localhost:8000/research/solve/1 \
  -H "Content-Type: application/json" \
  --output results.txt

# Streams multi-agent collaboration output
cat results.txt
```

---

## Development Roadmap (3 Months)

### Week 1-2: Core Setup ✅ (Skeleton Ready)

**Done:**
- ✅ FastAPI backend scaffold
- ✅ Multi-agent classes (Thinker, Coder, Critic, Learner)
- ✅ Memory system (SQLite)
- ✅ Task queue foundation
- ✅ Basic API endpoints

**TODO:**
- [ ] Enhanced error handling
- [ ] Logging system (Winston or similar)
- [ ] Unit tests for each agent
- [ ] API documentation (OpenAPI/Swagger)

**Deliverable:** `python forge_ai.py` runs without errors

---

### Week 3-4: Agent Refinement

**Focus:** Make agents smarter and more reliable

**Tasks:**
- [ ] Improve Thinker prompt engineering
  - Make it reference discoveries more effectively
  - Test with 20+ diverse problems
  - Measure plan quality (manual review)

- [ ] Optimize Coder output
  - Add code validation (syntax check)
  - Ensure solutions are actually executable
  - Add inline comments for clarity
  - Implement code formatting (Black)

- [ ] Strengthen Critic feedback
  - Define quality rubric (1-10 scoring)
  - Make feedback actionable (not vague)
  - Test on known-bad code

- [ ] Enhance Learner insights
  - Extract more specific patterns
  - Calculate relevance scores accurately
  - Test if patterns actually work in future tasks

**Success Criteria:**
- Thinker plans reviewed as "good" 80%+ of the time
- Coder solutions pass syntax validation 100%
- Critic feedback catches real issues 90%+ of the time
- Learner patterns reused in 50%+ of similar tasks

**Deliverable:** Agents work well on 20+ test problems

---

### Week 5-8: Distributed Compute

**Focus:** Enable multiple machines to work together

**Phase 5-6: Worker Foundation**
- [ ] Implement worker registration (`POST /workers/register`)
- [ ] Build task assignment logic
- [ ] Create worker heartbeat monitoring
- [ ] Implement task status tracking

**Phase 7-8: Worker Integration**
- [ ] Worker can pull tasks (`GET /tasks/pending`)
- [ ] Worker can submit results (`POST /tasks/{id}/complete`)
- [ ] Handle worker failures gracefully (task reassignment)
- [ ] Track worker performance metrics

**Testing:**
- [ ] Test with 5 local workers (different processes)
- [ ] Simulate worker crashes and test recovery
- [ ] Verify no task loss
- [ ] Measure latency with concurrent workers

**Success Criteria:**
- 10 workers can process tasks in parallel
- 99%+ of tasks complete successfully
- Tasks reassigned within 30s of worker failure
- System handles 100 concurrent tasks

**Deliverable:** Working distributed system (5-10 nodes)

---

### Week 9-12: Knowledge System & Launch

**Phase 9: Knowledge Mining**
- [ ] Implement pattern extraction from solutions
- [ ] Build similarity search (keyword-based MVP)
- [ ] Create discovery scoring
- [ ] Test with 100+ stored experiments

**Phase 10: Performance Tuning**
- [ ] Optimize database queries
- [ ] Add indexes on frequently searched columns
- [ ] Benchmark memory retrieval (target: <500ms)
- [ ] Load test with 1000 experiments

**Phase 11: UI & Dashboard**
- [ ] Build web dashboard (React/Vue)
  - [ ] Task submission form
  - [ ] Live progress view
  - [ ] Experiment history
  - [ ] Discovery feed
  - [ ] Worker status page
  - [ ] System statistics

**Phase 12: Beta Launch**
- [ ] Deploy to staging environment
- [ ] Invite 50-100 beta testers
- [ ] Gather feedback
- [ ] Fix critical bugs
- [ ] Prepare public launch

**Success Criteria:**
- 60%+ of new tasks find similar solutions
- System stays up 99%+ of the time
- Dashboard is intuitive (no training needed)
- Users report 5+ minutes saved per task

**Deliverable:** Public beta launch

---

## Code Structure

```
forge_ai/
├── forge_ai.py          # Main application (can split later)
├── requirements.txt
├── tests/
│   ├── test_agents.py
│   ├── test_memory.py
│   ├── test_task_queue.py
│   └── test_api.py
├── docs/
│   ├── API.md
│   ├── ARCHITECTURE.md
│   └── DEPLOYMENT.md
└── scripts/
    ├── migrate_to_postgres.py
    ├── load_test.py
    ├── seed_data.py
    └── cleanup.py
```

---

## Database Migrations

### SQLite → PostgreSQL (Week 8-9)

**When:** When you have 10k+ experiments or 100+ concurrent tasks

**Script:**
```bash
# 1. Dump SQLite
sqlite3 forge_memory.db .dump > dump.sql

# 2. Create PostgreSQL database
createdb forge_agi

# 3. Load schema
psql forge_agi < dump.sql

# 4. Update connection string
# In forge_ai.py, change:
# self.conn = sqlite3.connect(self.db_path)
# To:
# self.conn = psycopg2.connect("postgresql://user:pass@localhost/forge_agi")

# 5. Run migration script
python scripts/migrate_to_postgres.py
```

---

## Testing Strategy

### Unit Tests (Week 1-4)

```python
# tests/test_agents.py
def test_thinker_generates_plan():
    thinker = ThinkerAgent(memory)
    plan = thinker.think("Create a web scraper")
    assert "approach" in plan.lower()
    assert len(plan) > 100

def test_coder_writes_code():
    coder = CoderAgent(memory)
    code = coder.think("Write a function to reverse a string")
    assert "def " in code
    assert "string" in code.lower()

def test_critic_reviews_code():
    critic = CriticAgent(memory)
    review = critic.think("Review: def add(a,b): return a+b")
    assert "review" in review.lower() or "issue" in review.lower()

def test_learner_extracts_insights():
    learner = LearnerAgent(memory)
    insights = learner.think("What did we learn from this solution?")
    assert len(insights) > 50
```

### Integration Tests (Week 5-8)

```python
# tests/test_api.py
def test_create_and_solve_task():
    # POST /research/task
    response = client.post("/research/task", json={
        "task_name": "test-1",
        "description": "Write hello world"
    })
    assert response.status_code == 200
    task_id = response.json()["task_id"]
    
    # POST /research/solve/{task_id}
    response = client.post(f"/research/solve/{task_id}")
    assert response.status_code == 200
    # Check streaming output contains all agents

def test_worker_registration_and_task_pull():
    # Register worker
    response = client.post("/workers/register", json={"worker_id": "test-worker"})
    assert response.status_code == 200
    
    # Pull task
    response = client.get("/tasks/pending?limit=1")
    assert len(response.json()["tasks"]) > 0
    
    # Complete task
    response = client.post("/tasks/1/complete", json={"result": "solution"})
    assert response.status_code == 200
```

### Load Tests (Week 9-10)

```bash
# tests/load_test.py
# Simulate 100 concurrent tasks
# Measure: latency, throughput, error rate
# Target: <2s response, 10+ tasks/min, <0.1% errors

python tests/load_test.py --workers 10 --tasks 100 --duration 60
```

---

## Deployment Checklist

### Development (Week 1-4)
- [ ] Code runs locally without errors
- [ ] All unit tests pass
- [ ] Environment variables documented
- [ ] API docs generated

### Staging (Week 9-11)
- [ ] Deploy to cloud (AWS/GCP/Azure)
- [ ] Database on separate server
- [ ] SSL/TLS certificates
- [ ] Monitoring and alerting enabled
- [ ] Log aggregation (CloudWatch/ELK)
- [ ] Backup strategy implemented
- [ ] Run load tests

### Production (Week 12-13)
- [ ] Blue-green deployment setup
- [ ] Canary release process
- [ ] Incident response playbook
- [ ] Support team trained
- [ ] Documentation complete
- [ ] Security audit passed
- [ ] Launch! 🚀

---

## Key Metrics to Track

### From Day 1

```python
# Add to every endpoint
import time
from datetime import datetime

@app.middleware("http")
async def log_request(request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    
    print(f"""
    {datetime.now()} | {request.method} {request.url.path}
    Status: {response.status_code} | Duration: {duration:.3f}s
    """)
    return response
```

### Dashboard Queries

```sql
-- Tasks completed per day
SELECT DATE(completed_at), COUNT(*) 
FROM tasks 
WHERE status = 'completed' 
GROUP BY DATE(completed_at);

-- Average solution time
SELECT AVG(EXTRACT(EPOCH FROM (completed_at - created_at))/60) as avg_minutes
FROM tasks 
WHERE status = 'completed';

-- Reuse rate
SELECT COUNT(*) as found_similar / COUNT(*) * 100 as reuse_rate
FROM tasks 
WHERE status = 'completed' AND ...;

-- Most useful patterns
SELECT pattern_type, success_rate, COUNT(*) as reuse_count
FROM patterns
GROUP BY pattern_type
ORDER BY reuse_count DESC;
```

---

## Common Pitfalls & Solutions

| Problem | Cause | Solution |
|---------|-------|----------|
| Agents taking too long | Prompts are verbose | Optimize prompts, add examples |
| Memory growing too fast | No retention policy | Archive experiments older than 90 days |
| Workers going offline | No heartbeat | Implement 30s heartbeat, auto-reassign tasks |
| Solutions are low quality | Critic not rigorous enough | Refine Critic prompts with rubric examples |
| High reuse rate not achieved | Keyword search too simple | Add vector embeddings, improve similarity matching |
| Database performance degrading | No indexes | Add indexes on `task_name`, `description`, `status` |

---

## Future Enhancements (Post-Launch)

### Architecture Improvements
- [ ] Replace SQLite with PostgreSQL
- [ ] Add Redis for caching
- [ ] Implement message queue (RabbitMQ/NATS)
- [ ] Switch to microservices (separate API, task queue, memory service)

### Feature Additions
- [ ] Vector embeddings for semantic search
- [ ] Multi-task workflows (chained jobs)
- [ ] Federated learning
- [ ] Private deployments
- [ ] Custom agent fine-tuning
- [ ] Blockchain verification (optional)

### Scaling Improvements
- [ ] Database replication and sharding
- [ ] CDN for static content
- [ ] Load balancer for API
- [ ] Distributed task scheduler (Celery/Airflow)
- [ ] Kubernetes deployment configs

### Monetization
- [ ] Freemium model (limited tasks/month)
- [ ] Enterprise pricing
- [ ] API marketplace (premium agents)
- [ ] Rewards in native token (optional)

---

## Getting Help

### Debugging

**Agent not responding:**
```bash
# Check API key
echo $ANTHROPIC_API_KEY

# Test Claude API directly
curl -X POST https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":100,"messages":[{"role":"user","content":"Hi"}]}'
```

**Database issues:**
```bash
# Check SQLite integrity
sqlite3 forge_memory.db "PRAGMA integrity_check;"

# Export and reimport
sqlite3 forge_memory.db .dump > backup.sql
rm forge_memory.db
sqlite3 forge_memory.db < backup.sql
```

**Task queue stuck:**
```bash
# Check pending tasks
sqlite3 tasks.db "SELECT COUNT(*) FROM tasks WHERE status='pending';"

# Reset if needed (careful!)
sqlite3 tasks.db "UPDATE tasks SET status='pending' WHERE status='assigned' AND assigned_to='dead-worker';"
```

---

## Resources

- **FastAPI Docs:** https://fastapi.tiangolo.com
- **Anthropic API:** https://docs.anthropic.com
- **SQLite Docs:** https://www.sqlite.org/docs.html
- **Python Async:** https://docs.python.org/3/library/asyncio.html

---

## Success Checklist

### Week 4 Checkpoint
- [ ] All agents run and produce output
- [ ] Memory system stores and retrieves experiments
- [ ] API is documented and testable
- [ ] Unit tests at 80%+ coverage

### Week 8 Checkpoint
- [ ] 5+ workers running in parallel
- [ ] Task queue correctly assigns and tracks work
- [ ] No task loss in failure scenarios
- [ ] Integration tests passing

### Week 12 Checkpoint
- [ ] 60%+ similar solution reuse rate
- [ ] System uptime 99%+
- [ ] Dashboard is functional
- [ ] Documentation complete
- [ ] Ready for beta launch

---

Good luck! 🚀

**Questions?** Start with the PRD and API docs. Then check existing code comments. Then ask Claude again (you can share this guide with it for context).
