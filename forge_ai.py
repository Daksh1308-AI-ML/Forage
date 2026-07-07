"""
Forge AGI - Distributed AI Research Platform
A working prototype with 3 advanced features:
1. Multi-Agent Autonomous Collaboration (Thinker, Coder, Critic, Learner)
2. Persistent Knowledge Memory with Vector Search
3. Distributed Task Queue with Worker Nodes
"""

import os
import json
import sqlite3
import logging
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import uvicorn
from pydantic import BaseModel
from contextlib import asynccontextmanager

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("forge_agi")

try:
    from anthropic import Anthropic
except ImportError:
    class Anthropic:
        """Fallback Anthropic client stub when the package is unavailable."""

        class messages:
            @staticmethod
            def create(*args, **kwargs):
                class Response:
                    def __init__(self):
                        content_item = type("ContentItem", (), {"text": "Anthropic client unavailable. Install the 'anthropic' package to enable AI features."})
                        self.content = [content_item]

                return Response()

# ============================================================================
# FEATURE 1: PERSISTENT MEMORY SYSTEM (Vector-like embedding storage)
# ============================================================================

class MemoryDB:
    """Simple but powerful memory system - stores solutions and reuses them"""

    def __init__(self, db_path="forge_memory.db"):
        self.db_path = db_path
        self.conn = None
        self.init_db()

    def init_db(self):
        """Create memory tables"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        c = self.conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT UNIQUE,
            description TEXT,
            solution TEXT,
            success INTEGER,
            timestamp DATETIME,
            agent_type TEXT
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_type TEXT,
            description TEXT,
            code_template TEXT,
            success_rate REAL,
            timestamp DATETIME
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS discoveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discovery TEXT,
            relevance_score REAL,
            agent_who_found TEXT,
            timestamp DATETIME
        )''')

        self.conn.commit()
        self._migrate_schema()
        log.info("MemoryDB initialized at %s", self.db_path)

    def _migrate_schema(self):
        """Add new columns to existing tables (safe for existing databases)."""
        migrations = [
            "ALTER TABLE experiments ADD COLUMN reused_from TEXT DEFAULT NULL",
            "ALTER TABLE patterns ADD COLUMN domain TEXT DEFAULT 'general'",
            "ALTER TABLE patterns ADD COLUMN reuse_count INTEGER DEFAULT 0",
            "ALTER TABLE patterns ADD COLUMN last_used DATETIME DEFAULT NULL",
            "ALTER TABLE discoveries ADD COLUMN trending_score REAL DEFAULT 0.0",
            "ALTER TABLE discoveries ADD COLUMN reuse_count INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                self.conn.execute(sql)
            except Exception:
                pass

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            log.info("MemoryDB connection closed")

    def store_experiment(self, task_name: str, description: str, solution: str, success: bool, agent_type: str, reused_from: Optional[str] = None):
        """Store an experiment result"""
        c = self.conn.cursor()
        try:
            c.execute('''INSERT OR REPLACE INTO experiments
                        (task_name, description, solution, success, timestamp, agent_type, reused_from)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                     (task_name, description, solution, int(success), datetime.now().isoformat(), agent_type, reused_from))
            self.conn.commit()
            log.info("Stored experiment '%s' (success=%s, reused_from=%s)", task_name, success, reused_from)
        except Exception as e:
            log.error("Error storing experiment: %s", e)

    def find_similar_solution(self, task_description: str) -> Optional[dict]:
        """Find a previous solution that might be relevant (TF-IDF-like keyword matching)."""
        c = self.conn.cursor()
        keywords = [w for w in task_description.lower().split() if len(w) > 2]
        if not keywords:
            return None

        best_match = None
        best_score = 0.0

        c.execute('''SELECT * FROM experiments WHERE success = 1''')
        all_rows = c.fetchall()
        total_docs = len(all_rows)

        for row in all_rows:
            desc_words = set(row[2].lower().split())
            hits = 0
            for kw in keywords:
                if kw in desc_words:
                    doc_freq = sum(1 for r in all_rows if kw in r[2].lower().split())
                    idf = 1.0
                    if doc_freq > 0 and total_docs > 0:
                        idf = 1.0 + (total_docs / (1 + doc_freq))
                    hits += idf
            score = hits / max(len(keywords), 1)
            if score > best_score:
                best_score = score
                best_match = {
                    "task_name": row[1],
                    "description": row[2],
                    "solution": row[3],
                    "agent_type": row[6]
                }

        if best_match and best_score >= 0.5:
            log.info("Found similar solution '%s' with score %.2f", best_match["task_name"], best_score)
            return best_match
        log.info("No similar solution found for description (best=%.2f)", best_score)
        return None

    # -----------------------------------------------------------------------
    # Phase 3: Knowledge Sharing methods
    # -----------------------------------------------------------------------

    def store_pattern(self, pattern_type: str, description: str, code_template: str, success_rate: float, domain: str = "general"):
        """Store a reusable pattern learned from previous work."""
        c = self.conn.cursor()
        try:
            c.execute('''INSERT INTO patterns (pattern_type, description, code_template, success_rate, timestamp, domain)
                        VALUES (?, ?, ?, ?, ?, ?)''',
                     (pattern_type, description, code_template, success_rate, datetime.now().isoformat(), domain))
            self.conn.commit()
            log.info("Stored pattern '%s' in domain '%s' (success_rate=%.2f)", pattern_type, domain, success_rate)
        except Exception as e:
            log.error("Error storing pattern: %s", e)

    def find_relevant_patterns(self, keywords: list, limit: int = 5) -> list:
        """Find patterns matching the given keywords."""
        c = self.conn.cursor()
        results = []
        for keyword in keywords:
            c.execute('''SELECT id, pattern_type, description, code_template, success_rate, domain, reuse_count FROM patterns
                        WHERE description LIKE ? OR pattern_type LIKE ?
                        ORDER BY success_rate DESC LIMIT ?''',
                     (f"%{keyword}%", f"%{keyword}%", limit))
            for row in c.fetchall():
                entry = {
                    "id": row[0],
                    "pattern_type": row[1],
                    "description": row[2],
                    "code_template": row[3],
                    "success_rate": row[4],
                    "domain": row[5] if len(row) > 5 else "general",
                    "reuse_count": row[6] if len(row) > 6 else 0
                }
                if entry not in results:
                    results.append(entry)
        log.info("Found %d relevant patterns for keywords", len(results))
        return results[:limit]

    def update_pattern_usage(self, pattern_id: int, success: bool):
        """Increment reuse count and update success rate for a pattern."""
        c = self.conn.cursor()
        try:
            c.execute('''UPDATE patterns SET reuse_count = reuse_count + 1,
                        last_used = ? WHERE id = ?''',
                     (datetime.now().isoformat(), pattern_id))
            c.execute('''SELECT success_rate, reuse_count FROM patterns WHERE id = ?''', (pattern_id,))
            row = c.fetchone()
            if row:
                old_rate, count = row
                adjustment = 0.05 if success else -0.05
                new_rate = max(0.0, min(1.0, old_rate + adjustment))
                c.execute('''UPDATE patterns SET success_rate = ? WHERE id = ?''', (new_rate, pattern_id))
            self.conn.commit()
        except Exception as e:
            log.error("Error updating pattern usage: %s", e)

    def get_recommendations(self, task_description: str, limit: int = 3) -> dict:
        """Get both similar solutions and relevant patterns for a task description."""
        similar = self.find_similar_solution(task_description)
        keywords = [w for w in task_description.lower().split() if len(w) > 2]
        patterns = self.find_relevant_patterns(keywords, limit)
        return {
            "similar_solution": similar,
            "relevant_patterns": patterns
        }

    def get_trending_discoveries(self, limit: int = 10, days: int = 30) -> list:
        """Get discoveries sorted by trending score (relevance * recency decay)."""
        self._update_trending_scores(days)
        c = self.conn.cursor()
        c.execute('''SELECT discovery, relevance_score, agent_who_found, trending_score, timestamp
                     FROM discoveries ORDER BY trending_score DESC LIMIT ?''', (limit,))
        return [
            {
                "discovery": row[0],
                "relevance_score": row[1],
                "from": row[2],
                "trending_score": round(row[3], 3),
                "timestamp": row[4]
            }
            for row in c.fetchall()
        ]

    def _update_trending_scores(self, decay_days: int = 30):
        """Recalculate trending scores for all discoveries."""
        c = self.conn.cursor()
        now = datetime.now()
        c.execute('''SELECT id, relevance_score, timestamp, reuse_count FROM discoveries''')
        for row in c.fetchall():
            disc_id, relevance, ts_str, reuse_count = row
            try:
                ts = datetime.fromisoformat(ts_str) if ts_str else now
            except Exception:
                ts = now
            days_old = max(0, (now - ts).total_seconds() / 86400.0)
            recency_factor = max(0.0, 1.0 - days_old / decay_days)
            reuse_boost = min(1.0, (reuse_count or 0) * 0.1)
            trending = relevance * recency_factor + reuse_boost
            c.execute('''UPDATE discoveries SET trending_score = ? WHERE id = ?''',
                     (round(trending, 4), disc_id))
        self.conn.commit()

    def get_cross_domain_patterns(self, min_domains: int = 2, limit: int = 10) -> list:
        """Find patterns that appear across multiple domains (cross-domain recognition)."""
        c = self.conn.cursor()
        c.execute('''SELECT pattern_type, COUNT(DISTINCT domain) as domain_count,
                            GROUP_CONCAT(DISTINCT domain) as domains,
                            AVG(success_rate) as avg_success,
                            SUM(reuse_count) as total_reuse
                     FROM patterns
                     GROUP BY pattern_type
                     HAVING domain_count >= ?
                     ORDER BY domain_count DESC, avg_success DESC
                     LIMIT ?''', (min_domains, limit))
        return [
            {
                "pattern_type": row[0],
                "domain_count": row[1],
                "domains": row[2].split(",") if row[2] else [],
                "avg_success_rate": round(row[3], 3) if row[3] else 0.0,
                "total_reuse": row[4] or 0
            }
            for row in c.fetchall()
        ]

    def search_patterns(self, pattern_type: Optional[str] = None, domain: Optional[str] = None,
                        min_success_rate: float = 0.0, limit: int = 20, offset: int = 0) -> dict:
        """Search patterns with filters and pagination."""
        c = self.conn.cursor()
        conditions = []
        params = []
        if pattern_type:
            conditions.append("pattern_type LIKE ?")
            params.append(f"%{pattern_type}%")
        if domain:
            conditions.append("domain LIKE ?")
            params.append(f"%{domain}%")
        if min_success_rate > 0:
            conditions.append("success_rate >= ?")
            params.append(min_success_rate)
        where = " AND ".join(conditions) if conditions else "1=1"

        c.execute(f'''SELECT COUNT(*) FROM patterns WHERE {where}''', params)
        total = c.fetchone()[0]

        c.execute(f'''SELECT id, pattern_type, description, code_template, success_rate, domain, reuse_count, last_used FROM patterns WHERE {where}
                     ORDER BY success_rate DESC, reuse_count DESC
                     LIMIT ? OFFSET ?''', params + [limit, offset])
        results = [
            {
                "id": row[0],
                "pattern_type": row[1],
                "description": row[2],
                "code_template": row[3],
                "success_rate": row[4],
                "domain": row[5] if len(row) > 5 else "general",
                "reuse_count": row[6] if len(row) > 6 else 0,
                "last_used": row[7] if len(row) > 7 else None
            }
            for row in c.fetchall()
        ]
        return {"total": total, "results": results, "offset": offset, "limit": limit}

    def get_reuse_rate(self) -> dict:
        """Calculate what percentage of experiments reused a previous solution."""
        c = self.conn.cursor()
        c.execute('''SELECT COUNT(*) FROM experiments''')
        total = c.fetchone()[0]
        c.execute('''SELECT COUNT(*) FROM experiments WHERE reused_from IS NOT NULL''')
        reused = c.fetchone()[0]
        rate = round((reused / total * 100), 1) if total > 0 else 0.0
        return {"total_experiments": total, "reused_count": reused, "reuse_rate": rate}

    def extract_domain(self, description: str) -> str:
        """Basic domain extraction from a task description."""
        domain_keywords = {
            "data": ["data", "dataset", "csv", "json", "database", "sql", "analytics"],
            "ml": ["machine learning", "neural", "train", "model", "classif", "regression", "deep learning"],
            "web": ["api", "web", "http", "rest", "server", "endpoint", "route"],
            "algorithm": ["algorithm", "sort", "search", "graph", "tree", "optimize"],
            "security": ["security", "encrypt", "auth", "password", "hash", "token"],
            "automation": ["automation", "script", "pipeline", "workflow", "cron", "scheduler"],
            "nlp": ["nlp", "text", "natural language", "sentiment", "tokenize", "parse"],
            "devops": ["deploy", "docker", "ci/cd", "infrastructure", "terraform", "kubernetes"],
        }
        desc_lower = description.lower()
        scores = {}
        for domain, kws in domain_keywords.items():
            score = sum(1 for kw in kws if kw in desc_lower)
            if score > 0:
                scores[domain] = score
        if not scores:
            return "general"
        return max(scores, key=scores.get)

    def store_discovery(self, discovery: str, relevance: float, agent: str):
        """Store a new discovery with trending score."""
        c = self.conn.cursor()
        initial_trending = relevance
        c.execute('''INSERT INTO discoveries (discovery, relevance_score, agent_who_found, timestamp, trending_score)
                    VALUES (?, ?, ?, ?, ?)''',
                 (discovery, relevance, agent, datetime.now().isoformat(), initial_trending))
        self.conn.commit()
        log.info("Stored discovery from %s (relevance=%.2f, trending=%.2f)", agent, relevance, initial_trending)

    def get_recent_discoveries(self, limit: int = 5) -> list:
        """Get recent discoveries to learn from"""
        c = self.conn.cursor()
        c.execute('''SELECT discovery, agent_who_found FROM discoveries
                    ORDER BY timestamp DESC LIMIT ?''', (limit,))
        return [{"discovery": row[0], "from": row[1]} for row in c.fetchall()]

# ============================================================================
# FEATURE 2: MULTI-AGENT AUTONOMOUS COLLABORATION
# ============================================================================

class AIAgent:
    """Base class for autonomous AI agents"""

    def __init__(self, name: str, role: str, memory: MemoryDB):
        self.name = name
        self.role = role
        self.memory = memory
        self.client = Anthropic()
        self.conversation_history = []

    def think(self, task: str) -> str:
        """Agent thinks about a task"""
        log.info("Agent %s starting task", self.name)
        self.conversation_history.append({
            "role": "user",
            "content": task
        })

        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1000,
                system=self._get_system_prompt(),
                messages=self.conversation_history
            )
            result = response.content[0].text
            self.conversation_history.append({
                "role": "assistant",
                "content": result
            })
            log.info("Agent %s completed task (output length=%d)", self.name, len(result))
            return result
        except Exception as e:
            log.error("Agent %s failed with error: %s", self.name, e)
            return f"[{self.name} ERROR] Unable to process request: {e}"

    def _get_system_prompt(self) -> str:
        """Override in subclasses"""
        return f"You are {self.name}, a {self.role} AI agent."

class ThinkerAgent(AIAgent):
    """Plans research and generates ideas"""

    def __init__(self, memory: MemoryDB):
        super().__init__("Thinker", "planning and research", memory)

    def _get_system_prompt(self) -> str:
        recent = self.memory.get_recent_discoveries(3)
        recent_str = "\n".join([f"- {d['discovery']}" for d in recent]) if recent else "None yet"

        return f"""You are the Thinker Agent, a world-class research strategist. Your job is to:

1. **Analyze the problem deeply** — Understand the core challenge, constraints, and desired outcome before proposing anything.
2. **Break the problem into sub-tasks** — Decompose the work into clear, ordered, actionable steps.
3. **Reference recent discoveries** — Consider these past findings and explain how they apply or conflict:
   {recent_str}
4. **Propose hypotheses** — Suggest 1-2 specific approaches or hypotheses to test, explaining why each might work.
5. **Identify risks** — Flag potential pitfalls or assumptions that could derail the approach.

Output format:
- Summary of the problem (1-2 sentences)
- Sub-tasks (numbered list)
- Recommended approach with rationale
- Open questions or risks

Be concise but thorough. Think step by step."""

class CoderAgent(AIAgent):
    """Writes and improves code"""

    def __init__(self, memory: MemoryDB):
        super().__init__("Coder", "implementation and coding", memory)

    def _get_system_prompt(self) -> str:
        patterns = self.memory.find_relevant_patterns(["code", "implementation", "python"])
        patterns_str = ""
        if patterns:
            patterns_str = "\n".join([f"- {p['description']}" for p in patterns[:3]])

        return f"""You are the Coder Agent, an expert software engineer. Your job is to:

1. **Write production-ready code** — Clean, idiomatic, well-structured code with proper error handling.
2. **Handle edge cases** — Consider empty inputs, boundary values, type mismatches, and failure states.
3. **Use patterns from memory** — Reuse and adapt these proven patterns where applicable:
   {patterns_str if patterns_str else "  (No stored patterns yet — focus on writing clean, reusable code.)"}
4. **Follow the approach suggested by the Thinker** — Stay aligned with the research plan.
5. **Add inline comments** only for complex logic where the intent is not obvious.
6. **Use standard library when possible** — Prefer built-in solutions over external dependencies.

Write Python code when possible. Be pragmatic. Output only the code and a brief usage example."""

class CriticAgent(AIAgent):
    """Reviews and improves solutions"""

    def __init__(self, memory: MemoryDB):
        super().__init__("Critic", "evaluation and improvement", memory)

    def _get_system_prompt(self) -> str:
        return """You are the Critic Agent, a senior code reviewer and quality assurance expert. Your job is to:

Review the solution against this **quality rubric** (rate each category 1-10):

| Category       | What to check                                                        |
|----------------|----------------------------------------------------------------------|
| **Correctness**  | Does the solution solve the stated problem? Are there logic errors?  |
| **Performance**  | Is the algorithm efficient? Could it be optimized? Any O(n²) issues? |
| **Readability**  | Is the code clear, well-structured, and easy to follow?              |
| **Security**     | Are there injection risks, data leaks, or unsafe patterns?           |
| **Edge Cases**   | Are empty inputs, errors, and boundary conditions handled?           |

Output format:
- **Overall score: X/10**
- Per-category scores with brief justification
- Top 3 actionable improvements (most important first)
- One thing that was done well

Be honest and constructive. Focus on practical improvements that can be implemented immediately."""

class LearnerAgent(AIAgent):
    """Extracts insights and stores learnings"""

    def __init__(self, memory: MemoryDB):
        super().__init__("Learner", "knowledge extraction and memory", memory)

    def _get_system_prompt(self) -> str:
        return """You are the Learner Agent, a knowledge management specialist. Your job is to:

1. **Extract specific reusable patterns** — What code structure, algorithm, or design pattern can we reuse later?
2. **Calculate relevance scores** — For each insight, assign a relevance score (0.0 - 1.0) based on how generalizable it is.
3. **Identify generalizable techniques** — What did we learn that applies beyond this specific problem?
4. **Note what worked and what didn't** — Be honest about failures or suboptimal choices.
5. **Suggest future experiments** — What should we try next based on these results?

Output format (JSON-like structure):
- **pattern**: Brief description of the reusable pattern
- **relevance**: Score 0.0-1.0
- **technique**: The generalizable technique learned
- **what_worked**: Key success factors
- **what_didnt**: Things to avoid
- **next_steps**: Suggested follow-up experiments

Be precise and actionable. Every insight should be something another agent can actually use."""

# ============================================================================
# FEATURE 3: DISTRIBUTED TASK ORCHESTRATION
# ============================================================================

class TaskQueue:
    """Manages distributed task execution"""

    def __init__(self, db_path="tasks.db"):
        self.db_path = db_path
        self.conn = None
        self.init_db()
        self.active_workers = {}

    def init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        c = self.conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT,
            description TEXT,
            status TEXT,
            assigned_to TEXT,
            result TEXT,
            created_at DATETIME,
            completed_at DATETIME,
            user_id TEXT DEFAULT 'anonymous'
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS workers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id TEXT UNIQUE,
            status TEXT,
            last_heartbeat DATETIME,
            tasks_completed INTEGER
        )''')

        self.conn.commit()
        self._migrate_schema()
        log.info("TaskQueue initialized at %s", self.db_path)

    def _migrate_schema(self):
        """Add new columns to existing tables (safe for existing databases)."""
        migrations = [
            "ALTER TABLE tasks ADD COLUMN user_id TEXT DEFAULT 'anonymous'",
        ]
        for sql in migrations:
            try:
                self.conn.execute(sql)
            except Exception:
                pass

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            log.info("TaskQueue connection closed")

    def register_worker(self, worker_id: str):
        """Register a distributed worker"""
        c = self.conn.cursor()
        c.execute('''INSERT OR REPLACE INTO workers (worker_id, status, last_heartbeat, tasks_completed)
                    VALUES (?, ?, ?, ?)''',
                 (worker_id, 'online', datetime.now().isoformat(), 0))
        self.conn.commit()
        self.active_workers[worker_id] = True
        log.info("Worker '%s' registered", worker_id)

    def enqueue_task(self, task_name: str, description: str, user_id: str = "anonymous") -> int:
        """Add task to queue"""
        c = self.conn.cursor()
        c.execute('''INSERT INTO tasks (task_name, description, status, created_at, user_id)
                    VALUES (?, ?, ?, ?, ?)''',
                 (task_name, description, 'pending', datetime.now().isoformat(), user_id))
        self.conn.commit()
        log.info("Task '%s' enqueued (id=%d, user=%s)", task_name, c.lastrowid, user_id)
        return c.lastrowid

    def assign_task(self, task_id: int, worker_id: str) -> bool:
        """Assign task to a worker"""
        c = self.conn.cursor()
        c.execute('''UPDATE tasks SET status = ?, assigned_to = ? WHERE id = ?''',
                 ('assigned', worker_id, task_id))
        self.conn.commit()
        log.info("Task %d assigned to worker '%s'", task_id, worker_id)
        return True

    def complete_task(self, task_id: int, result: str):
        """Mark task as complete"""
        c = self.conn.cursor()
        c.execute('''UPDATE tasks SET status = ?, result = ?, completed_at = ? WHERE id = ?''',
                 ('completed', result, datetime.now().isoformat(), task_id))
        self.conn.commit()
        log.info("Task %d completed", task_id)

    def get_pending_tasks(self, limit: int = 10) -> list:
        """Get tasks waiting for a worker"""
        c = self.conn.cursor()
        c.execute('''SELECT id, task_name, description FROM tasks WHERE status = 'pending' LIMIT ?''', (limit,))
        return [{"id": row[0], "name": row[1], "description": row[2]} for row in c.fetchall()]

    def get_worker_stats(self) -> dict:
        """Get cluster statistics"""
        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM workers WHERE status = "online"')
        online_workers = c.fetchone()[0]

        c.execute('SELECT COUNT(*) FROM tasks WHERE status = "completed"')
        completed_tasks = c.fetchone()[0]

        return {
            "online_workers": online_workers,
            "completed_tasks": completed_tasks,
            "active_workers": list(self.active_workers.keys())
        }

    def record_heartbeat(self, worker_id: str):
        """Record a heartbeat from a worker, marking them online."""
        c = self.conn.cursor()
        now = datetime.now().isoformat()
        c.execute('''UPDATE workers SET status = ?, last_heartbeat = ? WHERE worker_id = ?''',
                  ('online', now, worker_id))
        if c.rowcount == 0:
            c.execute('''INSERT INTO workers (worker_id, status, last_heartbeat, tasks_completed)
                        VALUES (?, ?, ?, 0)''',
                     (worker_id, 'online', now))
        self.conn.commit()
        self.active_workers[worker_id] = True
        log.info("Heartbeat recorded for worker '%s'", worker_id)

    def mark_workers_offline(self, stale_threshold_seconds: int = 60):
        """Mark workers as offline if their heartbeat is older than the threshold.

        Returns the list of worker IDs that were marked offline.
        """
        c = self.conn.cursor()
        threshold = (datetime.now() - timedelta(seconds=stale_threshold_seconds)).isoformat()
        c.execute('''UPDATE workers SET status = ? WHERE status = ? AND last_heartbeat < ?''',
                  ('offline', 'online', threshold))
        affected = c.fetchone() if c.description else None
        self.conn.commit()
        log.info("Marked workers offline (threshold=%ss)", stale_threshold_seconds)
        return self._get_recently_offline_workers(threshold)

    def _get_recently_offline_workers(self, threshold: str) -> list:
        """Get workers that were just marked offline within this cycle."""
        c = self.conn.cursor()
        c.execute('''SELECT worker_id FROM workers WHERE status = ? AND last_heartbeat < ?''',
                  ('offline', threshold))
        workers = [row[0] for row in c.fetchall()]
        for w in workers:
            self.active_workers.pop(w, None)
        return workers

    def reassign_orphaned_tasks(self, stale_threshold_seconds: int = 60) -> int:
        """Reassign tasks that were assigned to workers who went offline.

        Returns the number of tasks reassigned.
        """
        c = self.conn.cursor()
        threshold = (datetime.now() - timedelta(seconds=stale_threshold_seconds)).isoformat()
        c.execute('''UPDATE tasks SET status = ?, assigned_to = NULL, completed_at = NULL
                     WHERE status = ? AND assigned_to IN (
                         SELECT worker_id FROM workers
                         WHERE status = ? AND last_heartbeat < ?
                     )''',
                  ('pending', 'assigned', 'offline', threshold))
        self.conn.commit()
        count = c.rowcount
        if count > 0:
            log.info("Reassigned %d orphaned tasks back to pending", count)
        return count

    def run_maintenance(self, stale_threshold_seconds: int = 60):
        """Run all maintenance tasks: detect stale workers and reassign orphaned tasks."""
        self.mark_workers_offline(stale_threshold_seconds)
        self.reassign_orphaned_tasks(stale_threshold_seconds)

# ============================================================================
# PHASE 4: INCENTIVES & REWARDS SYSTEM
# ============================================================================

class RewardsSystem:
    """Points, reputation, achievements, and leaderboard system."""

    POINTS_BASE_TASK = 10
    POINTS_QUALITY_BONUS_MULTIPLIER = 2
    POINTS_DISCOVERY = 5

    def __init__(self, memory: MemoryDB):
        self.memory = memory
        self.conn = memory.conn
        self.init_db()
        self.seed_achievements()

    def init_db(self):
        """Create rewards-related tables."""
        c = self.conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS user_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE,
            balance INTEGER DEFAULT 0,
            total_earned INTEGER DEFAULT 0,
            last_updated DATETIME
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS reward_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            task_id INTEGER,
            points INTEGER,
            reason TEXT,
            critic_score REAL,
            timestamp DATETIME
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS reputation_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE,
            overall_score REAL DEFAULT 0.0,
            task_completion_score REAL DEFAULT 0.0,
            quality_score REAL DEFAULT 0.0,
            consistency_score REAL DEFAULT 0.0,
            total_tasks INTEGER DEFAULT 0,
            last_updated DATETIME
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS achievements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            description TEXT,
            icon TEXT,
            criteria TEXT
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS user_achievements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            achievement_id INTEGER,
            earned_at DATETIME,
            FOREIGN KEY (achievement_id) REFERENCES achievements(id)
        )''')

        self.conn.commit()
        log.info("RewardsSystem initialized")

    def seed_achievements(self):
        """Insert default achievement definitions if they don't exist."""
        achievements = [
            ("first_task", "Complete your first research task", "🚀", "complete 1 task"),
            ("task_master_10", "Complete 10 research tasks", "🏆", "complete 10 tasks"),
            ("task_master_50", "Complete 50 research tasks", "👑", "complete 50 tasks"),
            ("quality_master", "Earn a perfect 10/10 critic score", "⭐", "earn critic score of 10"),
            ("high_quality_5", "Earn 5 scores of 8+", "💎", "earn 5 scores of 8 or higher"),
            ("perfect_streak_3", "3 consecutive tasks with score >= 9", "🔥", "3 consecutive scores >= 9"),
            ("discovery_finder_5", "Contribute to 5 discoveries", "🔬", "5 discoveries contributed"),
            ("knowledge_miner_10", "Extract 10 reusable patterns", "📚", "10 patterns extracted"),
            ("first_points", "Earn your first 100 points", "🪙", "earn 100 total points"),
            ("points_master_1000", "Earn 1000 total points", "💰", "earn 1000 total points"),
        ]
        c = self.conn.cursor()
        for name, desc, icon, criteria in achievements:
            try:
                c.execute('''INSERT OR IGNORE INTO achievements (name, description, icon, criteria)
                            VALUES (?, ?, ?, ?)''', (name, desc, icon, criteria))
            except Exception:
                pass
        self.conn.commit()

    # ------------------------------------------------------------------
    # Points
    # ------------------------------------------------------------------

    def get_or_create_user(self, user_id: str) -> dict:
        """Get or create a user points record."""
        c = self.conn.cursor()
        c.execute('''SELECT balance, total_earned FROM user_points WHERE user_id = ?''', (user_id,))
        row = c.fetchone()
        if row:
            return {"user_id": user_id, "balance": row[0], "total_earned": row[1]}
        c.execute('''INSERT INTO user_points (user_id, balance, total_earned, last_updated)
                    VALUES (?, 0, 0, ?)''', (user_id, datetime.now().isoformat()))
        self.conn.commit()
        return {"user_id": user_id, "balance": 0, "total_earned": 0}

    def award_points(self, user_id: str, points: int, reason: str, task_id: Optional[int] = None, critic_score: Optional[float] = None) -> dict:
        """Award points to a user and log the event."""
        c = self.conn.cursor()
        c.execute('''INSERT INTO user_points (user_id, balance, total_earned, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        balance = balance + ?,
                        total_earned = total_earned + ?,
                        last_updated = ?''',
                  (user_id, points, points, datetime.now().isoformat(),
                   points, points, datetime.now().isoformat()))
        c.execute('''INSERT INTO reward_events (user_id, task_id, points, reason, critic_score, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                  (user_id, task_id, points, reason, critic_score, datetime.now().isoformat()))
        self.conn.commit()
        c.execute('''SELECT balance, total_earned FROM user_points WHERE user_id = ?''', (user_id,))
        row = c.fetchone()
        return {"user_id": user_id, "balance": row[0], "total_earned": row[1], "awarded": points}

    def get_balance(self, user_id: str) -> dict:
        """Get a user's points balance and total earned."""
        return self.get_or_create_user(user_id)

    def get_transaction_history(self, user_id: str, limit: int = 20) -> list:
        """Get recent reward events for a user."""
        c = self.conn.cursor()
        c.execute('''SELECT id, task_id, points, reason, critic_score, timestamp
                     FROM reward_events WHERE user_id = ?
                     ORDER BY timestamp DESC LIMIT ?''', (user_id, limit))
        return [
            {
                "id": row[0],
                "task_id": row[1],
                "points": row[2],
                "reason": row[3],
                "critic_score": row[4],
                "timestamp": row[5],
            }
            for row in c.fetchall()
        ]

    # ------------------------------------------------------------------
    # Reputation
    # ------------------------------------------------------------------

    def update_reputation(self, user_id: str, critic_score: float, task_success: bool = True) -> dict:
        """Update a user's reputation based on a new task result."""
        c = self.conn.cursor()
        c.execute('''SELECT overall_score, task_completion_score, quality_score, consistency_score, total_tasks
                     FROM reputation_scores WHERE user_id = ?''', (user_id,))
        row = c.fetchone()

        if row:
            overall, completion, quality, consistency, total = row
            total += 1
            new_completion = (completion * (total - 1) + (1.0 if task_success else 0.0)) / total
            new_quality = (quality * (total - 1) + (critic_score / 10.0)) / total
            scores = [critic_score / 10.0]
            c.execute('''SELECT critic_score FROM reward_events
                         WHERE user_id = ? AND critic_score IS NOT NULL
                         ORDER BY timestamp DESC LIMIT 4''', (user_id,))
            prev = [r[0] for r in c.fetchall() if r[0] is not None]
            if prev:
                scores = [critic_score / 10.0] + prev[:4]
            variance = sum((s - new_quality) ** 2 for s in scores) / len(scores)
            new_consistency = max(0.0, 1.0 - variance * 2.0)
            new_overall = (new_completion * 0.3 + new_quality * 0.5 + new_consistency * 0.2)

            c.execute('''UPDATE reputation_scores SET
                         overall_score = ?, task_completion_score = ?, quality_score = ?,
                         consistency_score = ?, total_tasks = ?, last_updated = ?
                         WHERE user_id = ?''',
                      (round(new_overall, 4), round(new_completion, 4), round(new_quality, 4),
                       round(new_consistency, 4), total, datetime.now().isoformat(), user_id))
        else:
            new_overall = 0.5 if task_success else 0.3
            c.execute('''INSERT INTO reputation_scores
                         (user_id, overall_score, task_completion_score, quality_score, consistency_score, total_tasks, last_updated)
                         VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (user_id, new_overall, 1.0 if task_success else 0.0, critic_score / 10.0, 1.0, 1, datetime.now().isoformat()))

        self.conn.commit()
        return self.get_reputation(user_id)

    def get_reputation(self, user_id: str) -> dict:
        """Get a user's reputation scores."""
        c = self.conn.cursor()
        c.execute('''SELECT overall_score, task_completion_score, quality_score, consistency_score, total_tasks, last_updated
                     FROM reputation_scores WHERE user_id = ?''', (user_id,))
        row = c.fetchone()
        if row:
            return {
                "user_id": user_id,
                "overall_score": row[0],
                "task_completion_score": row[1],
                "quality_score": row[2],
                "consistency_score": row[3],
                "total_tasks": row[4],
                "last_updated": row[5],
            }
        return {"user_id": user_id, "overall_score": 0.0, "total_tasks": 0}

    # ------------------------------------------------------------------
    # Achievements
    # ------------------------------------------------------------------

    def check_achievements(self, user_id: str) -> list:
        """Check and award any newly earned achievements for a user."""
        c = self.conn.cursor()
        new_achievements = []
        user_data = self.get_or_create_user(user_id)
        rep = self.get_reputation(user_id)
        total_tasks = rep["total_tasks"]
        total_earned = user_data["total_earned"]

        c.execute('''SELECT a.id, a.name, a.description, a.icon, a.criteria
                     FROM achievements a
                     WHERE a.id NOT IN (
                         SELECT achievement_id FROM user_achievements WHERE user_id = ?
                     )''', (user_id,))
        available = c.fetchall()

        c.execute('''SELECT critic_score FROM reward_events
                     WHERE user_id = ? AND critic_score IS NOT NULL
                     ORDER BY timestamp DESC''', (user_id,))
        scores = [r[0] for r in c.fetchall()]

        for ach_id, name, desc, icon, criteria in available:
            earned = False
            if name == "first_task" and total_tasks >= 1:
                earned = True
            elif name == "task_master_10" and total_tasks >= 10:
                earned = True
            elif name == "task_master_50" and total_tasks >= 50:
                earned = True
            elif name == "quality_master" and any(s == 10.0 for s in scores):
                earned = True
            elif name == "high_quality_5" and sum(1 for s in scores if s >= 8) >= 5:
                earned = True
            elif name == "perfect_streak_3":
                streak = 0
                for s in scores:
                    if s >= 9:
                        streak += 1
                        if streak >= 3:
                            earned = True
                            break
                    else:
                        streak = 0
            elif name == "discovery_finder_5":
                c.execute('''SELECT COUNT(*) FROM discoveries WHERE agent_who_found = ?''', (user_id,))
                if c.fetchone()[0] >= 5:
                    earned = True
            elif name == "knowledge_miner_10":
                c.execute('''SELECT COUNT(*) FROM patterns''')
                if c.fetchone()[0] >= 10:
                    earned = True
            elif name == "first_points" and total_earned >= 100:
                earned = True
            elif name == "points_master_1000" and total_earned >= 1000:
                earned = True

            if earned:
                c.execute('''INSERT INTO user_achievements (user_id, achievement_id, earned_at)
                            VALUES (?, ?, ?)''', (user_id, ach_id, datetime.now().isoformat()))
                new_achievements.append({"id": ach_id, "name": name, "description": desc, "icon": icon})

        if new_achievements:
            self.conn.commit()
            log.info("Awarded %d new achievement(s) to user '%s': %s",
                     len(new_achievements), user_id, [a["name"] for a in new_achievements])
        return new_achievements

    def get_user_achievements(self, user_id: str) -> list:
        """Get all achievements earned by a user."""
        c = self.conn.cursor()
        c.execute('''SELECT a.id, a.name, a.description, a.icon, ua.earned_at
                     FROM user_achievements ua
                     JOIN achievements a ON a.id = ua.achievement_id
                     WHERE ua.user_id = ?
                     ORDER BY ua.earned_at DESC''', (user_id,))
        return [
            {"id": row[0], "name": row[1], "description": row[2], "icon": row[3], "earned_at": row[4]}
            for row in c.fetchall()
        ]

    def get_all_achievements(self) -> list:
        """Get all available achievements."""
        c = self.conn.cursor()
        c.execute('''SELECT id, name, description, icon, criteria FROM achievements ORDER BY id''')
        return [
            {"id": row[0], "name": row[1], "description": row[2], "icon": row[3], "criteria": row[4]}
            for row in c.fetchall()
        ]

    # ------------------------------------------------------------------
    # Leaderboard
    # ------------------------------------------------------------------

    def get_leaderboard(self, metric: str = "points", limit: int = 20) -> list:
        """Get leaderboard ranked by the given metric.

        Supported metrics: points, reputation, tasks, quality.
        """
        c = self.conn.cursor()
        if metric == "points":
            c.execute('''SELECT user_id, balance, total_earned FROM user_points
                         ORDER BY total_earned DESC LIMIT ?''', (limit,))
            return [
                {"rank": i + 1, "user_id": row[0], "balance": row[1], "total_earned": row[2]}
                for i, row in enumerate(c.fetchall())
            ]
        elif metric == "reputation":
            c.execute('''SELECT user_id, overall_score, quality_score, total_tasks FROM reputation_scores
                         ORDER BY overall_score DESC LIMIT ?''', (limit,))
            return [
                {"rank": i + 1, "user_id": row[0], "overall_score": row[1], "quality_score": row[2], "total_tasks": row[3]}
                for i, row in enumerate(c.fetchall())
            ]
        elif metric == "tasks":
            c.execute('''SELECT user_id, total_tasks, quality_score FROM reputation_scores
                         ORDER BY total_tasks DESC LIMIT ?''', (limit,))
            return [
                {"rank": i + 1, "user_id": row[0], "total_tasks": row[1], "quality_score": row[2]}
                for i, row in enumerate(c.fetchall())
            ]
        elif metric == "quality":
            c.execute('''SELECT user_id, quality_score, total_tasks FROM reputation_scores
                         ORDER BY quality_score DESC LIMIT ?''', (limit,))
            return [
                {"rank": i + 1, "user_id": row[0], "quality_score": row[1], "total_tasks": row[2]}
                for i, row in enumerate(c.fetchall())
            ]
        return []

    @staticmethod
    def parse_critic_score(criticism: str) -> float:
        """Extract the numeric score from a critic's output.

        Looks for patterns like 'Overall score: X/10', 'Score: X/10', or 'X/10'.
        Returns 0.0 if unable to parse.
        """
        import re
        patterns = [
            r"overall score[:\s]*(\d+(?:\.\d+)?)\s*/\s*10",
            r"score[:\s]*(\d+(?:\.\d+)?)\s*/\s*10",
            r"(\d+(?:\.\d+)?)\s*/\s*10",
        ]
        for p in patterns:
            match = re.search(p, criticism, re.IGNORECASE)
            if match:
                score = float(match.group(1))
                return max(0.0, min(10.0, score))
        log.warning("Could not parse critic score from output, defaulting to 0.0")
        return 0.0

    def process_task_completion(self, user_id: str, task_id: int, critic_score: float, task_success: bool = True) -> dict:
        """Full rewards pipeline for a completed task.

        Awards base points, quality bonus, updates reputation, and checks achievements.
        """
        points = self.POINTS_BASE_TASK
        if critic_score >= 8.0:
            bonus = int(critic_score * self.POINTS_QUALITY_BONUS_MULTIPLIER)
            points += bonus

        result = self.award_points(
            user_id=user_id,
            points=points,
            reason="task_completed" if critic_score < 8.0 else "task_completed_with_quality_bonus",
            task_id=task_id,
            critic_score=critic_score
        )
        rep = self.update_reputation(user_id, critic_score, task_success)
        new_achievements = self.check_achievements(user_id)

        return {
            "points_awarded": points,
            "new_balance": result["balance"],
            "total_earned": result["total_earned"],
            "reputation": rep,
            "new_achievements": new_achievements,
        }


# ============================================================================
# LIFESPAN (startup / shutdown)
# ============================================================================

# ---------------------------------------------------------------------------
# Background maintenance: detect stale workers, reassign orphaned tasks
# ---------------------------------------------------------------------------

_stop_event = asyncio.Event()

async def maintenance_loop(interval_seconds: int = 30, stale_threshold: int = 60):
    """Periodically detect stale workers and reassign orphaned tasks."""
    log.info("Maintenance loop started (interval=%ss, stale_threshold=%ss)", interval_seconds, stale_threshold)
    try:
        while not _stop_event.is_set():
            await asyncio.sleep(interval_seconds)
            task_queue.run_maintenance(stale_threshold)
    except asyncio.CancelledError:
        log.info("Maintenance loop cancelled")
    except Exception as e:
        log.error("Maintenance loop error: %s", e)

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Forge AGI starting up")
    task = asyncio.create_task(maintenance_loop())
    yield
    log.info("Forge AGI shutting down — closing database connections")
    _stop_event.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    memory.close()
    task_queue.close()

# ============================================================================
# API SETUP
# ============================================================================

app = FastAPI(title="Forge AGI", description="Distributed AI Research Platform", lifespan=lifespan)

memory = MemoryDB()
task_queue = TaskQueue()
rewards = RewardsSystem(memory)
agents = {
    "thinker": ThinkerAgent(memory),
    "coder": CoderAgent(memory),
    "critic": CriticAgent(memory),
    "learner": LearnerAgent(memory)
}

class ResearchTask(BaseModel):
    task_name: str
    description: str
    user_id: Optional[str] = "anonymous"

class WorkerRegistration(BaseModel):
    worker_id: str

class TaskCompletion(BaseModel):
    result: str

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "agents_loaded": 4,
        "database": "connected"
    }

@app.post("/research/task")
async def create_research_task(task: ResearchTask):
    """Create a new research task that agents will solve collaboratively"""
    log.info("API: create_research_task '%s' by user '%s'", task.task_name, task.user_id)
    task_id = task_queue.enqueue_task(task.task_name, task.description, task.user_id)

    similar = memory.find_similar_solution(task.description)
    if similar:
        return {
            "task_id": task_id,
            "status": "created",
            "similar_solution_found": True,
            "hint": f"We solved something similar before: {similar['task_name']}",
            "previous_solution": similar['solution']
        }

    return {
        "task_id": task_id,
        "status": "created",
        "similar_solution_found": False
    }

@app.post("/research/solve/{task_id}")
async def solve_research_task(task_id: int):
    """Solve a research task using multi-agent collaboration"""
    log.info("API: solve_research_task task_id=%d", task_id)
    c = task_queue.conn.cursor()
    c.execute('SELECT description, user_id FROM tasks WHERE id = ?', (task_id,))
    row = c.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")

    description, user_id = row

    recommendations = memory.get_recommendations(description)

    def generate():
        try:
            if recommendations["similar_solution"]:
                sim = recommendations["similar_solution"]
                data = json.dumps({
                    "type": "similar_solution",
                    "task_name": sim["task_name"],
                    "hint": f"Found similar solution: {sim['task_name']}"
                })
                yield f"event: recommendation\ndata: {data}\n\n"

            if recommendations["relevant_patterns"]:
                data = json.dumps({
                    "type": "patterns",
                    "count": len(recommendations["relevant_patterns"])
                })
                yield f"event: recommendation\ndata: {data}\n\n"

            yield "event: stage\ndata: {\"agent\": \"thinker\", \"message\": \"Analyzing problem...\"}\n\n"
            thinking = agents["thinker"].think(description)
            yield f"event: result\ndata: {json.dumps({'stage': 'thinker', 'output': thinking[:200] + '...'})}\n\n"

            yield "event: stage\ndata: {\"agent\": \"coder\", \"message\": \"Writing solution...\"}\n\n"
            coding_prompt = f"Based on this plan: {thinking}\n\nNow write the code:"
            code = agents["coder"].think(coding_prompt)
            yield f"event: result\ndata: {json.dumps({'stage': 'coder', 'output': code[:200] + '...'})}\n\n"

            yield "event: stage\ndata: {\"agent\": \"critic\", \"message\": \"Reviewing solution...\"}\n\n"
            criticism = agents["critic"].think(f"Review this code:\n{code}")
            yield f"event: result\ndata: {json.dumps({'stage': 'critic', 'output': criticism[:200] + '...'})}\n\n"

            yield "event: stage\ndata: {\"agent\": \"learner\", \"message\": \"Storing knowledge...\"}\n\n"
            insights = agents["learner"].think(f"What did we learn from this solution? Key insights:\n{code}")
            yield f"event: result\ndata: {json.dumps({'stage': 'learner', 'output': insights[:200] + '...'})}\n\n"

            domain = memory.extract_domain(description)
            reused_from = recommendations["similar_solution"]["task_name"] if recommendations["similar_solution"] else None
            memory.store_experiment(
                task_name=f"task_{task_id}",
                description=description,
                solution=code,
                success=True,
                agent_type="multi-agent-ensemble",
                reused_from=reused_from
            )
            memory.store_discovery(insights, 0.8, user_id)

            memory.store_pattern(
                pattern_type="code_solution",
                description=f"Solution for: {description[:100]}",
                code_template=code[:500],
                success_rate=0.8,
                domain=domain
            )

            task_queue.complete_task(task_id, code)

            critic_score = RewardsSystem.parse_critic_score(criticism)
            rewards_result = rewards.process_task_completion(user_id, task_id, critic_score)

            rewards_data = json.dumps({
                "points_awarded": rewards_result["points_awarded"],
                "new_balance": rewards_result["new_balance"],
                "critic_score": critic_score,
                "new_achievements": rewards_result["new_achievements"],
            })
            yield f"event: rewards\ndata: {rewards_data}\n\n"

            yield f"event: complete\ndata: {json.dumps({'status': 'completed', 'task_id': task_id})}\n\n"

        except Exception as e:
            log.error("Solve pipeline failed for task %d: %s", task_id, e)
            yield f"event: error\ndata: {json.dumps({'status': 'failed', 'task_id': task_id, 'error': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/research/history")
async def get_research_history():
    """Get all experiments the system has run"""
    log.info("API: get_research_history")
    c = memory.conn.cursor()
    c.execute('SELECT task_name, description, success FROM experiments ORDER BY timestamp DESC LIMIT 20')
    results = [{"task": row[0], "description": row[1], "success": bool(row[2])} for row in c.fetchall()]
    return {"experiments": results}

@app.get("/research/discoveries")
async def get_discoveries():
    """Get recent discoveries learned by the system"""
    log.info("API: get_discoveries")
    discoveries = memory.get_recent_discoveries(10)
    return {"discoveries": discoveries}

@app.get("/research/recommendations")
async def get_recommendations(task_description: str, limit: int = 3):
    """Get similar solutions and relevant patterns for a task (Phase 3: Knowledge Sharing)."""
    log.info("API: get_recommendations")
    return memory.get_recommendations(task_description, limit)

@app.get("/discoveries/trending")
async def get_trending_discoveries(limit: int = 10, days: int = 30):
    """Get trending discoveries sorted by trending score (Phase 3: Trending)."""
    log.info("API: get_trending_discoveries")
    return {"trending": memory.get_trending_discoveries(limit, days)}

@app.get("/patterns/search")
async def search_patterns(
    pattern_type: Optional[str] = None,
    domain: Optional[str] = None,
    min_success_rate: float = 0.0,
    limit: int = 20,
    offset: int = 0
):
    """Search patterns with filters and pagination (Phase 3: Knowledge Sharing)."""
    log.info("API: search_patterns")
    return memory.search_patterns(pattern_type, domain, min_success_rate, limit, offset)

@app.get("/patterns/cross-domain")
async def get_cross_domain_patterns(min_domains: int = 2, limit: int = 10):
    """Find patterns that appear across multiple domains (Phase 3: Cross-domain)."""
    log.info("API: get_cross_domain_patterns")
    return {"cross_domain_patterns": memory.get_cross_domain_patterns(min_domains, limit)}

@app.get("/research/reuse-rate")
async def get_reuse_rate():
    """Get the current reuse rate of solutions (Phase 3: Reuse tracking)."""
    log.info("API: get_reuse_rate")
    return memory.get_reuse_rate()

@app.post("/workers/register")
async def register_worker(worker: WorkerRegistration):
    """Register a distributed compute worker"""
    log.info("API: register_worker '%s'", worker.worker_id)
    task_queue.register_worker(worker.worker_id)
    return {
        "worker_id": worker.worker_id,
        "status": "registered",
        "pending_tasks": task_queue.get_pending_tasks(5)
    }

@app.post("/workers/heartbeat")
async def worker_heartbeat(worker: WorkerRegistration):
    """Record a heartbeat from a worker to keep it marked online."""
    log.info("API: heartbeat from worker '%s'", worker.worker_id)
    task_queue.record_heartbeat(worker.worker_id)
    return {"worker_id": worker.worker_id, "status": "online", "timestamp": datetime.now().isoformat()}

@app.get("/workers/stats")
async def get_worker_stats():
    """Get cluster statistics"""
    return task_queue.get_worker_stats()

@app.get("/tasks/pending")
async def get_pending_tasks():
    """Get list of pending tasks (for workers to pull)"""
    return {"tasks": task_queue.get_pending_tasks()}

@app.post("/tasks/{task_id}/complete")
async def complete_task(task_id: int, completion: TaskCompletion):
    """Mark a task as complete (called by worker)"""
    log.info("API: complete_task task_id=%d", task_id)
    task_queue.complete_task(task_id, completion.result)
    return {"status": "completed", "task_id": task_id}

# ---------------------------------------------------------------------------
# Phase 4: Incentives & Rewards Endpoints
# ---------------------------------------------------------------------------

@app.get("/rewards/balance/{user_id}")
async def get_points_balance(user_id: str):
    """Get a user's points balance (Phase 4: Points System)."""
    log.info("API: get_points_balance user='%s'", user_id)
    return rewards.get_balance(user_id)

@app.get("/rewards/history/{user_id}")
async def get_reward_history(user_id: str, limit: int = 20):
    """Get a user's reward transaction history (Phase 4: Points System)."""
    log.info("API: get_reward_history user='%s'", user_id)
    return {"transactions": rewards.get_transaction_history(user_id, limit)}

@app.get("/rewards/reputation/{user_id}")
async def get_reputation(user_id: str):
    """Get a user's reputation scores (Phase 4: Reputation)."""
    log.info("API: get_reputation user='%s'", user_id)
    return rewards.get_reputation(user_id)

@app.get("/rewards/leaderboard")
async def get_leaderboard(metric: str = "points", limit: int = 20):
    """Get the leaderboard ranked by points, reputation, tasks, or quality (Phase 4: Leaderboards)."""
    log.info("API: get_leaderboard metric='%s' limit=%d", metric, limit)
    return {"leaderboard": rewards.get_leaderboard(metric, limit)}

@app.get("/rewards/achievements")
async def list_achievements():
    """List all available achievements (Phase 4: Achievements)."""
    log.info("API: list_achievements")
    return {"achievements": rewards.get_all_achievements()}

@app.get("/rewards/achievements/{user_id}")
async def get_user_achievements(user_id: str):
    """Get achievements earned by a user (Phase 4: Achievements)."""
    log.info("API: get_user_achievements user='%s'", user_id)
    return {"achievements": rewards.get_user_achievements(user_id)}

# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    """API overview"""
    return {
        "system": "Forge AGI",
        "version": "0.2.0",
        "features": [
            "Multi-Agent Autonomous Collaboration",
            "Persistent Learning Memory",
            "Distributed Task Orchestration",
            "Knowledge Sharing & Discovery Trending",
            "Incentives & Rewards System"
        ],
        "endpoints": {
            "health": {
                "GET /health": "Health check"
            },
            "research": {
                "POST /research/task": "Create a new research task",
                "POST /research/solve/{task_id}": "Solve task with agent collaboration",
                "GET /research/history": "View past experiments",
                "GET /research/discoveries": "View system discoveries",
                "GET /research/recommendations": "Get similar solutions and patterns (Phase 3)",
                "GET /research/reuse-rate": "Get solution reuse rate (Phase 3)"
            },
            "knowledge": {
                "GET /discoveries/trending": "Get trending discoveries (Phase 3)",
                "GET /patterns/search": "Search reusable patterns (Phase 3)",
                "GET /patterns/cross-domain": "Find cross-domain patterns (Phase 3)"
            },
            "workers": {
                "POST /workers/register": "Register a compute worker",
                "POST /workers/heartbeat": "Send heartbeat to stay online",
                "GET /workers/stats": "Get cluster statistics",
                "GET /tasks/pending": "Get pending tasks",
                "POST /tasks/{task_id}/complete": "Mark task complete"
            },
            "rewards": {
                "GET /rewards/balance/{user_id}": "Get points balance (Phase 4)",
                "GET /rewards/history/{user_id}": "Get reward transaction history (Phase 4)",
                "GET /rewards/reputation/{user_id}": "Get reputation scores (Phase 4)",
                "GET /rewards/leaderboard": "Get leaderboard (Phase 4)",
                "GET /rewards/achievements": "List all achievements (Phase 4)",
                "GET /rewards/achievements/{user_id}": "Get user achievements (Phase 4)"
            }
        }
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
