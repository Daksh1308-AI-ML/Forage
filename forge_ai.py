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
import re
import time
from typing import Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse, Response
import uvicorn
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Prometheus monitoring
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
import psutil

# RabbitMQ
try:
    from aio_pika import connect_robust, Message, DeliveryMode, ExchangeType
    from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel
    AIO_PIKA_AVAILABLE = True
except ImportError:
    AIO_PIKA_AVAILABLE = False

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
        class messages:
            @staticmethod
            def create(*args, **kwargs):
                class Response:
                    def __init__(self):
                        content_item = type("ContentItem", (), {"text": "Anthropic client unavailable. Install the 'anthropic' package to enable AI features."})
                        self.content = [content_item]
                return Response()

# ============================================================================
# Prometheus metrics
# ============================================================================

forge_tasks_total = Counter('forge_tasks_total', 'Total tasks created/completed/failed', ['status'])
forge_active_workers = Gauge('forge_active_workers', 'Number of active workers')
forge_request_duration_seconds = Histogram('forge_request_duration_seconds', 'API request duration', ['method', 'endpoint'])
forge_memory_usage_bytes = Gauge('forge_memory_usage_bytes', 'Memory usage of the process')
forge_db_connections = Gauge('forge_db_connections', 'Database connection count')

# ============================================================================
# DATABASE BACKEND (SQLite sync + PostgreSQL async)
# ============================================================================

class DatabaseBackend:
    """Abstract database backend supporting SQLite (sync) and PostgreSQL (async)."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.is_postgres = bool(db_path and db_path.startswith("postgresql://"))
        self._conn = None
        self._pool = None

    async def connect(self):
        if self.is_postgres:
            import asyncpg
            dsn = os.environ.get("DATABASE_URL", self.db_path)
            self._pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        else:
            if self._conn is None:
                path = self.db_path or ":memory:"
                self._conn = sqlite3.connect(path, check_same_thread=False)

    def connect_sync(self):
        """Connect to SQLite synchronously (for backward compatibility in __init__)."""
        if not self.is_postgres:
            path = self.db_path or ":memory:"
            self._conn = sqlite3.connect(path, check_same_thread=False)
            return self._conn
        raise RuntimeError("Cannot connect to PostgreSQL synchronously")

    async def execute(self, sql: str, params=None):
        if self.is_postgres:
            async with self._pool.acquire() as conn:
                pg_sql = self._convert_placeholders(sql)
                if params:
                    return await conn.execute(pg_sql, *params)
                return await conn.execute(pg_sql)
        else:
            return self._conn.execute(sql, params or [])

    async def executemany(self, sql: str, params_list: list):
        if self.is_postgres:
            async with self._pool.acquire() as conn:
                pg_sql = self._convert_placeholders(sql)
                await conn.executemany(pg_sql, params_list)
        else:
            self._conn.executemany(sql, params_list)

    async def fetchall(self, sql: str, params=None) -> list:
        if self.is_postgres:
            async with self._pool.acquire() as conn:
                pg_sql = self._convert_placeholders(sql)
                if params:
                    rows = await conn.fetch(pg_sql, *params)
                else:
                    rows = await conn.fetch(pg_sql)
                return [list(r.values()) for r in rows]
        else:
            return self._conn.execute(sql, params or []).fetchall()

    async def fetchone(self, sql: str, params=None):
        if self.is_postgres:
            async with self._pool.acquire() as conn:
                pg_sql = self._convert_placeholders(sql)
                if params:
                    row = await conn.fetchrow(pg_sql, *params)
                else:
                    row = await conn.fetchrow(pg_sql)
                return list(row.values()) if row else None
        else:
            return self._conn.execute(sql, params or []).fetchone()

    async def fetchval(self, sql: str, params=None):
        if self.is_postgres:
            async with self._pool.acquire() as conn:
                pg_sql = self._convert_placeholders(sql)
                if params:
                    return await conn.fetchval(pg_sql, *params)
                return await conn.fetchval(pg_sql)
        else:
            row = self._conn.execute(sql, params or []).fetchone()
            return row[0] if row else None

    async def execute_insert_id(self, sql: str, params=None) -> int:
        if self.is_postgres:
            if "RETURNING" not in sql.upper():
                sql = sql.rstrip(";") + " RETURNING id"
            async with self._pool.acquire() as conn:
                pg_sql = self._convert_placeholders(sql)
                if params:
                    row = await conn.fetchrow(pg_sql, *params)
                else:
                    row = await conn.fetchrow(pg_sql)
                return row["id"] if row else None
        else:
            c = self._conn.execute(sql, params or [])
            return c.lastrowid

    async def commit(self):
        if not self.is_postgres:
            self._conn.commit()

    async def close(self):
        if self.is_postgres:
            if self._pool:
                await self._pool.close()
        else:
            if self._conn:
                self._conn.close()

    def _convert_placeholders(self, sql: str) -> str:
        if not self.is_postgres:
            return sql
        i = 0
        def repl(m):
            nonlocal i
            i += 1
            return f"${i}"
        return re.sub(r'\?', repl, sql)

    async def table_exists(self, table_name: str) -> bool:
        if self.is_postgres:
            row = await self.fetchone(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?", (table_name,)
            )
            return row is not None
        else:
            row = await self.fetchone(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
            )
            return row is not None

    async def column_exists(self, table_name: str, column_name: str) -> bool:
        if self.is_postgres:
            row = await self.fetchone(
                "SELECT 1 FROM information_schema.columns WHERE table_name=? AND column_name=?",
                (table_name, column_name)
            )
            return row is not None
        else:
            try:
                await self.fetchall(f"SELECT {column_name} FROM {table_name} LIMIT 0")
                return True
            except Exception:
                return False

# ============================================================================
# VECTOR STORE (semantic search with embeddings)
# ============================================================================

class VectorStore:
    """Vector search using sentence-transformers with pgvector or numpy fallback."""

    def __init__(self, backend: DatabaseBackend, model_name: Optional[str] = None):
        self.backend = backend
        self.model_name = model_name or os.environ.get("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")
        self._model = None
        self._model_available = False

    def _load_model(self):
        if self._model is not None:
            return True
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
            self._model_available = True
            return True
        except Exception as e:
            log.warning("Embedding model '%s' not available: %s", self.model_name, e)
            self._model_available = False
            return False

    def embed(self, text: str) -> list[float]:
        if not self._load_model():
            return []
        return self._model.encode(text).tolist()

    async def _init_embedding_table(self):
        """Create the embeddings table if it doesn't exist."""
        if self.backend.is_postgres:
            try:
                await self.backend.execute("CREATE EXTENSION IF NOT EXISTS vector")
            except Exception:
                log.warning("pgvector extension not available")
            await self.backend.execute("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    id SERIAL PRIMARY KEY,
                    source_table TEXT NOT NULL,
                    record_id INTEGER NOT NULL,
                    embedding vector(384),
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
        else:
            await self.backend.execute("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_table TEXT NOT NULL,
                    record_id INTEGER NOT NULL,
                    embedding TEXT,
                    metadata TEXT,
                    created_at DATETIME
                )
            """)
        await self.backend.commit()

    async def store_embedding(self, table: str, record_id: int, text: str, metadata: Optional[dict] = None):
        if not text:
            return
        embedding = self.embed(text)
        if not embedding:
            return
        await self._init_embedding_table()
        meta_json = json.dumps(metadata or {})
        if self.backend.is_postgres:
            emb_str = "[" + ",".join(str(x) for x in embedding) + "]"
            await self.backend.execute(
                "INSERT INTO embeddings (source_table, record_id, embedding, metadata) VALUES (?, ?, ?::vector, ?)",
                (table, record_id, emb_str, meta_json)
            )
        else:
            emb_json = json.dumps(embedding)
            await self.backend.execute(
                "INSERT INTO embeddings (source_table, record_id, embedding, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
                (table, record_id, emb_json, meta_json, datetime.now().isoformat())
            )
        await self.backend.commit()

    async def search_similar(self, text: str, table: str, top_k: int = 5) -> list[dict]:
        query_emb = self.embed(text)
        if not query_emb:
            return []

        await self._init_embedding_table()

        if self.backend.is_postgres:
            try:
                emb_str = "[" + ",".join(str(x) for x in query_emb) + "]"
                rows = await self.backend.fetchall(
                    """SELECT record_id, metadata, embedding <=> ?::vector(384) as distance
                       FROM embeddings WHERE source_table = ?
                       ORDER BY distance ASC LIMIT ?""",
                    (emb_str, table, top_k)
                )
                results = []
                for row in rows:
                    results.append({
                        "record_id": row[0],
                        "metadata": json.loads(row[1]) if row[1] else {},
                        "distance": float(row[2]),
                        "score": 1.0 - float(row[2])
                    })
                return results
            except Exception as e:
                log.warning("pgvector search failed, falling back: %s", e)
                return await self._fallback_search(query_emb, table, top_k)
        else:
            return await self._fallback_search(query_emb, table, top_k)

    async def _fallback_search(self, query_emb: list[float], table: str, top_k: int) -> list[dict]:
        """Fallback: load all embeddings from table, compute cosine similarity in Python."""
        import math
        rows = await self.backend.fetchall(
            "SELECT id, record_id, embedding, metadata FROM embeddings WHERE source_table = ?",
            (table,)
        )

        def cosine_sim(a, b):
            dot = sum(ai * bi for ai, bi in zip(a, b))
            na = math.sqrt(sum(ai * ai for ai in a))
            nb = math.sqrt(sum(bi * bi for bi in b))
            if na == 0 or nb == 0:
                return 0.0
            return dot / (na * nb)

        scored = []
        for row in rows:
            if not row[2]:
                continue
            try:
                emb = json.loads(row[2])
            except Exception:
                continue
            score = cosine_sim(query_emb, emb)
            scored.append({
                "record_id": row[1],
                "metadata": json.loads(row[3]) if row[3] else {},
                "distance": 1.0 - score,
                "score": score
            })

        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:top_k]

# ============================================================================
# FEATURE 1: PERSISTENT MEMORY SYSTEM (Vector-like embedding storage)
# ============================================================================

class MemoryDB:
    """Simple but powerful memory system - stores solutions and reuses them"""

    def __init__(self, db_path="forge_memory.db"):
        self.db_path = db_path
        self.backend = DatabaseBackend(db_path)
        self.vector_store = VectorStore(self.backend)
        if not self.backend.is_postgres:
            self._init_sqlite()

    def _init_sqlite(self):
        """Initialize SQLite synchronously (backward compatible with old tests)."""
        conn = self.backend.connect_sync()
        conn.execute('''CREATE TABLE IF NOT EXISTS experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT UNIQUE,
            description TEXT,
            solution TEXT,
            success INTEGER,
            timestamp DATETIME,
            agent_type TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_type TEXT,
            description TEXT,
            code_template TEXT,
            success_rate REAL,
            timestamp DATETIME
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS discoveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discovery TEXT,
            relevance_score REAL,
            agent_who_found TEXT,
            timestamp DATETIME
        )''')
        conn.commit()
        self._migrate_schema_sync()
        log.info("MemoryDB initialized at %s", self.db_path)

    def _migrate_schema_sync(self):
        migrations = [
            "ALTER TABLE experiments ADD COLUMN reused_from TEXT DEFAULT NULL",
            "ALTER TABLE patterns ADD COLUMN domain TEXT DEFAULT 'general'",
            "ALTER TABLE patterns ADD COLUMN reuse_count INTEGER DEFAULT 0",
            "ALTER TABLE patterns ADD COLUMN last_used DATETIME DEFAULT NULL",
            "ALTER TABLE discoveries ADD COLUMN trending_score REAL DEFAULT 0.0",
            "ALTER TABLE discoveries ADD COLUMN reuse_count INTEGER DEFAULT 0",
        ]
        conn = self.backend._conn
        for sql in migrations:
            try:
                conn.execute(sql)
            except Exception:
                pass
        conn.commit()

    async def init_db(self):
        if self.backend.is_postgres:
            dsn = os.environ.get("DATABASE_URL", self.db_path)
            self.backend.db_path = dsn
            await self.backend.connect()
            c = self.backend
            await c.execute('''CREATE TABLE IF NOT EXISTS experiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT UNIQUE,
                description TEXT,
                solution TEXT,
                success INTEGER,
                timestamp DATETIME,
                agent_type TEXT
            )''')
            await c.execute('''CREATE TABLE IF NOT EXISTS patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_type TEXT,
                description TEXT,
                code_template TEXT,
                success_rate REAL,
                timestamp DATETIME
            )''')
            await c.execute('''CREATE TABLE IF NOT EXISTS discoveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discovery TEXT,
                relevance_score REAL,
                agent_who_found TEXT,
                timestamp DATETIME
            )''')
            await c.commit()
            await self._migrate_schema()
            log.info("MemoryDB initialized at %s", self.db_path)

    async def _migrate_schema(self):
        c = self.backend
        if self.backend.is_postgres:
            cols_to_add = [
                ("experiments", "reused_from", "TEXT", "NULL"),
                ("patterns", "domain", "TEXT", "'general'"),
                ("patterns", "reuse_count", "INTEGER", "0"),
                ("patterns", "last_used", "TIMESTAMP", "NULL"),
                ("discoveries", "trending_score", "REAL", "0.0"),
                ("discoveries", "reuse_count", "INTEGER", "0"),
            ]
            for tbl, col, dtype, default in cols_to_add:
                exists = await c.column_exists(tbl, col)
                if not exists:
                    try:
                        await c.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {dtype} DEFAULT {default}")
                    except Exception:
                        pass
        else:
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
                    await c.execute(sql)
                except Exception:
                    pass
        await c.commit()

    async def close(self):
        await self.backend.close()
        log.info("MemoryDB connection closed")

    async def store_experiment(self, task_name: str, description: str, solution: str, success: bool, agent_type: str, reused_from: Optional[str] = None):
        try:
            row_id = await self.backend.execute_insert_id(
                '''INSERT OR REPLACE INTO experiments
                    (task_name, description, solution, success, timestamp, agent_type, reused_from)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (task_name, description, solution, int(success), datetime.now().isoformat(), agent_type, reused_from)
            )
            await self.backend.commit()
            if row_id:
                await self.vector_store.store_embedding("experiments", row_id, f"{task_name} {description}", {
                    "task_name": task_name, "agent_type": agent_type
                })
            log.info("Stored experiment '%s' (success=%s, reused_from=%s)", task_name, success, reused_from)
        except Exception as e:
            log.error("Error storing experiment: %s", e)

    async def find_similar_solution(self, task_description: str) -> Optional[dict]:
        similar = await self.vector_store.search_similar(task_description, "experiments", top_k=1)
        if similar and similar[0]["score"] >= 0.5:
            row_id = similar[0]["record_id"]
            row = await self.backend.fetchone("SELECT * FROM experiments WHERE id = ?", (row_id,))
            if row:
                log.info("Found similar solution via vector search (score=%.3f)", similar[0]["score"])
                return {
                    "task_name": row[1],
                    "description": row[2],
                    "solution": row[3],
                    "agent_type": row[6]
                }

        keywords = [w for w in task_description.lower().split() if len(w) > 2]
        if not keywords:
            return None

        best_match = None
        best_score = 0.0

        all_rows = await self.backend.fetchall('''SELECT * FROM experiments WHERE success = 1''')
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
            log.info("Found similar solution via keyword match (score=%.2f)", best_score)
            return best_match
        log.info("No similar solution found (best=%.2f)", best_score)
        return None

    async def store_pattern(self, pattern_type: str, description: str, code_template: str, success_rate: float, domain: str = "general"):
        try:
            row_id = await self.backend.execute_insert_id(
                '''INSERT INTO patterns (pattern_type, description, code_template, success_rate, timestamp, domain)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                (pattern_type, description, code_template, success_rate, datetime.now().isoformat(), domain)
            )
            await self.backend.commit()
            if row_id:
                await self.vector_store.store_embedding("patterns", row_id, f"{pattern_type} {description}", {
                    "pattern_type": pattern_type, "domain": domain
                })
            log.info("Stored pattern '%s' in domain '%s' (success_rate=%.2f)", pattern_type, domain, success_rate)
        except Exception as e:
            log.error("Error storing pattern: %s", e)

    async def find_relevant_patterns(self, keywords: list, limit: int = 5) -> list:
        keyword_str = " ".join(keywords)
        vector_results = await self.vector_store.search_similar(keyword_str, "patterns", top_k=limit)

        if vector_results:
            results = []
            for vr in vector_results:
                row = await self.backend.fetchone("SELECT * FROM patterns WHERE id = ?", (vr["record_id"],))
                if row:
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
            if results:
                log.info("Found %d relevant patterns via vector search", len(results))
                return results[:limit]

        results = []
        for keyword in keywords:
            rows = await self.backend.fetchall(
                '''SELECT id, pattern_type, description, code_template, success_rate, domain, reuse_count FROM patterns
                    WHERE description LIKE ? OR pattern_type LIKE ?
                    ORDER BY success_rate DESC LIMIT ?''',
                (f"%{keyword}%", f"%{keyword}%", limit)
            )
            for row in rows:
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

    async def update_pattern_usage(self, pattern_id: int, success: bool):
        try:
            await self.backend.execute(
                '''UPDATE patterns SET reuse_count = reuse_count + 1,
                    last_used = ? WHERE id = ?''',
                (datetime.now().isoformat(), pattern_id)
            )
            row = await self.backend.fetchone('''SELECT success_rate, reuse_count FROM patterns WHERE id = ?''', (pattern_id,))
            if row:
                old_rate, count = row
                adjustment = 0.05 if success else -0.05
                new_rate = max(0.0, min(1.0, old_rate + adjustment))
                await self.backend.execute('''UPDATE patterns SET success_rate = ? WHERE id = ?''', (new_rate, pattern_id))
            await self.backend.commit()
        except Exception as e:
            log.error("Error updating pattern usage: %s", e)

    async def get_recommendations(self, task_description: str, limit: int = 3) -> dict:
        similar = await self.find_similar_solution(task_description)
        keywords = [w for w in task_description.lower().split() if len(w) > 2]
        patterns = await self.find_relevant_patterns(keywords, limit)
        return {
            "similar_solution": similar,
            "relevant_patterns": patterns
        }

    async def get_trending_discoveries(self, limit: int = 10, days: int = 30) -> list:
        await self._update_trending_scores(days)
        rows = await self.backend.fetchall(
            '''SELECT discovery, relevance_score, agent_who_found, trending_score, timestamp
                 FROM discoveries ORDER BY trending_score DESC LIMIT ?''', (limit,)
        )
        return [
            {
                "discovery": row[0],
                "relevance_score": row[1],
                "from": row[2],
                "trending_score": round(row[3], 3),
                "timestamp": row[4]
            }
            for row in rows
        ]

    async def _update_trending_scores(self, decay_days: int = 30):
        now = datetime.now()
        rows = await self.backend.fetchall('''SELECT id, relevance_score, timestamp, reuse_count FROM discoveries''')
        for row in rows:
            disc_id, relevance, ts_str, reuse_count = row
            try:
                ts = datetime.fromisoformat(ts_str) if ts_str else now
            except Exception:
                ts = now
            days_old = max(0, (now - ts).total_seconds() / 86400.0)
            recency_factor = max(0.0, 1.0 - days_old / decay_days)
            reuse_boost = min(1.0, (reuse_count or 0) * 0.1)
            trending = relevance * recency_factor + reuse_boost
            await self.backend.execute('''UPDATE discoveries SET trending_score = ? WHERE id = ?''',
                                     (round(trending, 4), disc_id))
        await self.backend.commit()

    async def get_cross_domain_patterns(self, min_domains: int = 2, limit: int = 10) -> list:
        group_concat_sql = "GROUP_CONCAT(DISTINCT domain)" if not self.backend.is_postgres else "STRING_AGG(DISTINCT domain, ',')"
        rows = await self.backend.fetchall(f'''SELECT pattern_type, COUNT(DISTINCT domain) as domain_count,
                            {group_concat_sql} as domains,
                            AVG(success_rate) as avg_success,
                            SUM(reuse_count) as total_reuse
                     FROM patterns
                     GROUP BY pattern_type
                     HAVING domain_count >= ?
                     ORDER BY domain_count DESC, avg_success DESC
                     LIMIT ?''', (min_domains, limit))
        results = []
        for row in rows:
            domains_str = row[2]
            results.append({
                "pattern_type": row[0],
                "domain_count": row[1],
                "domains": domains_str.split(",") if domains_str else [],
                "avg_success_rate": round(row[3], 3) if row[3] else 0.0,
                "total_reuse": row[4] or 0
            })
        return results

    async def search_patterns(self, pattern_type: Optional[str] = None, domain: Optional[str] = None,
                        min_success_rate: float = 0.0, limit: int = 20, offset: int = 0) -> dict:
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

        total = await self.backend.fetchval(f'''SELECT COUNT(*) FROM patterns WHERE {where}''', params)

        rows = await self.backend.fetchall(
            f'''SELECT id, pattern_type, description, code_template, success_rate, domain, reuse_count, last_used FROM patterns WHERE {where}
                 ORDER BY success_rate DESC, reuse_count DESC
                 LIMIT ? OFFSET ?''', params + [limit, offset]
        )
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
            for row in rows
        ]
        return {"total": total, "results": results, "offset": offset, "limit": limit}

    async def get_reuse_rate(self) -> dict:
        total = await self.backend.fetchval('''SELECT COUNT(*) FROM experiments''')
        reused = await self.backend.fetchval('''SELECT COUNT(*) FROM experiments WHERE reused_from IS NOT NULL''')
        rate = round((reused / total * 100), 1) if total > 0 else 0.0
        return {"total_experiments": total, "reused_count": reused, "reuse_rate": rate}

    def extract_domain(self, description: str) -> str:
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

    async def store_discovery(self, discovery: str, relevance: float, agent: str):
        initial_trending = relevance
        row_id = await self.backend.execute_insert_id(
            '''INSERT INTO discoveries (discovery, relevance_score, agent_who_found, timestamp, trending_score)
                VALUES (?, ?, ?, ?, ?)''',
            (discovery, relevance, agent, datetime.now().isoformat(), initial_trending)
        )
        await self.backend.commit()
        if row_id:
            await self.vector_store.store_embedding("discoveries", row_id, discovery, {
                "agent": agent, "relevance": relevance
            })
        log.info("Stored discovery from %s (relevance=%.2f, trending=%.2f)", agent, relevance, initial_trending)

    async def get_recent_discoveries(self, limit: int = 5) -> list:
        rows = await self.backend.fetchall(
            '''SELECT discovery, agent_who_found FROM discoveries
                ORDER BY timestamp DESC LIMIT ?''', (limit,)
        )
        return [{"discovery": row[0], "from": row[1]} for row in rows]

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

    async def think(self, task: str) -> str:
        log.info("Agent %s starting task", self.name)
        self.conversation_history.append({
            "role": "user",
            "content": task
        })

        try:
            system_prompt = await self._get_system_prompt()
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1000,
                system=system_prompt,
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

    async def _get_system_prompt(self) -> str:
        return f"You are {self.name}, a {self.role} AI agent."

class ThinkerAgent(AIAgent):
    def __init__(self, memory: MemoryDB):
        super().__init__("Thinker", "planning and research", memory)

    async def _get_system_prompt(self) -> str:
        recent = await self.memory.get_recent_discoveries(3)
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
    def __init__(self, memory: MemoryDB):
        super().__init__("Coder", "implementation and coding", memory)

    async def _get_system_prompt(self) -> str:
        patterns = await self.memory.find_relevant_patterns(["code", "implementation", "python"])
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
    def __init__(self, memory: MemoryDB):
        super().__init__("Critic", "evaluation and improvement", memory)

    async def _get_system_prompt(self) -> str:
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
    def __init__(self, memory: MemoryDB):
        super().__init__("Learner", "knowledge extraction and memory", memory)

    async def _get_system_prompt(self) -> str:
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
        self.backend = DatabaseBackend(db_path)
        self.active_workers = {}
        if not self.backend.is_postgres:
            self._init_sqlite()

    def _init_sqlite(self):
        conn = self.backend.connect_sync()
        conn.execute('''CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT,
            description TEXT,
            status TEXT,
            assigned_to TEXT,
            result TEXT,
            created_at DATETIME,
            completed_at DATETIME
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS workers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id TEXT UNIQUE,
            status TEXT,
            last_heartbeat DATETIME,
            tasks_completed INTEGER
        )''')
        conn.commit()
        log.info("TaskQueue initialized at %s", self.db_path)

    async def init_db(self):
        if self.backend.is_postgres:
            dsn = os.environ.get("DATABASE_URL", self.db_path)
            self.backend.db_path = dsn
            await self.backend.connect()
            c = self.backend
            await c.execute('''CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                description TEXT,
                status TEXT,
                assigned_to TEXT,
                result TEXT,
                created_at DATETIME,
                completed_at DATETIME
            )''')
            await c.execute('''CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id TEXT UNIQUE,
                status TEXT,
                last_heartbeat DATETIME,
                tasks_completed INTEGER
            )''')
            await c.commit()
            log.info("TaskQueue initialized at %s", self.db_path)

    async def close(self):
        await self.backend.close()
        log.info("TaskQueue connection closed")

    async def register_worker(self, worker_id: str):
        try:
            await self.backend.execute(
                '''INSERT INTO workers (worker_id, status, last_heartbeat, tasks_completed)
                    VALUES (?, ?, ?, ?)''',
                (worker_id, 'online', datetime.now().isoformat(), 0)
            )
        except Exception:
            await self.backend.execute(
                '''UPDATE workers SET status = ?, last_heartbeat = ?, tasks_completed = 0 WHERE worker_id = ?''',
                ('online', datetime.now().isoformat(), worker_id)
            )
        await self.backend.commit()
        self.active_workers[worker_id] = True
        log.info("Worker '%s' registered", worker_id)

    async def enqueue_task(self, task_name: str, description: str) -> int:
        row_id = await self.backend.execute_insert_id(
            '''INSERT INTO tasks (task_name, description, status, created_at)
                VALUES (?, ?, ?, ?)''',
            (task_name, description, 'pending', datetime.now().isoformat())
        )
        await self.backend.commit()
        log.info("Task '%s' enqueued (id=%d)", task_name, row_id)
        forge_tasks_total.labels(status='created').inc()
        if broker.is_connected:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(broker.publish_task(row_id, {
                    "task_name": task_name,
                    "description": description,
                }))
            except RuntimeError:
                pass
        return row_id

    async def assign_task(self, task_id: int, worker_id: str) -> bool:
        await self.backend.execute(
            '''UPDATE tasks SET status = ?, assigned_to = ? WHERE id = ?''',
            ('assigned', worker_id, task_id)
        )
        await self.backend.commit()
        log.info("Task %d assigned to worker '%s'", task_id, worker_id)
        return True

    async def complete_task(self, task_id: int, result: str):
        await self.backend.execute(
            '''UPDATE tasks SET status = ?, result = ?, completed_at = ? WHERE id = ?''',
            ('completed', result, datetime.now().isoformat(), task_id)
        )
        await self.backend.commit()
        forge_tasks_total.labels(status='completed').inc()
        log.info("Task %d completed", task_id)

    async def get_pending_tasks(self, limit: int = 10) -> list:
        rows = await self.backend.fetchall(
            '''SELECT id, task_name, description FROM tasks WHERE status = 'pending' LIMIT ?''', (limit,)
        )
        return [{"id": row[0], "name": row[1], "description": row[2]} for row in rows]

    async def get_worker_stats(self) -> dict:
        online_workers = await self.backend.fetchval(
            'SELECT COUNT(*) FROM workers WHERE status = ?', ('online',)
        )
        completed_tasks = await self.backend.fetchval(
            'SELECT COUNT(*) FROM tasks WHERE status = ?', ('completed',)
        )
        return {
            "online_workers": online_workers,
            "completed_tasks": completed_tasks,
            "active_workers": list(self.active_workers.keys())
        }

    async def record_heartbeat(self, worker_id: str):
        now = datetime.now().isoformat()
        try:
            await self.backend.execute(
                '''INSERT INTO workers (worker_id, status, last_heartbeat, tasks_completed)
                    VALUES (?, ?, ?, 0)''',
                (worker_id, 'online', now)
            )
        except Exception:
            await self.backend.execute(
                '''UPDATE workers SET status = ?, last_heartbeat = ? WHERE worker_id = ?''',
                ('online', now, worker_id)
            )
        await self.backend.commit()
        self.active_workers[worker_id] = True
        log.info("Heartbeat recorded for worker '%s'", worker_id)

    async def mark_workers_offline(self, stale_threshold_seconds: int = 60) -> list:
        threshold = (datetime.now() - timedelta(seconds=stale_threshold_seconds)).isoformat()
        await self.backend.execute(
            '''UPDATE workers SET status = ? WHERE status = ? AND last_heartbeat < ?''',
            ('offline', 'online', threshold)
        )
        await self.backend.commit()
        workers = await self._get_recently_offline_workers(threshold)
        log.info("Marked workers offline (threshold=%ss)", stale_threshold_seconds)
        return workers

    async def _get_recently_offline_workers(self, threshold: str) -> list:
        rows = await self.backend.fetchall(
            '''SELECT worker_id FROM workers WHERE status = ? AND last_heartbeat < ?''',
            ('offline', threshold)
        )
        workers = [row[0] for row in rows]
        for w in workers:
            self.active_workers.pop(w, None)
        forge_active_workers.set(len(self.active_workers))
        return workers

    async def reassign_orphaned_tasks(self, stale_threshold_seconds: int = 60) -> int:
        threshold = (datetime.now() - timedelta(seconds=stale_threshold_seconds)).isoformat()
        result = await self.backend.execute(
            '''UPDATE tasks SET status = ?, assigned_to = NULL, completed_at = NULL
                 WHERE status = ? AND assigned_to IN (
                     SELECT worker_id FROM workers
                     WHERE status = ? AND last_heartbeat < ?
                 )''',
            ('pending', 'assigned', 'offline', threshold)
        )
        await self.backend.commit()
        if self.backend.is_postgres:
            match = re.search(r'UPDATE (\d+)', result or '')
            count = int(match.group(1)) if match else 0
        else:
            c = self.backend._conn.execute("SELECT changes()")
            count = c.fetchone()[0]
        if count > 0:
            log.info("Reassigned %d orphaned tasks back to pending", count)
        return count

    async def run_maintenance(self, stale_threshold_seconds: int = 60):
        await self.mark_workers_offline(stale_threshold_seconds)
        await self.reassign_orphaned_tasks(stale_threshold_seconds)

# ============================================================================
# RABBITMQ MESSAGE BROKER
# ============================================================================

class MessageBroker:
    """RabbitMQ message broker for distributed job distribution."""

    def __init__(self, url: Optional[str] = None):
        self.url = url or os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractRobustChannel] = None
        self.exchange = None
        self._connected = False

    async def connect(self):
        if not AIO_PIKA_AVAILABLE:
            log.warning("aio-pika not available, RabbitMQ disabled")
            return False
        try:
            self.connection = await connect_robust(self.url)
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                "forge_tasks", ExchangeType.DIRECT, durable=True
            )
            queue = await self.channel.declare_queue("forge_tasks_queue", durable=True)
            await queue.bind(self.exchange, routing_key="task")
            self._connected = True
            log.info("MessageBroker connected to RabbitMQ at %s", self.url)
            return True
        except Exception as e:
            log.warning("RabbitMQ unavailable, falling back to DB polling: %s", e)
            self._connected = False
            return False

    async def publish_task(self, task_id: int, task_data: dict):
        if not self._connected:
            return False
        try:
            message = Message(
                body=json.dumps(task_data).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=str(task_id),
            )
            await self.exchange.publish(message, routing_key="task")
            log.info("Published task %d to RabbitMQ", task_id)
            return True
        except Exception as e:
            log.error("Failed to publish task %d: %s", task_id, e)
            return False

    async def consume_tasks(self, callback):
        if not self._connected:
            return
        queue = await self.channel.declare_queue("forge_tasks_queue", durable=True)
        await queue.consume(callback)

    async def acknowledge(self, delivery_tag):
        if not self._connected:
            return
        await self.channel.basic_ack(delivery_tag)

    async def close(self):
        if self.connection:
            await self.connection.close()
            self._connected = False
            log.info("MessageBroker connection closed")

    @property
    def is_connected(self) -> bool:
        return self._connected


broker = MessageBroker()

# ============================================================================
# LIFESPAN (startup / shutdown)
# ============================================================================

_stop_event = asyncio.Event()

async def maintenance_loop(interval_seconds: int = 30, stale_threshold: int = 60):
    log.info("Maintenance loop started (interval=%ss, stale_threshold=%ss)", interval_seconds, stale_threshold)
    try:
        while not _stop_event.is_set():
            await asyncio.sleep(interval_seconds)
            await task_queue.run_maintenance(stale_threshold)
    except asyncio.CancelledError:
        log.info("Maintenance loop cancelled")
    except Exception as e:
        log.error("Maintenance loop error: %s", e)

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Forge AGI starting up")
    await broker.connect()
    await memory.init_db()
    await task_queue.init_db()
    task = asyncio.create_task(maintenance_loop())
    yield
    log.info("Forge AGI shutting down — closing database connections")
    _stop_event.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await broker.close()
    await memory.close()
    await task_queue.close()

# ============================================================================
# API SETUP
# ============================================================================

app = FastAPI(title="Forge AGI", description="Distributed AI Research Platform", lifespan=lifespan)

# Prometheus HTTP middleware
@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    forge_request_duration_seconds.labels(method=request.method, endpoint=request.url.path).observe(duration)
    return response

memory = MemoryDB()
task_queue = TaskQueue()
agents = {
    "thinker": ThinkerAgent(memory),
    "coder": CoderAgent(memory),
    "critic": CriticAgent(memory),
    "learner": LearnerAgent(memory)
}

class ResearchTask(BaseModel):
    task_name: str
    description: str

class WorkerRegistration(BaseModel):
    worker_id: str

class TaskCompletion(BaseModel):
    result: str

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "agents_loaded": len(agents),
        "database": "connected",
        "rabbitmq": "connected" if broker.is_connected else "disconnected",
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    forge_memory_usage_bytes.set(psutil.Process().memory_info().rss)
    forge_active_workers.set(len(task_queue.active_workers))
    forge_db_connections.set(1)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/stats")
async def get_api_stats():
    """JSON stats endpoint for the dashboard."""
    forge_memory_usage_bytes.set(psutil.Process().memory_info().rss)
    forge_active_workers.set(len(task_queue.active_workers))
    forge_db_connections.set(1)

    rows = await task_queue.backend.fetchall("SELECT status, COUNT(*) FROM tasks GROUP BY status")
    task_counts = {}
    for row in rows:
        task_counts[row[0]] = row[1]

    online_workers = await task_queue.backend.fetchval(
        'SELECT COUNT(*) FROM workers WHERE status = ?', ('online',)
    )

    process = psutil.Process()
    cpu_percent = process.cpu_percent(interval=0)

    return {
        "system": {
            "status": "healthy",
            "memory_mb": round(process.memory_info().rss / 1024 / 1024, 1),
            "cpu_percent": cpu_percent,
            "agents_loaded": len(agents),
        },
        "tasks": {
            "total": sum(task_counts.values()),
            "pending": task_counts.get("pending", 0),
            "assigned": task_counts.get("assigned", 0),
            "completed": task_counts.get("completed", 0),
        },
        "workers": {
            "online": online_workers,
            "active": len(task_queue.active_workers),
            "list": sorted(task_queue.active_workers.keys()),
        },
        "rabbitmq": {
            "connected": broker.is_connected,
        },
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Forge AGI Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f2f5; color: #333; }
header { background: linear-gradient(135deg, #1a1a2e, #16213e); color: white; padding: 20px 30px; display: flex; align-items: center; justify-content: space-between; }
header h1 { font-size: 24px; font-weight: 600; }
header .subtitle { font-size: 14px; opacity: 0.7; }
header .status-badge { display: inline-block; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600; }
.status-badge.online { background: #22c55e; color: white; }
.status-badge.offline { background: #ef4444; color: white; }
.container { max-width: 1400px; margin: 0 auto; padding: 20px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; }
.card { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.card .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: #6b7280; margin-bottom: 8px; }
.card .value { font-size: 28px; font-weight: 700; color: #1a1a2e; }
.card .value.green { color: #22c55e; }
.card .value.blue { color: #3b82f6; }
.card .value.purple { color: #8b5cf6; }
.card .value.orange { color: #f59e0b; }
.charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
.chart-box { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.chart-box h3 { font-size: 14px; font-weight: 600; color: #6b7280; margin-bottom: 12px; }
.workers-section { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.workers-section h3 { font-size: 14px; font-weight: 600; color: #6b7280; margin-bottom: 12px; }
table { width: 100%; border-collapse: collapse; }
th { text-align: left; padding: 8px 12px; border-bottom: 2px solid #e5e7eb; font-size: 12px; text-transform: uppercase; color: #6b7280; }
td { padding: 8px 12px; border-bottom: 1px solid #e5e7eb; font-size: 14px; }
.agent-badge { display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 12px; font-weight: 500; }
.agent-badge.thinker { background: #dbeafe; color: #1d4ed8; }
.agent-badge.coder { background: #dcfce7; color: #15803d; }
.agent-badge.critic { background: #fef3c7; color: #b45309; }
.agent-badge.learner { background: #f3e8ff; color: #7c3aed; }
.last-updated { text-align: center; color: #9ca3af; font-size: 12px; margin-top: 16px; }
@media (max-width: 768px) { .charts { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<header>
<div>
<h1>Forge AGI</h1>
<div class="subtitle">Distributed AI Research Platform</div>
</div>
<div>
<span class="status-badge online" id="statusBadge">Connected</span>
</div>
</header>
<div class="container">
<div class="cards">
<div class="card"><div class="label">System Status</div><div class="value green" id="systemStatus">Healthy</div></div>
<div class="card"><div class="label">Memory Usage</div><div class="value blue" id="memoryUsage">0 MB</div></div>
<div class="card"><div class="label">CPU</div><div class="value purple" id="cpuUsage">0%</div></div>
<div class="card"><div class="label">Agents Loaded</div><div class="value orange" id="agentsLoaded">4</div></div>
</div>
<div class="cards">
<div class="card"><div class="label">Total Tasks</div><div class="value" id="totalTasks">0</div></div>
<div class="card"><div class="label">Pending</div><div class="value blue" id="pendingTasks">0</div></div>
<div class="card"><div class="label">Completed</div><div class="value green" id="completedTasks">0</div></div>
<div class="card"><div class="label">Failed</div><div class="value" id="failedTasks">0</div></div>
</div>
<div class="cards">
<div class="card"><div class="label">Online Workers</div><div class="value green" id="onlineWorkers">0</div></div>
<div class="card"><div class="label">Active Workers</div><div class="value blue" id="activeWorkers">0</div></div>
<div class="card"><div class="label">RabbitMQ</div><div class="value" id="rabbitmqStatus">Disconnected</div></div>
<div class="card"><div class="label">DB Connections</div><div class="value" id="dbConnections">1</div></div>
</div>
<div class="charts">
<div class="chart-box"><h3>Task Distribution</h3><canvas id="taskChart"></canvas></div>
<div class="chart-box"><h3>Tasks Over Time (since app start)</h3><canvas id="taskTimeChart"></canvas></div>
</div>
<div class="workers-section">
<h3>Active Workers</h3>
<table><thead><tr><th>Worker ID</th><th>Status</th></tr></thead><tbody id="workerTable"><tr><td colspan="2">No workers registered</td></tr></tbody></table>
</div>
<div class="last-updated" id="lastUpdated">Loading...</div>
</div>
<script>
const taskCtx = document.getElementById('taskChart').getContext('2d');
const taskTimeCtx = document.getElementById('taskTimeChart').getContext('2d');
let taskChart, taskTimeChart;
const taskHistory = [];
function initCharts() {
taskChart = new Chart(taskCtx, {
type: 'doughnut',
data: { labels: ['Pending', 'Assigned', 'Completed'], datasets: [{ data: [0, 0, 0], backgroundColor: ['#3b82f6', '#f59e0b', '#22c55e'] }] },
options: { responsive: true, plugins: { legend: { position: 'bottom' } } }
});
taskTimeChart = new Chart(taskTimeCtx, {
type: 'bar',
data: { labels: [], datasets: [{ label: 'Pending', data: [], backgroundColor: '#3b82f6', borderRadius: 4 }, { label: 'Completed', data: [], backgroundColor: '#22c55e', borderRadius: 4 }] },
options: { responsive: true, scales: { y: { beginAtZero: true, stacked: true }, x: { stacked: true } }, plugins: { legend: { position: 'bottom' } } }
});
}
async function fetchStats() {
try {
const res = await fetch('/api/stats');
if (!res.ok) throw new Error('Network error');
const data = await res.json();
const sys = data.system, tasks = data.tasks, workers = data.workers, mq = data.rabbitmq;
document.getElementById('systemStatus').textContent = sys.status;
document.getElementById('memoryUsage').textContent = sys.memory_mb + ' MB';
document.getElementById('cpuUsage').textContent = sys.cpu_percent + '%';
document.getElementById('agentsLoaded').textContent = sys.agents_loaded;
document.getElementById('totalTasks').textContent = tasks.total;
document.getElementById('pendingTasks').textContent = tasks.pending;
document.getElementById('completedTasks').textContent = tasks.completed;
const failed = tasks.total - tasks.pending - tasks.assigned - tasks.completed;
document.getElementById('failedTasks').textContent = Math.max(0, failed);
document.getElementById('onlineWorkers').textContent = workers.online;
document.getElementById('activeWorkers').textContent = workers.active;
const mqEl = document.getElementById('rabbitmqStatus');
mqEl.textContent = mq.connected ? 'Connected' : 'Disconnected';
mqEl.style.color = mq.connected ? '#22c55e' : '#ef4444';
document.getElementById('dbConnections').textContent = '1';
const badge = document.getElementById('statusBadge');
badge.textContent = 'Connected';
badge.className = 'status-badge online';
taskChart.data.datasets[0].data = [tasks.pending, tasks.assigned, tasks.completed];
taskChart.update();
taskHistory.push({ pending: tasks.pending, completed: tasks.completed });
if (taskHistory.length > 20) taskHistory.shift();
taskTimeChart.data.labels = taskHistory.map((_, i) => 'T-' + (taskHistory.length - i));
taskTimeChart.data.datasets[0].data = taskHistory.map(h => h.pending);
taskTimeChart.data.datasets[1].data = taskHistory.map(h => h.completed);
taskTimeChart.update();
const tbody = document.getElementById('workerTable');
if (workers.list.length === 0) {
tbody.innerHTML = '<tr><td colspan="2">No workers registered</td></tr>';
} else {
tbody.innerHTML = workers.list.map(id => '<tr><td>' + id + '</td><td><span class="status-badge online" style="font-size:11px;padding:2px 8px;">Online</span></td></tr>').join('');
}
document.getElementById('lastUpdated').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
} catch (err) {
document.getElementById('statusBadge').textContent = 'Disconnected';
document.getElementById('statusBadge').className = 'status-badge offline';
document.getElementById('lastUpdated').textContent = 'Error: ' + err.message;
}
}
initCharts();
fetchStats();
setInterval(fetchStats, 10000);
</script>
</body>
</html>"""


@app.get("/dashboard")
async def dashboard():
    """Web-based monitoring dashboard."""
    return HTMLResponse(content=DASHBOARD_HTML)


@app.post("/research/task")
async def create_research_task(task: ResearchTask):
    log.info("API: create_research_task '%s'", task.task_name)
    task_id = await task_queue.enqueue_task(task.task_name, task.description)

    similar = await memory.find_similar_solution(task.description)
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
    log.info("API: solve_research_task task_id=%d", task_id)
    row = await task_queue.backend.fetchone('SELECT description FROM tasks WHERE id = ?', (task_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")

    description = row[0]

    recommendations = await memory.get_recommendations(description)

    async def generate():
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
            thinking = await agents["thinker"].think(description)
            yield f"event: result\ndata: {json.dumps({'stage': 'thinker', 'output': thinking[:200] + '...'})}\n\n"

            yield "event: stage\ndata: {\"agent\": \"coder\", \"message\": \"Writing solution...\"}\n\n"
            coding_prompt = f"Based on this plan: {thinking}\n\nNow write the code:"
            code = await agents["coder"].think(coding_prompt)
            yield f"event: result\ndata: {json.dumps({'stage': 'coder', 'output': code[:200] + '...'})}\n\n"

            yield "event: stage\ndata: {\"agent\": \"critic\", \"message\": \"Reviewing solution...\"}\n\n"
            criticism = await agents["critic"].think(f"Review this code:\n{code}")
            yield f"event: result\ndata: {json.dumps({'stage': 'critic', 'output': criticism[:200] + '...'})}\n\n"

            yield "event: stage\ndata: {\"agent\": \"learner\", \"message\": \"Storing knowledge...\"}\n\n"
            insights = await agents["learner"].think(f"What did we learn from this solution? Key insights:\n{code}")
            yield f"event: result\ndata: {json.dumps({'stage': 'learner', 'output': insights[:200] + '...'})}\n\n"

            domain = memory.extract_domain(description)
            reused_from = recommendations["similar_solution"]["task_name"] if recommendations["similar_solution"] else None
            await memory.store_experiment(
                task_name=f"task_{task_id}",
                description=description,
                solution=code,
                success=True,
                agent_type="multi-agent-ensemble",
                reused_from=reused_from
            )
            await memory.store_discovery(insights, 0.8, "ensemble")

            await memory.store_pattern(
                pattern_type="code_solution",
                description=f"Solution for: {description[:100]}",
                code_template=code[:500],
                success_rate=0.8,
                domain=domain
            )

            await task_queue.complete_task(task_id, code)

            yield f"event: complete\ndata: {json.dumps({'status': 'completed', 'task_id': task_id})}\n\n"

        except Exception as e:
            forge_tasks_total.labels(status='failed').inc()
            log.error("Solve pipeline failed for task %d: %s", task_id, e)
            yield f"event: error\ndata: {json.dumps({'status': 'failed', 'task_id': task_id, 'error': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/research/history")
async def get_research_history():
    log.info("API: get_research_history")
    rows = await memory.backend.fetchall(
        'SELECT task_name, description, success FROM experiments ORDER BY timestamp DESC LIMIT 20'
    )
    results = [{"task": row[0], "description": row[1], "success": bool(row[2])} for row in rows]
    return {"experiments": results}

@app.get("/research/discoveries")
async def get_discoveries():
    log.info("API: get_discoveries")
    discoveries = await memory.get_recent_discoveries(10)
    return {"discoveries": discoveries}

@app.get("/research/recommendations")
async def get_recommendations(task_description: str, limit: int = 3):
    log.info("API: get_recommendations")
    result = await memory.get_recommendations(task_description, limit)
    return result

@app.get("/discoveries/trending")
async def get_trending_discoveries(limit: int = 10, days: int = 30):
    log.info("API: get_trending_discoveries")
    return {"trending": await memory.get_trending_discoveries(limit, days)}

@app.get("/patterns/search")
async def search_patterns(
    pattern_type: Optional[str] = None,
    domain: Optional[str] = None,
    min_success_rate: float = 0.0,
    limit: int = 20,
    offset: int = 0
):
    log.info("API: search_patterns")
    return await memory.search_patterns(pattern_type, domain, min_success_rate, limit, offset)

@app.get("/patterns/cross-domain")
async def get_cross_domain_patterns(min_domains: int = 2, limit: int = 10):
    log.info("API: get_cross_domain_patterns")
    return {"cross_domain_patterns": await memory.get_cross_domain_patterns(min_domains, limit)}

@app.get("/research/reuse-rate")
async def get_reuse_rate():
    log.info("API: get_reuse_rate")
    return await memory.get_reuse_rate()

@app.post("/workers/register")
async def register_worker(worker: WorkerRegistration):
    log.info("API: register_worker '%s'", worker.worker_id)
    await task_queue.register_worker(worker.worker_id)
    return {
        "worker_id": worker.worker_id,
        "status": "registered",
        "pending_tasks": await task_queue.get_pending_tasks(5)
    }

@app.post("/workers/heartbeat")
async def worker_heartbeat(worker: WorkerRegistration):
    log.info("API: heartbeat from worker '%s'", worker.worker_id)
    await task_queue.record_heartbeat(worker.worker_id)
    return {"worker_id": worker.worker_id, "status": "online", "timestamp": datetime.now().isoformat()}

@app.get("/workers/stats")
async def get_worker_stats():
    return await task_queue.get_worker_stats()

@app.get("/tasks/pending")
async def get_pending_tasks():
    return {"tasks": await task_queue.get_pending_tasks()}

@app.post("/tasks/{task_id}/complete")
async def complete_task(task_id: int, completion: TaskCompletion):
    log.info("API: complete_task task_id=%d", task_id)
    await task_queue.complete_task(task_id, completion.result)
    return {"status": "completed", "task_id": task_id}

@app.get("/")
async def root():
    return {
        "system": "Forge AGI",
        "version": "0.3.0",
        "features": [
            "Multi-Agent Autonomous Collaboration",
            "Persistent Learning Memory with Vector Search",
            "Distributed Task Orchestration",
            "PostgreSQL Database Support",
            "Knowledge Sharing & Discovery Trending"
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
            }
        }
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
