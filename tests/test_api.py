import pytest
from fastapi.testclient import TestClient
import sys
sys.path.insert(0, ".")

from forge_ai import app


@pytest.fixture
def client():
    return TestClient(app)


def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "system" in data
    assert "Forge" in data["system"]


def test_create_task(client):
    response = client.post("/research/task", json={
        "task_name": "test-task-1",
        "description": "Write hello world in Python"
    })
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert data["status"] == "created"


def test_create_task_missing_name(client):
    response = client.post("/research/task", json={
        "description": "No name provided"
    })
    assert response.status_code == 422


def test_get_history_empty(client):
    response = client.get("/research/history")
    assert response.status_code == 200
    data = response.json()
    assert "experiments" in data


def test_get_discoveries(client):
    response = client.get("/research/discoveries")
    assert response.status_code == 200
    data = response.json()
    assert "discoveries" in data


def test_get_pending_tasks(client):
    response = client.get("/tasks/pending")
    assert response.status_code == 200
    data = response.json()
    assert "tasks" in data


def test_register_worker(client):
    response = client.post("/workers/register", json={
        "worker_id": "test-worker-1"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "registered"


def test_worker_stats(client):
    response = client.get("/workers/stats")
    assert response.status_code == 200
    data = response.json()
    assert "online_workers" in data
    assert "completed_tasks" in data


def test_worker_heartbeat(client):
    client.post("/workers/register", json={"worker_id": "hb-worker"})
    response = client.post("/workers/heartbeat", json={"worker_id": "hb-worker"})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "online"
    assert data["worker_id"] == "hb-worker"


# -----------------------------------------------------------------------
# Phase 3: Knowledge Sharing API tests
# -----------------------------------------------------------------------

def test_get_recommendations(client):
    # Create a task first to have some data
    client.post("/research/task", json={
        "task_name": "test-sort",
        "description": "Write a function to sort a list of numbers"
    })
    response = client.get("/research/recommendations",
                          params={"task_description": "sort a list", "limit": 3})
    assert response.status_code == 200
    data = response.json()
    assert "similar_solution" in data
    assert "relevant_patterns" in data


def test_get_recommendations_no_params(client):
    response = client.get("/research/recommendations",
                          params={"task_description": "nonexistent unique task"})
    assert response.status_code == 200
    data = response.json()
    assert data["similar_solution"] is None


def test_get_trending_discoveries(client):
    response = client.get("/discoveries/trending",
                          params={"limit": 5, "days": 30})
    assert response.status_code == 200
    data = response.json()
    assert "trending" in data
    assert isinstance(data["trending"], list)


def test_search_patterns_default(client):
    response = client.get("/patterns/search")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "results" in data
    assert "offset" in data
    assert "limit" in data


def test_search_patterns_with_filters(client):
    response = client.get("/patterns/search",
                          params={"pattern_type": "code", "min_success_rate": 0.5, "limit": 5})
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data["results"], list)


def test_get_cross_domain_patterns(client):
    response = client.get("/patterns/cross-domain",
                          params={"min_domains": 2, "limit": 10})
    assert response.status_code == 200
    data = response.json()
    assert "cross_domain_patterns" in data


def test_get_reuse_rate(client):
    response = client.get("/research/reuse-rate")
    assert response.status_code == 200
    data = response.json()
    assert "total_experiments" in data
    assert "reused_count" in data
    assert "reuse_rate" in data
