import pytest
import json
import sys
sys.path.insert(0, ".")

from forge_ai import MemoryDB, PipelineExecutor, app
from forge_ai import ThinkerAgent, CoderAgent, CriticAgent, LearnerAgent
from fastapi.testclient import TestClient


@pytest.fixture
def memory():
    return MemoryDB(":memory:")


@pytest.fixture
def agents(memory):
    return {
        "thinker": ThinkerAgent(memory),
        "coder": CoderAgent(memory),
        "critic": CriticAgent(memory),
        "learner": LearnerAgent(memory),
    }


@pytest.fixture
def executor(memory, agents):
    return PipelineExecutor(memory, agents, None)


@pytest.fixture
def client():
    return TestClient(app)


def _valid_definition():
    return {
        "tasks": {
            "analyze": {
                "description": "Analyze the problem",
                "agent": "thinker",
                "depends_on": [],
            },
            "implement": {
                "description": "Implement the solution",
                "agent": "coder",
                "depends_on": ["analyze"],
            },
        }
    }


def test_create_pipeline(executor):
    import asyncio

    pid = asyncio.run(
        executor.create_pipeline("test-pipe", "A test pipeline", _valid_definition())
    )
    assert pid > 0


def test_get_pipeline(executor):
    import asyncio

    pid = asyncio.run(
        executor.create_pipeline("get-test", "Get test", _valid_definition())
    )
    pipeline = asyncio.run(executor.get_pipeline(pid))
    assert pipeline is not None
    assert pipeline["name"] == "get-test"
    assert "tasks" in pipeline["definition"]


def test_get_pipeline_not_found(executor):
    import asyncio

    pipeline = asyncio.run(executor.get_pipeline(9999))
    assert pipeline is None


def test_list_pipelines(executor):
    import asyncio

    asyncio.run(
        executor.create_pipeline("pipe-1", "First", _valid_definition())
    )
    asyncio.run(
        executor.create_pipeline("pipe-2", "Second", _valid_definition())
    )
    pipelines = asyncio.run(executor.list_pipelines())
    assert len(pipelines) >= 2
    names = [p["name"] for p in pipelines]
    assert "pipe-1" in names
    assert "pipe-2" in names


def test_delete_pipeline(executor):
    import asyncio

    pid = asyncio.run(
        executor.create_pipeline("del-test", "To delete", _valid_definition())
    )
    asyncio.run(executor.delete_pipeline(pid))
    pipeline = asyncio.run(executor.get_pipeline(pid))
    assert pipeline is None


def test_validate_definition_missing_tasks(executor):
    with pytest.raises(ValueError, match="must contain a 'tasks' object"):
        executor._validate_definition({})


def test_validate_definition_empty_tasks(executor):
    with pytest.raises(ValueError, match="at least one task"):
        executor._validate_definition({"tasks": {}})


def test_validate_definition_missing_description(executor):
    with pytest.raises(ValueError, match="missing required field"):
        executor._validate_definition({"tasks": {"t1": {"agent": "thinker"}}})


def test_validate_definition_unknown_agent(executor):
    with pytest.raises(ValueError, match="unknown agent"):
        executor._validate_definition(
            {"tasks": {"t1": {"description": "test", "agent": "unknown"}}}
        )


def test_validate_definition_unknown_dependency(executor):
    with pytest.raises(ValueError, match="depends on unknown task"):
        executor._validate_definition(
            {
                "tasks": {
                    "t1": {
                        "description": "test",
                        "agent": "thinker",
                        "depends_on": ["nonexistent"],
                    }
                }
            }
        )


def test_validate_definition_cycle(executor):
    with pytest.raises(ValueError, match="Cycle detected"):
        executor._validate_definition(
            {
                "tasks": {
                    "a": {
                        "description": "task a",
                        "agent": "thinker",
                        "depends_on": ["b"],
                    },
                    "b": {
                        "description": "task b",
                        "agent": "coder",
                        "depends_on": ["a"],
                    },
                }
            }
        )


def test_topological_sort(executor):
    tasks = {
        "a": {"description": "a", "agent": "thinker", "depends_on": []},
        "b": {"description": "b", "agent": "coder", "depends_on": ["a"]},
        "c": {"description": "c", "agent": "critic", "depends_on": ["a"]},
        "d": {"description": "d", "agent": "learner", "depends_on": ["b", "c"]},
    }
    ordered = executor._topological_sort(tasks)
    assert ordered.index("a") < ordered.index("b")
    assert ordered.index("a") < ordered.index("c")
    assert ordered.index("b") < ordered.index("d")
    assert ordered.index("c") < ordered.index("d")
    assert len(ordered) == 4


def test_run_and_get_run(executor):
    import asyncio

    pid = asyncio.run(
        executor.create_pipeline("run-test", "Run test", _valid_definition())
    )
    run_id = asyncio.run(executor.run_pipeline(pid))
    assert run_id > 0

    import time
    time.sleep(0.5)

    run = asyncio.run(executor.get_run(run_id))
    assert run is not None
    assert run["pipeline_id"] == pid
    assert run["status"] in ("running", "completed", "failed")
    assert "tasks" in run


def test_list_runs(executor):
    import asyncio

    pid = asyncio.run(
        executor.create_pipeline("list-runs", "List runs", _valid_definition())
    )
    asyncio.run(executor.run_pipeline(pid))
    asyncio.run(executor.run_pipeline(pid))
    runs = asyncio.run(executor.list_runs(limit=10))
    assert len(runs) >= 2


def test_run_not_found(executor):
    import asyncio

    run = asyncio.run(executor.get_run(9999))
    assert run is None


def test_create_pipeline_via_api(client):
    response = client.post("/pipelines", json={
        "name": "api-pipeline",
        "description": "Created via API",
        "definition": _valid_definition()
    })
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "created"
    assert data["name"] == "api-pipeline"
    assert data["pipeline_id"] > 0


def test_list_pipelines_via_api(client):
    client.post("/pipelines", json={
        "name": "api-list-1", "description": "First",
        "definition": _valid_definition()
    })
    response = client.get("/pipelines")
    assert response.status_code == 200
    data = response.json()
    assert "pipelines" in data
    assert any(p["name"] == "api-list-1" for p in data["pipelines"])


def test_get_pipeline_via_api(client):
    resp = client.post("/pipelines", json={
        "name": "api-get", "description": "Get me",
        "definition": _valid_definition()
    })
    pid = resp.json()["pipeline_id"]
    response = client.get(f"/pipelines/{pid}")
    assert response.status_code == 200
    assert response.json()["name"] == "api-get"
    assert "tasks" in response.json()["definition"]


def test_get_pipeline_not_found_via_api(client):
    response = client.get("/pipelines/9999")
    assert response.status_code == 404


def test_delete_pipeline_via_api(client):
    resp = client.post("/pipelines", json={
        "name": "api-delete", "description": "Delete me",
        "definition": _valid_definition()
    })
    pid = resp.json()["pipeline_id"]
    response = client.delete(f"/pipelines/{pid}")
    assert response.status_code == 200
    get_resp = client.get(f"/pipelines/{pid}")
    assert get_resp.status_code == 404


def test_run_pipeline_via_api(client):
    resp = client.post("/pipelines", json={
        "name": "api-run", "description": "Run me",
        "definition": _valid_definition()
    })
    pid = resp.json()["pipeline_id"]
    response = client.post(f"/pipelines/{pid}/run")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["run_id"] > 0


def test_run_pipeline_not_found_via_api(client):
    response = client.post("/pipelines/9999/run")
    assert response.status_code == 404


def test_list_runs_via_api(client):
    resp = client.post("/pipelines", json={
        "name": "api-runs-list", "description": "Runs list",
        "definition": _valid_definition()
    })
    pid = resp.json()["pipeline_id"]
    client.post(f"/pipelines/{pid}/run")
    response = client.get("/pipeline-runs")
    assert response.status_code == 200
    data = response.json()
    assert "runs" in data
    assert len(data["runs"]) >= 1


def test_get_run_via_api(client):
    resp = client.post("/pipelines", json={
        "name": "api-get-run", "description": "Get run",
        "definition": _valid_definition()
    })
    pid = resp.json()["pipeline_id"]
    run_resp = client.post(f"/pipelines/{pid}/run")
    run_id = run_resp.json()["run_id"]
    import time
    time.sleep(0.5)
    response = client.get(f"/pipeline-runs/{run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["pipeline_id"] == pid
    assert "tasks" in data


def test_get_run_not_found_via_api(client):
    response = client.get("/pipeline-runs/9999")
    assert response.status_code == 404


def test_stream_run_via_api(client):
    resp = client.post("/pipelines", json={
        "name": "api-stream", "description": "Stream test",
        "definition": _valid_definition()
    })
    pid = resp.json()["pipeline_id"]
    run_resp = client.post(f"/pipelines/{pid}/run")
    run_id = run_resp.json()["run_id"]
    import time
    time.sleep(0.5)
    response = client.get(f"/pipeline-runs/{run_id}/stream")
    assert response.status_code == 200
    assert "text/event-stream" in response.headers.get("content-type", "")


def test_invalid_definition_via_api(client):
    response = client.post("/pipelines", json={
        "name": "bad-pipe", "description": "Bad",
        "definition": {"tasks": {}}
    })
    assert response.status_code == 400
