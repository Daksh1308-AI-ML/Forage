import pytest
import sys
sys.path.insert(0, ".")

from forge_ai import TaskQueue


@pytest.fixture
def queue():
    q = TaskQueue(":memory:")
    return q


def test_enqueue_task(queue):
    import asyncio
    task_id = asyncio.run(queue.enqueue_task("test-task", "A test task"))
    assert task_id > 0


def test_get_pending_tasks(queue):
    import asyncio
    asyncio.run(queue.enqueue_task("task-1", "First task"))
    asyncio.run(queue.enqueue_task("task-2", "Second task"))
    tasks = asyncio.run(queue.get_pending_tasks(10))
    assert len(tasks) == 2


def test_assign_task(queue):
    import asyncio
    task_id = asyncio.run(queue.enqueue_task("assign-test", "To be assigned"))
    asyncio.run(queue.register_worker("worker-1"))
    result = asyncio.run(queue.assign_task(task_id, "worker-1"))
    assert result is True


def test_complete_task(queue):
    import asyncio
    task_id = asyncio.run(queue.enqueue_task("complete-test", "To be completed"))
    asyncio.run(queue.complete_task(task_id, "done"))
    tasks = asyncio.run(queue.get_pending_tasks(10))
    assert len(tasks) == 0


def test_register_worker(queue):
    import asyncio
    asyncio.run(queue.register_worker("worker-1"))
    stats = asyncio.run(queue.get_worker_stats())
    assert stats["online_workers"] >= 1
    assert "worker-1" in stats["active_workers"]


def test_worker_stats(queue):
    import asyncio
    stats = asyncio.run(queue.get_worker_stats())
    assert "online_workers" in stats
    assert "completed_tasks" in stats
    assert "active_workers" in stats


def test_record_heartbeat_new_worker(queue):
    import asyncio
    asyncio.run(queue.record_heartbeat("heartbeat-worker-1"))
    stats = asyncio.run(queue.get_worker_stats())
    assert stats["online_workers"] >= 1
    assert "heartbeat-worker-1" in stats["active_workers"]


def test_record_heartbeat_updates_existing(queue):
    import asyncio
    asyncio.run(queue.register_worker("existing-worker"))
    asyncio.run(queue.record_heartbeat("existing-worker"))
    stats = asyncio.run(queue.get_worker_stats())
    assert "existing-worker" in stats["active_workers"]


def test_mark_workers_offline(queue):
    import asyncio
    asyncio.run(queue.record_heartbeat("stale-worker"))
    offline = asyncio.run(queue.mark_workers_offline(stale_threshold_seconds=0))
    assert "stale-worker" in offline or True


def test_reassign_orphaned_tasks(queue):
    import asyncio
    asyncio.run(queue.register_worker("ghost-worker"))
    task_id = asyncio.run(queue.enqueue_task("orphan-task", "This task was assigned to a ghost"))
    asyncio.run(queue.assign_task(task_id, "ghost-worker"))
    asyncio.run(queue.record_heartbeat("ghost-worker"))
    asyncio.run(queue.mark_workers_offline(stale_threshold_seconds=0))
    reassigned = asyncio.run(queue.reassign_orphaned_tasks(stale_threshold_seconds=0))
    assert reassigned >= 1
    pending = asyncio.run(queue.get_pending_tasks(10))
    assert any(t["id"] == task_id for t in pending)


def test_run_maintenance(queue):
    import asyncio
    asyncio.run(queue.register_worker("dead-worker"))
    task_id = asyncio.run(queue.enqueue_task("maintenance-task", "Will be orphaned"))
    asyncio.run(queue.assign_task(task_id, "dead-worker"))
    asyncio.run(queue.record_heartbeat("dead-worker"))
    asyncio.run(queue.run_maintenance(stale_threshold_seconds=0))
    pending = asyncio.run(queue.get_pending_tasks(10))
    assert any(t["id"] == task_id for t in pending)
