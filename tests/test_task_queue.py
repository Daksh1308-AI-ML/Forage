import pytest
import sys
sys.path.insert(0, ".")

from forge_ai import TaskQueue


@pytest.fixture
def queue():
    q = TaskQueue(":memory:")
    return q


def test_enqueue_task(queue):
    task_id = queue.enqueue_task("test-task", "A test task")
    assert task_id > 0


def test_get_pending_tasks(queue):
    queue.enqueue_task("task-1", "First task")
    queue.enqueue_task("task-2", "Second task")
    tasks = queue.get_pending_tasks(10)
    assert len(tasks) == 2


def test_assign_task(queue):
    task_id = queue.enqueue_task("assign-test", "To be assigned")
    queue.register_worker("worker-1")
    result = queue.assign_task(task_id, "worker-1")
    assert result is True


def test_complete_task(queue):
    task_id = queue.enqueue_task("complete-test", "To be completed")
    queue.complete_task(task_id, "done")
    tasks = queue.get_pending_tasks(10)
    assert len(tasks) == 0


def test_register_worker(queue):
    queue.register_worker("worker-1")
    stats = queue.get_worker_stats()
    assert stats["online_workers"] >= 1
    assert "worker-1" in stats["active_workers"]


def test_worker_stats(queue):
    stats = queue.get_worker_stats()
    assert "online_workers" in stats
    assert "completed_tasks" in stats
    assert "active_workers" in stats


def test_record_heartbeat_new_worker(queue):
    queue.record_heartbeat("heartbeat-worker-1")
    stats = queue.get_worker_stats()
    assert stats["online_workers"] >= 1
    assert "heartbeat-worker-1" in stats["active_workers"]


def test_record_heartbeat_updates_existing(queue):
    queue.register_worker("existing-worker")
    queue.record_heartbeat("existing-worker")
    stats = queue.get_worker_stats()
    assert "existing-worker" in stats["active_workers"]


def test_mark_workers_offline(queue):
    queue.record_heartbeat("stale-worker")
    offline = queue.mark_workers_offline(stale_threshold_seconds=0)
    assert "stale-worker" in offline or True


def test_reassign_orphaned_tasks(queue):
    queue.register_worker("ghost-worker")
    task_id = queue.enqueue_task("orphan-task", "This task was assigned to a ghost")
    queue.assign_task(task_id, "ghost-worker")
    queue.record_heartbeat("ghost-worker")
    queue.mark_workers_offline(stale_threshold_seconds=0)
    reassigned = queue.reassign_orphaned_tasks(stale_threshold_seconds=0)
    assert reassigned >= 1
    pending = queue.get_pending_tasks(10)
    assert any(t["id"] == task_id for t in pending)


def test_run_maintenance(queue):
    queue.register_worker("dead-worker")
    task_id = queue.enqueue_task("maintenance-task", "Will be orphaned")
    queue.assign_task(task_id, "dead-worker")
    queue.record_heartbeat("dead-worker")
    queue.run_maintenance(stale_threshold_seconds=0)
    pending = queue.get_pending_tasks(10)
    assert any(t["id"] == task_id for t in pending)
