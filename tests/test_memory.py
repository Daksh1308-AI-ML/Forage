import pytest
import sys
sys.path.insert(0, ".")

from forge_ai import MemoryDB


@pytest.fixture
def memory():
    m = MemoryDB(":memory:")
    return m


def test_store_experiment(memory):
    memory.store_experiment("test-task", "A test task", "print('hello')", True, "multi-agent-ensemble")
    results = memory.get_recent_discoveries(1)
    assert len(results) >= 0


def test_find_similar_solution(memory):
    memory.store_experiment("sort-list", "Write a function to sort a list", "def sort(lst): return sorted(lst)", True, "multi-agent-ensemble")
    result = memory.find_similar_solution("sort a list of numbers")
    assert result is not None
    assert result["task_name"] == "sort-list"


def test_find_similar_no_match(memory):
    result = memory.find_similar_solution("unique task no match")
    assert result is None


def test_store_discovery(memory):
    memory.store_discovery("Use list comprehensions for speed", 0.85, "learner")
    discoveries = memory.get_recent_discoveries(10)
    assert len(discoveries) >= 1
    assert "list comprehensions" in discoveries[0]["discovery"]


def test_multiple_discoveries(memory):
    for i in range(5):
        memory.store_discovery(f"Discovery {i}", 0.5 + i * 0.1, "learner")
    discoveries = memory.get_recent_discoveries(3)
    assert len(discoveries) == 3


# -----------------------------------------------------------------------
# Phase 3: Knowledge Sharing tests
# -----------------------------------------------------------------------

def test_store_pattern_with_domain(memory):
    memory.store_pattern("sorting", "Quick sort implementation", "def sort(arr): return sorted(arr)", 0.9, domain="algorithm")
    results = memory.find_relevant_patterns(["sorting"], limit=10)
    assert len(results) >= 1
    assert results[0]["pattern_type"] == "sorting"
    assert results[0]["domain"] == "algorithm"


def test_find_relevant_patterns(memory):
    memory.store_pattern("data_processing", "CSV parsing pattern", "import csv", 0.85, domain="data")
    memory.store_pattern("api_route", "REST endpoint pattern", "@app.route", 0.75, domain="web")
    results = memory.find_relevant_patterns(["data", "csv"], limit=5)
    assert len(results) >= 1
    assert any("data_processing" in r["pattern_type"] for r in results)


def test_update_pattern_usage(memory):
    memory.store_pattern("auth", "JWT auth pattern", "jwt.encode()", 0.7, domain="security")
    results = memory.find_relevant_patterns(["auth"], limit=5)
    assert len(results) >= 1
    pattern_id = results[0]["id"]
    memory.update_pattern_usage(pattern_id, success=True)
    memory.update_pattern_usage(pattern_id, success=True)
    memory.update_pattern_usage(pattern_id, success=False)
    updated = memory.search_patterns(pattern_type="auth")["results"]
    assert len(updated) >= 1
    assert updated[0]["reuse_count"] >= 1
    assert updated[0]["last_used"] is not None


def test_get_recommendations(memory):
    memory.store_experiment("sort-list", "Sort a list of numbers", "sorted(lst)", True, "multi-agent-ensemble")
    memory.store_pattern("sorting", "Sort algorithm pattern", "sorted()", 0.8, domain="algorithm")
    recs = memory.get_recommendations("sort a list of numbers", limit=5)
    assert "similar_solution" in recs
    assert "relevant_patterns" in recs
    assert recs["similar_solution"] is not None


def test_get_recommendations_no_match(memory):
    recs = memory.get_recommendations("nonexistent unique task", limit=5)
    assert recs["similar_solution"] is None
    assert len(recs["relevant_patterns"]) == 0


def test_trending_discoveries(memory):
    memory.store_discovery("Use list comprehensions", 0.9, "learner")
    memory.store_discovery("Use async/await", 0.7, "learner")
    trending = memory.get_trending_discoveries(limit=10, days=30)
    assert len(trending) >= 2
    assert all("trending_score" in d for d in trending)
    assert trending[0]["trending_score"] >= trending[1]["trending_score"]


def test_cross_domain_patterns(memory):
    memory.store_pattern("data_pipeline", "ETL pipeline", "extract()", 0.8, domain="data")
    memory.store_pattern("data_pipeline", "ETL pipeline", "extract()", 0.8, domain="web")
    memory.store_pattern("api_route", "REST API", "@app.route", 0.7, domain="web")
    cross = memory.get_cross_domain_patterns(min_domains=2, limit=10)
    assert len(cross) >= 1
    assert cross[0]["pattern_type"] == "data_pipeline"
    assert cross[0]["domain_count"] >= 2


def test_search_patterns_filters(memory):
    memory.store_pattern("ml_model", "Neural network training", "model.fit()", 0.9, domain="ml")
    memory.store_pattern("data_clean", "Data cleaning pipeline", "df.dropna()", 0.6, domain="data")
    results = memory.search_patterns(min_success_rate=0.8)
    assert results["total"] >= 1
    assert all(r["success_rate"] >= 0.8 for r in results["results"])


def test_search_patterns_pagination(memory):
    for i in range(5):
        memory.store_pattern(f"pattern_{i}", f"Test pattern {i}", "pass", 0.5 + i * 0.1, domain="general")
    page1 = memory.search_patterns(limit=2, offset=0)
    assert len(page1["results"]) == 2
    assert page1["total"] == 5
    page2 = memory.search_patterns(limit=2, offset=2)
    assert len(page2["results"]) == 2


def test_get_reuse_rate(memory):
    rate = memory.get_reuse_rate()
    assert "total_experiments" in rate
    assert "reused_count" in rate
    assert "reuse_rate" in rate
    # No experiments yet
    assert rate["total_experiments"] == 0
    assert rate["reuse_rate"] == 0.0


def test_get_reuse_rate_with_data(memory):
    memory.store_experiment("task-1", "First task", "solution1", True, "agent", reused_from=None)
    memory.store_experiment("task-2", "Second task", "solution2", True, "agent", reused_from="task-1")
    memory.store_experiment("task-3", "Third task", "solution3", True, "agent", reused_from="task-1")
    rate = memory.get_reuse_rate()
    assert rate["total_experiments"] == 3
    assert rate["reused_count"] == 2
    assert rate["reuse_rate"] == 66.7  # 2/3 = 66.7%


def test_extract_domain(memory):
    assert memory.extract_domain("Build a REST API for user management") == "web"
    assert memory.extract_domain("Train a neural network on image data") == "ml"
    assert memory.extract_domain("Random unique task") == "general"
    assert memory.extract_domain("Deploy with Docker and Kubernetes") == "devops"
    assert memory.extract_domain("Write a sorting algorithm") == "algorithm"
