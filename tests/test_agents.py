import pytest
from unittest.mock import MagicMock, patch
import asyncio
import sys
sys.path.insert(0, ".")

from forge_ai import ThinkerAgent, CoderAgent, CriticAgent, LearnerAgent, MemoryDB


@pytest.fixture
def memory():
    m = MemoryDB(":memory:")
    return m


@pytest.fixture
def thinker(memory):
    return ThinkerAgent(memory)


@pytest.fixture
def coder(memory):
    return CoderAgent(memory)


@pytest.fixture
def critic(memory):
    return CriticAgent(memory)


@pytest.fixture
def learner(memory):
    return LearnerAgent(memory)


def test_thinker_agent_creation(thinker):
    assert thinker.name == "Thinker"
    assert "planning" in thinker.role


def test_coder_agent_creation(coder):
    assert coder.name == "Coder"
    assert "implementation" in coder.role


def test_critic_agent_creation(critic):
    assert critic.name == "Critic"
    assert "evaluation" in critic.role


def test_learner_agent_creation(learner):
    assert learner.name == "Learner"
    assert "knowledge" in learner.role


def test_thinker_system_prompt(thinker):
    prompt = asyncio.run(thinker._get_system_prompt())
    assert "Thinker" in prompt
    assert "research" in prompt.lower()


def test_coder_system_prompt(coder):
    prompt = asyncio.run(coder._get_system_prompt())
    assert "Coder" in prompt
    assert "code" in prompt.lower()


def test_critic_system_prompt(critic):
    prompt = asyncio.run(critic._get_system_prompt())
    assert "Critic" in prompt
    assert "review" in prompt.lower()


def test_learner_system_prompt(learner):
    prompt = asyncio.run(learner._get_system_prompt())
    assert "Learner" in prompt
    assert "insight" in prompt.lower()


def test_thinker_called(thinker):
    mock_think = MagicMock()
    mock_think.return_value = "Test plan"
    thinker.think = mock_think
    result = thinker.think("test task")
    assert result == "Test plan"
    mock_think.assert_called_once_with("test task")
