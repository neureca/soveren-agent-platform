from pathlib import Path

from agent_platform.llm.contracts import LlmRequest, LlmResponse


def test_llm_contracts_are_backend_neutral():
    request = LlmRequest(
        prompt="hello",
        system_prompt="system",
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
        model="test-model",
        metadata={"app": "test"},
    )
    response = LlmResponse(text="{}", session_id="session-1")

    assert request.resume is False
    assert request.metadata == {"app": "test"}
    assert response.cost_usd == 0.0

