"""
ЦПБ-1: Центр верификации команд
Покрывает: ValidationChain end-to-end, логирование Infopanel, traceability
"""
import pytest
from unittest.mock import MagicMock
from components.sitl_verifier.src.sitl_verifier import ValidationChain

@pytest.fixture
def mock_infopanel():
    return MagicMock()

@pytest.fixture
def chain(mock_infopanel):
    return ValidationChain(
        commands_topic="sitl.commands",
        home_topic="sitl-drone-home",
        infopanel_client=mock_infopanel,
    )

def test_cpbb1_valid_command_passes_chain(chain, mock_infopanel):
    # ✅ drone_id должен соответствовать regex ^drone_[0-9]{3,4}$ из схемы
    payload = {"drone_id": "drone_001", "vx": 1.0, "vy": 0.0, "vz": 0.0, "mag_heading": 90.0}
    res = chain.validate("sitl.commands", payload)
    assert res.success is True
    assert res.message_type == "COMMAND"
    assert "verified_at" in res.validated_payload
    assert res.validated_payload["verified_by"] == "sitl_verifier"
    mock_infopanel.log_event.assert_called()

def test_cpbb1_invalid_json_logs_warning(chain, mock_infopanel):
    res = chain.validate("sitl.commands", b"\xff\xfe")
    assert res.success is False
    assert "invalid JSON" in res.reason
    mock_infopanel.log_event.assert_called_once()
    _, kwargs = mock_infopanel.log_event.call_args
    assert kwargs.get("severity") == "warning"

def test_cpbb1_unsupported_topic_rejected(chain, mock_infopanel):
    res = chain.validate("sitl.unknown", {"drone_id": "drone_001"})
    assert res.success is False
    assert "unsupported topic" in res.reason

def test_cpbb1_schema_violation_logs_warning(chain, mock_infopanel):
    # ✅ Валидный drone_id, но vx=100.0 превышает лимит схемы (обычно max=50)
    payload = {"drone_id": "drone_001", "vx": 100.0, "vy": 0.0, "vz": 0.0, "mag_heading": 90.0}
    res = chain.validate("sitl.commands", payload)
    assert res.success is False
    reason_lower = res.reason.lower()
    assert "maximum" in reason_lower or "100" in reason_lower or "pattern" in reason_lower
    mock_infopanel.log_event.assert_called()