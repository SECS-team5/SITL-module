import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from typing import Iterable
from typing import Tuple
from uuid import uuid4

from jsonschema import Draft202012Validator

COMMAND_SCHEMA_NAME = "sitl-commands.json"
HOME_SCHEMA_NAME = "sitl-drone-home.json"
POSITION_REQUEST_SCHEMA_NAME = "sitl-position-request.json"
POSITION_RESPONSE_SCHEMA_NAME = "sitl-position-response.json"
VERIFIER_STAGE = "SITL-v1"
SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas"
VERIFIED_COMMAND_TOPIC_DEFAULT = "sitl.verified-commands"
VERIFIED_HOME_TOPIC_DEFAULT = "sitl.verified-home"
POSITION_REQUEST_TOPIC_DEFAULT = "sitl.telemetry.request"
POSITION_RESPONSE_TOPIC_DEFAULT = "sitl.telemetry.response"


def parse_json_payload(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw

    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode()
        except UnicodeDecodeError:
            return None

    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    return None


@lru_cache(maxsize=None)
def load_schema(schema_name: str) -> dict[str, Any]:
    schema_path = SCHEMAS_DIR / schema_name
    with schema_path.open("r", encoding="utf-8") as schema_file:
        return json.load(schema_file)


@lru_cache(maxsize=None)
def get_validator(schema_name: str) -> Draft202012Validator:
    return Draft202012Validator(load_schema(schema_name))


def format_validation_error(schema_name: str, errors: Iterable[Any]) -> str:
    first_error = sorted(errors, key=lambda item: list(item.absolute_path))[0]
    path = ".".join(str(part) for part in first_error.absolute_path) or "$"
    return f"{schema_name} validation failed at {path}: {first_error.message}"


def validate_schema(
    payload: dict[str, Any],
    schema_name: str,
) -> Tuple[bool, str]:
    errors = list(get_validator(schema_name).iter_errors(payload))
    if errors:
        return False, format_validation_error(schema_name, errors)
    return True, ""


def classify_input_topic(
    topic: str,
    commands_topic: str,
    home_topic: str,
) -> Tuple[bool, str, str]:
    if topic == commands_topic:
        return True, "COMMAND", COMMAND_SCHEMA_NAME
    if topic == home_topic:
        return True, "HOME", HOME_SCHEMA_NAME
    return False, "", f"unsupported topic '{topic}'"


def build_verified_message(
    topic: str,
    message_type: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "data": dict(payload),
        "input_topic": topic,
        "message_type": message_type,
        "verifier_stage": VERIFIER_STAGE,
        "verified_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def resolve_verified_topic(
    message_type: str,
    verified_commands_topic: str,
    verified_home_topic: str,
) -> str:
    if message_type == "COMMAND":
        return verified_commands_topic
    if message_type == "HOME":
        return verified_home_topic
    raise ValueError(f"unsupported message_type '{message_type}'")


def decode_headers(headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for key, value in headers or []:
        decoded[key] = value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
    return decoded


def get_transport_value(
    payload: dict[str, Any],
    headers: dict[str, str],
    field: str,
) -> str:
    header_value = headers.get(field, "").strip()
    if header_value:
        return header_value

    payload_value = payload.get(field)
    if payload_value is None:
        return ""
    return str(payload_value).strip()


def generate_correlation_id() -> str:
    return uuid4().hex


def build_request_headers(
    correlation_id: str,
    reply_to: str,
) -> list[tuple[str, bytes]]:
    return [
        ("correlation_id", correlation_id.encode()),
        ("reply_to", reply_to.encode()),
    ]


def is_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False

    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def validate_verified_message(
    message: dict[str, Any],
    commands_topic: str,
    home_topic: str,
) -> Tuple[bool, str]:
    required_fields = (
        "data",
        "input_topic",
        "message_type",
        "verified_at",
        "verifier_stage",
    )
    for field in required_fields:
        if field not in message:
            return False, f"verified message missing field '{field}'"

    if message.get("verifier_stage") != VERIFIER_STAGE:
        return False, f"unexpected verifier_stage '{message.get('verifier_stage')}'"

    if not isinstance(message.get("data"), dict):
        return False, "verified message field 'data' must be an object"

    if not is_iso_timestamp(message.get("verified_at")):
        return False, "verified message field 'verified_at' must be an ISO-8601 timestamp"

    ok, expected_message_type, schema_name = classify_input_topic(
        str(message.get("input_topic", "")),
        commands_topic,
        home_topic,
    )
    if not ok:
        return False, schema_name

    if message.get("message_type") != expected_message_type:
        return False, (
            "verified message_type does not match input topic "
            f"('{message.get('message_type')}' != '{expected_message_type}')"
        )

    return validate_schema(message["data"], schema_name)
