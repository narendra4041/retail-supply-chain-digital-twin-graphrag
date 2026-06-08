import json
from pathlib import Path
from typing import Any, Dict

from jsonschema import Draft202012Validator


def load_json_file(file_path: str) -> Dict[str, Any]:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def validate_json_event(event: Dict[str, Any], schema: Dict[str, Any]) -> None:
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(event), key=lambda error: error.path)

    if errors:
        messages = []
        for error in errors:
            location = ".".join(str(part) for part in error.path)
            if not location:
                location = "<root>"
            messages.append(f"{location}: {error.message}")

        raise ValueError("Schema validation failed:\n" + "\n".join(messages))


def validate_event_file(event_file_path: str, schema_file_path: str) -> None:
    event = load_json_file(event_file_path)
    schema = load_json_file(schema_file_path)

    validate_json_event(event, schema)