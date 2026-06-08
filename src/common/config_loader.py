from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def find_project_root(start_path: Path | None = None) -> Path:
    """
    Finds project root by looking for configs/ and src/ folders.

    This works both locally and inside Databricks Git folders.
    """
    current_path = start_path or Path.cwd()

    for path in [current_path, *current_path.parents]:
        if (path / "configs").exists() and (path / "src").exists():
            return path

    raise FileNotFoundError(
        "Project root not found. Expected to find both configs/ and src/ folders."
    )


def load_config(environment: str = "dev") -> Dict[str, Any]:
    project_root = find_project_root()
    config_path = project_root / "configs" / f"{environment}.json"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)