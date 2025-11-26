"""Dagster definitions for the Spotify pipeline - assets folder."""

from pathlib import Path
from dagster import Definitions, load_assets_from_modules

from . import data_assets, model_assets
from .resources import FileConfig
from ..io_manager import custom_io_manager
from ..sensors import my_sensor


def _find_project_root() -> Path:
    """Find the Spotify project root directory."""
    current = Path.cwd()
    
    # Try to find Spotify folder
    for parent in [current, current.parent, current.parent.parent]:
        if parent.name == "Spotify" or (parent / "data").exists():
            return parent
    
    return current.parent if current.name == "spotify_pipeline" else current


# Load all assets
all_assets = load_assets_from_modules([data_assets, model_assets])

# Get the storage path
project_root = _find_project_root()
storage_dir = project_root / "data" / "dagster_storage"
storage_dir.mkdir(parents=True, exist_ok=True)

# Shared file configuration for both assets and API consumers
file_cfg = FileConfig(
    data_dir=str(project_root / "data"),
    models_dir=str(project_root / "models"),
)

# Define the pipeline
defs = Definitions(
    assets=all_assets,
    resources={
        "file_config": file_cfg,
        "io_manager": custom_io_manager.configured({
            "base_dir": str(storage_dir),
        }),
    },
    sensors=[my_sensor],
)
