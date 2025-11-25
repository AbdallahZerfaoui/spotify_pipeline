from __future__ import annotations

import os
import pickle
from pathlib import Path
import pandas as pd
from dagster import IOManager, io_manager

class CustomIOManager(IOManager):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def handle_output(self, context, obj):
        self.base_dir.mkdir(parents=True, exist_ok=True)
        asset_name = self._determine_name(context)

        if isinstance(obj, pd.DataFrame):
            output_path = self.base_dir / f"{asset_name}.csv"
            obj.to_csv(output_path, index=False)
            loader_hint = "csv"
        else:
            output_path = self.base_dir / f"{asset_name}.pkl"
            with output_path.open("wb") as f:
                pickle.dump(obj, f)
            loader_hint = "pickle"

        context.log.info(f"✅ Saved output to: {output_path}")

        metadata = {
            "path": str(output_path),
            "format": loader_hint,
        }
        try:
            context.add_output_metadata(metadata)
        except Exception:
            # Older Dagster versions may not support add_output_metadata here.
            context.log.debug("Could not attach output metadata for %s", asset_name)

    def load_input(self, context):
        path = self._resolve_upstream_path(context)

        if path.suffix == ".csv":
            df = pd.read_csv(path)
            context.log.info(f"✅ Loaded input from: {path}")
            return df

        if path.suffix == ".pkl":
            with path.open("rb") as f:
                obj = pickle.load(f)
            context.log.info(f"✅ Loaded input from: {path}")
            return obj

        raise ValueError(f"Unsupported file format for {path}")

    def _determine_name(self, context) -> str:
        if context.asset_key:
            return "_".join(context.asset_key.path)
        return context.step_key

    def _resolve_upstream_path(self, context) -> Path:
        upstream = context.upstream_output

        # Prefer metadata provided during handle_output
        metadata = getattr(upstream, "metadata", {}) or {}
        path_hint = metadata.get("path")

        if not path_hint:
            # Dagster can cache the return value for handle_output; check for dict-like
            upstream_value = getattr(upstream, "value", None)
            if isinstance(upstream_value, dict) and "path" in upstream_value:
                path_hint = upstream_value["path"]

        if path_hint:
            path = Path(path_hint)
            if path.exists():
                return path

        # Fallback to deterministic naming based on step name
        base_name = "_".join(upstream.asset_key.path) if upstream.asset_key else upstream.step_key
        for suffix in (".csv", ".pkl"):
            candidate = self.base_dir / f"{base_name}{suffix}"
            if candidate.exists():
                return candidate

        raise FileNotFoundError(
            f"Unable to locate stored output for upstream step '{base_name}' in {self.base_dir}"
        )

@io_manager
def custom_io_manager(init_context):
    return CustomIOManager(base_dir=init_context.resource_config["base_dir"])