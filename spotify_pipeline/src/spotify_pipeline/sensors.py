from pathlib import Path
from datetime import datetime

from dagster import RunRequest, SkipReason, sensor


@sensor(
    job_name="__ASSET_JOB",
    # minimum_interval_seconds=120,
    required_resource_keys={"file_config"},
)
def my_sensor(context):
    """Sensor that watches the data directory for new/updated CSVs."""

    file_config = context.resources.file_config
    data_dir = Path(file_config.data_path)

    if not data_dir.exists():
        yield SkipReason(f"Data directory not found: {data_dir}")
        return

    csv_files = [p for p in data_dir.glob("*.csv") if p.is_file()]

    if not csv_files:
        yield SkipReason("No CSV files detected yet")
        return

    newest = max(csv_files, key=lambda p: p.stat().st_mtime)
    newest_mtime = newest.stat().st_mtime
    last_cursor = float(context.cursor) if context.cursor else None

    if last_cursor and newest_mtime <= last_cursor:
        yield SkipReason("No new data since last check")
        return

    run_key = f"csv-update-{newest.name}-{int(newest_mtime)}"
    context.update_cursor(str(newest_mtime))

    context.log.info(
        "Detected new/updated CSV %s (mtime=%s)", newest.name, datetime.fromtimestamp(newest_mtime)
    )

    yield RunRequest(run_key=run_key)