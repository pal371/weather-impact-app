from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

import pandas as pd


logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: Final[set[str]] = {
    "date",
    "t_min",
    "t_max",
    "precipitation",
    "wind_max",
    "city",
}


def project_root() -> Path:
    """Return the project root directory (parent of src/)."""
    return Path(__file__).resolve().parents[1]


def validate_raw_schema(df: pd.DataFrame) -> None:
    """
    Validate that required columns exist in the raw dataset.

    Raises:
        ValueError: if required columns are missing.
    """
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in raw data: {sorted(missing)}")


def add_climate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived climate indicators used in the analysis.

    Indicators:
        - t_mean: daily mean temperature
        - hot_day_30: t_max >= 30°C
        - hot_day_35: t_max >= 35°C
        - heavy_rain_20: precipitation >= 20mm

    Returns:
        A new DataFrame with additional columns.
    """
    out = df.copy()

    # Ensure numeric types (robustness)
    out["t_min"] = pd.to_numeric(out["t_min"], errors="coerce")
    out["t_max"] = pd.to_numeric(out["t_max"], errors="coerce")
    out["precipitation"] = pd.to_numeric(out["precipitation"], errors="coerce")
    out["wind_max"] = pd.to_numeric(out["wind_max"], errors="coerce")

    out["t_mean"] = (out["t_min"] + out["t_max"]) / 2
    out["hot_day_30"] = out["t_max"] >= 30
    out["hot_day_35"] = out["t_max"] >= 35
    out["heavy_rain_20"] = out["precipitation"] >= 20

    return out


def process_raw_to_processed(raw_csv_path: str | Path) -> Path:
    """
    Transform an Open-Meteo raw CSV into a processed dataset with indicators.

    Reads:
        data/raw/openmeteo_paca_<start>_<end>.csv

    Writes:
        data/processed/openmeteo_paca_<start>_<end>_processed.csv

    Args:
        raw_csv_path: Path to the raw CSV file.

    Returns:
        Path to the processed CSV file.

    Raises:
        FileNotFoundError: if raw file does not exist.
        ValueError: if schema is invalid.
    """
    raw_path = Path(raw_csv_path)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    logger.info("Reading raw data: %s", raw_path)
    df = pd.read_csv(raw_path, parse_dates=["date"])

    validate_raw_schema(df)

    # Basic cleaning
    df = df.dropna(subset=["date", "city"]).copy()
    df = df.sort_values(["city", "date"]).reset_index(drop=True)

    # Add indicators
    df_processed = add_climate_indicators(df)

    # Save processed
    out_dir = project_root() / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"{raw_path.stem}_processed.csv"
    df_processed.to_csv(out_file, index=False)

    logger.info("Processed data saved: %s (rows=%d)", out_file, len(df_processed))
    return out_file
