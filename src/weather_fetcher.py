from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

import pandas as pd
import requests


ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


# -------------------------
# Logging configuration
# -------------------------
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CityGeo:
    """Geographic coordinates for a city."""
    lat: float
    lon: float


PACA_CITIES: dict[str, CityGeo] = {
    "Marseille": CityGeo(lat=43.2965, lon=5.3698),
    "Nice": CityGeo(lat=43.7102, lon=7.2620),
    "Toulon": CityGeo(lat=43.1242, lon=5.9280),
    "Avignon": CityGeo(lat=43.9493, lon=4.8055),
    "Gap": CityGeo(lat=44.5580, lon=6.0827),
    "Digne-les-Bains": CityGeo(lat=44.0922, lon=6.2376),
}


def project_root() -> Path:
    """Return the project root directory (parent of src/)."""
    return Path(__file__).resolve().parents[1]


def _get_json_with_retries(
    url: str,
    params: Mapping[str, Any],
    timeout_s: int = 30,
    retries: int = 3,
    backoff_factor: float = 1.6,
) -> Dict[str, Any]:
    """
    Perform a GET request and return JSON, with retries and exponential backoff.

    Retries on common temporary failures:
    - HTTP 429 (rate limit)
    - HTTP 5xx (server errors)
    - network errors/timeouts
    """
    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=dict(params), timeout=timeout_s)

            # Rate limit / temporary server errors
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(
                    f"Temporary API error: {response.status_code} - {response.text[:200]}"
                )

            response.raise_for_status()
            return response.json()

        except Exception as exc:  # noqa: BLE001 (acceptable here: log+retry)
            last_exc = exc
            wait_s = backoff_factor ** attempt
            logger.warning(
                "Open-Meteo request failed (attempt %d/%d). Waiting %.1fs. Error: %s",
                attempt,
                retries,
                wait_s,
                exc,
            )
            time.sleep(wait_s)

    raise RuntimeError(f"Open-Meteo API failed after {retries} retries.") from last_exc


def fetch_openmeteo_daily(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    timezone: str = "Europe/Paris",
) -> pd.DataFrame:
    """
    Fetch daily weather data from Open-Meteo archive API for a given location/date range.

    Args:
        lat: Latitude.
        lon: Longitude.
        start_date: Start date in YYYY-MM-DD.
        end_date: End date in YYYY-MM-DD.
        timezone: IANA timezone (default: Europe/Paris).

    Returns:
        DataFrame with columns: date, t_max, t_min, precipitation, wind_max.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
        ],
        "timezone": timezone,
    }

    logger.info(
        "Fetching Open-Meteo daily data lat=%.4f lon=%.4f (%s -> %s)",
        lat,
        lon,
        start_date,
        end_date,
    )

    data = _get_json_with_retries(ARCHIVE_URL, params=params, timeout_s=30, retries=3)

    if "daily" not in data or "time" not in data["daily"]:
        # Log a small excerpt to help debugging without dumping huge payloads
        logger.error("Unexpected Open-Meteo response format: keys=%s", list(data.keys()))
        raise ValueError("Unexpected Open-Meteo response format (missing 'daily').")

    daily = data["daily"]
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(daily["time"], errors="coerce"),
            "t_max": daily.get("temperature_2m_max"),
            "t_min": daily.get("temperature_2m_min"),
            "precipitation": daily.get("precipitation_sum"),
            "wind_max": daily.get("wind_speed_10m_max"),
        }
    )

    # Basic sanitation
    df = df.dropna(subset=["date"]).reset_index(drop=True)
    logger.info("Fetched %d rows.", len(df))
    return df


def download_paca_cities(
    start_date: str,
    end_date: str,
    force_download: bool = False,
) -> Path:
    """
    Download daily weather data for PACA cities and save as a raw CSV.

    The file is saved to: data/raw/openmeteo_paca_<start>_<end>.csv

    Args:
        start_date: Start date in YYYY-MM-DD.
        end_date: End date in YYYY-MM-DD.
        force_download: If False and file exists, reuse it.

    Returns:
        Path to the saved CSV file.
    """
    out_dir = project_root() / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"openmeteo_paca_{start_date}_{end_date}.csv"
    if out_file.exists() and not force_download:
        logger.info("Raw file already exists, reusing: %s", out_file)
        return out_file

    all_frames: list[pd.DataFrame] = []
    for city, geo in PACA_CITIES.items():
        df_city = fetch_openmeteo_daily(
            lat=geo.lat,
            lon=geo.lon,
            start_date=start_date,
            end_date=end_date,
            timezone=os.getenv("APP_TIMEZONE", "Europe/Paris"),
        )
        df_city["city"] = city
        all_frames.append(df_city)

    full = pd.concat(all_frames, ignore_index=True)

    full.to_csv(out_file, index=False)
    logger.info("Saved raw data to %s (rows=%d)", out_file, len(full))
    return out_file


def _configure_logging() -> None:
    """Configure logging level/format (useful for local runs)."""
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


if __name__ == "__main__":
    _configure_logging()

    # Defaults via environment variables (best practice)
    start = os.getenv("DEFAULT_START_DATE", "2013-01-01")
    end = os.getenv("DEFAULT_END_DATE", "2023-12-31")

    # Logging instead of print
    path = download_paca_cities(start, end)
    logger.info("Download completed: %s", path)
