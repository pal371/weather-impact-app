from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# --- Fix imports src/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.weather_fetcher import download_paca_cities  # noqa: E402
from src.weather_processing import process_raw_to_processed  # noqa: E402

# -------------------------
# Logging configuration
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------
# Streamlit config
# -------------------------
st.set_page_config(page_title="PACA – Analyse météo & climat", layout="wide")
st.title("Analyse météo & impact climatique — Région PACA")

st.markdown(
    """
Cette application télécharge des données météo réelles (**Open-Meteo**), les traite avec **Pandas**
et affiche des indicateurs & tendances climatiques pour la région **PACA**.
"""
)

# Defaults via env vars (best practice)
DEFAULT_START = os.getenv("DEFAULT_START_DATE", "2013-01-01")
DEFAULT_END = os.getenv("DEFAULT_END_DATE", "2023-12-31")

# --- Inputs
col_a, col_b = st.columns(2)
with col_a:
    start_date = st.date_input("Date de début", value=pd.to_datetime(DEFAULT_START))
with col_b:
    end_date = st.date_input("Date de fin", value=pd.to_datetime(DEFAULT_END))

if start_date > end_date:
    st.error("La date de début doit être antérieure (ou égale) à la date de fin.")
    st.stop()

st.markdown("---")


@st.cache_data(show_spinner=False)
def run_pipeline(start: str, end: str) -> pd.DataFrame:
    """
    End-to-end pipeline:
    - download raw CSV (data/raw)
    - process into processed CSV (data/processed)
    - load processed dataset into a DataFrame
    """
    raw_path = download_paca_cities(start, end)
    processed_path = process_raw_to_processed(str(raw_path))
    df_local = pd.read_csv(processed_path, parse_dates=["date"])
    return df_local


# --- SINGLE BUTTON
if st.button("🚀 Lancer l’analyse climatique PACA", use_container_width=True):
    try:
        with st.spinner("Téléchargement et traitement des données en cours..."):
            df = run_pipeline(str(start_date), str(end_date))

        logger.info("Pipeline completed. Rows=%d Cols=%d", df.shape[0], df.shape[1])
        st.success("Analyse terminée avec succès ✅")

        # =====================
        # == APERÇU DES DONNÉES
        # =====================
        st.subheader("Aperçu des données")
        st.dataframe(df.head(50), use_container_width=True)

        # =====================
        # == INDICATEURS CLÉS
        # =====================
        st.subheader("Indicateurs climatiques (PACA)")
        m1, m2, m3 = st.columns(3)

        m1.metric("Jours ≥ 30°C", int(df["hot_day_30"].sum()))
        m2.metric("Jours ≥ 35°C", int(df["hot_day_35"].sum()))
        m3.metric("Jours pluie ≥ 20 mm", int(df["heavy_rain_20"].sum()))

        # =====================
        # == GRAPHIQUES (quotidien)
        # =====================
        st.subheader("Température moyenne journalière (t_mean)")
        temp = df.groupby(["date", "city"], as_index=False)["t_mean"].mean()
        temp_pivot = temp.pivot(index="date", columns="city", values="t_mean")
        st.line_chart(temp_pivot)

        st.subheader("Précipitations journalières")
        rain = df.groupby(["date", "city"], as_index=False)["precipitation"].sum()
        rain_pivot = rain.pivot(index="date", columns="city", values="precipitation")
        st.area_chart(rain_pivot)

        # =====================
        # == TENDANCES (annuel)
        # =====================
        st.markdown("---")
        st.header("Tendances annuelles")

        # au choix : tendance pour une ville ou toutes
        cities = sorted(df["city"].unique().tolist())
        selected_city = st.selectbox(
            "Choisir une ville (tendance) :",
            options=["Toutes (moyenne)"] + cities,
        )

        df_trend = df if selected_city == "Toutes (moyenne)" else df[df["city"] == selected_city]

        yearly = (
            df_trend.set_index("date")
            .resample("Y")
            .agg(
                t_mean=("t_mean", "mean"),
                precipitation=("precipitation", "sum"),
                hot_days_30=("hot_day_30", "sum"),
                hot_days_35=("hot_day_35", "sum"),
            )
            .reset_index()
        )
        yearly["year"] = yearly["date"].dt.year

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Température moyenne annuelle")
            st.line_chart(yearly.set_index("year")[["t_mean"]])

        with c2:
            st.subheader("Précipitations annuelles (somme)")
            st.bar_chart(yearly.set_index("year")[["precipitation"]])

        st.subheader("Jours chauds par an")
        st.line_chart(yearly.set_index("year")[["hot_days_30", "hot_days_35"]])

        with st.expander("Voir les données annuelles (table)"):
            st.dataframe(yearly, use_container_width=True)

    except Exception as exc:
        logger.exception("App error: %s", exc)
        st.error("Une erreur est survenue pendant l'analyse.")
        st.exception(exc)

else:
    st.info("Choisis une période puis clique sur le bouton pour lancer l’analyse.")
