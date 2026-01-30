# Weather Impact App (PACA)

Application data-driven qui télécharge des données météo réelles (Open-Meteo),
les traite (Pandas) et les visualise (Streamlit) pour analyser des tendances climatiques en PACA.


# Weather Impact App (PACA) — Analyse météo & impact climatique local

## Membres du groupe

- Ouedraogo Paligwende Rosette

## Dépôt Git (public)

Lien vers le dépôt : https://[github.com/pal371](https://github.com/pal371/weather-impact-app)

## Objectif du projet

Cette application de données complète :

1. Télécharge des données météo réelles (Open-Meteo)
2. Les prétraite et enrichit (Pandas)
3. Visualise des indicateurs et tendances climatiques via Streamlit

Périmètre : **Région PACA** (Marseille, Nice, Toulon, Avignon, Gap, Digne-les-Bains).


## Source de données

- **Open-Meteo Archive API** (données quotidiennes : température min/max, précipitations, vent)


## Structure du projet

- `src/` : logique métier (collecte + traitement)
- `app/` : application Streamlit
- `data/raw/` : données brutes téléchargées (non versionnées)
- `data/processed/` : données enrichies (non versionnées)
- `Dockerfile`, `requirements.txt` : exécution reproductible

---

## Gestion des données (important)

Les fichiers de données peuvent dépasser **4–5 Mo** sur une longue période.
**Les dossiers `data/raw/` et `data/processed/` ne sont pas inclus dans le dépôt Git** (voir `.gitignore`).

Pour générer les données localement :

- téléchargement (raw) : `src/weather_fetcher.py `
- traitement (processed) : `src/weather_processing.py`

---

## Exécution en local (venv)

## Créer et activer l’environnement

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -r requirements.txt

streamlit run app/streamlit_app.py

```

    Exécution avec Docker

docker build -t weather-impact-app .

docker run --rm -p 8501:8501 weather-impact-app
