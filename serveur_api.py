#!/usr/bin/env python3
"""
serveur_api.py — Oracle 2026 : API REST du dashboard (FastAPI)
===============================================================

Expose les données consolidées de PostgreSQL au frontend glassmorphism
(frontend/index.html). Lecture seule : l'écriture reste l'affaire du daemon
d'ingestion et du moteur IA.

Lancement :
    .venv/bin/uvicorn serveur_api:app --port 8000          # production locale
    .venv/bin/uvicorn serveur_api:app --port 8000 --reload # développement

Route :
    GET /api/matchs_du_jour  -> matchs récents/à venir + indice de risque,
                                statut, dernier pronostic LLM et dernier
                                évènement de contexte.
"""

import os
import sys

import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Réutilise la connexion (variables DB_* du .env) définie côté ingestion
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingestion"))
from scraper_daemon import connecter_postgres  # noqa: E402

load_dotenv()

app = FastAPI(title="Oracle 2026 — API dashboard", version="0.1.0")

# CORS : le dashboard est ouvert en local (file:// ou petit serveur statique),
# donc origine imprévisible -> tout autoriser, en lecture seule (GET).
# ⚠ À restreindre à l'origine réelle si l'API est un jour exposée au réseau.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Matchs de la fenêtre courante (12 h en arrière pour garder les matchs de la
# nuit affichés au matin, 48 h en avant), avec — via LATERAL — le pronostic le
# plus récent et le dernier évènement de contexte de chaque match.
SQL_MATCHS_DU_JOUR = """
    SELECT m.id,
           ed.nom            AS equipe_dom,
           ee.nom            AS equipe_ext,
           m.coup_envoi,
           m.stade,
           m.ville,
           m.phase,
           m.statut,
           m.indice_risque,
           m.score_dom,
           m.score_ext,
           p.issue,
           p.score_estime,
           p.confiance,
           p.statut          AS statut_pronostic,
           ev.description    AS dernier_evenement
    FROM matchs m
    JOIN equipes ed ON ed.id = m.equipe_dom_id
    JOIN equipes ee ON ee.id = m.equipe_ext_id
    LEFT JOIN LATERAL (
        SELECT issue, score_estime, confiance, statut
        FROM pronostics_llm
        WHERE match_id = m.id
        ORDER BY genere_le DESC
        LIMIT 1
    ) p ON TRUE
    LEFT JOIN LATERAL (
        SELECT description
        FROM contexte_actu
        WHERE match_id = m.id
        ORDER BY detecte_le DESC
        LIMIT 1
    ) ev ON TRUE
    WHERE m.coup_envoi >= now() - INTERVAL '12 hours'
      AND m.coup_envoi <  now() + INTERVAL '48 hours'
    ORDER BY m.coup_envoi;
"""


@app.get("/api/matchs_du_jour")
def matchs_du_jour() -> dict:
    """Matchs du jour avec indice de risque, statut et dernier pronostic LLM."""
    try:
        conn = connecter_postgres()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Base de données injoignable : {exc}")
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SQL_MATCHS_DU_JOUR)
            matchs = [dict(ligne) for ligne in cur.fetchall()]
        return {"matchs": matchs}
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
