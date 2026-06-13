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

import logging
import os
import sys
from contextlib import asynccontextmanager

import psycopg2.extras
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Réutilise la connexion (variables DB_* du .env) définie côté ingestion.
# NB : import direct des modules (pas ingestion.xxx) pour rester cohérent —
# deux styles d'import créeraient deux instances des mêmes modules.
RACINE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(RACINE, "ingestion"))
from moteur_ia import main as run_ia            # noqa: E402
from scraper_daemon import connecter_postgres   # noqa: E402
from scraper_daemon import main as run_scraper  # noqa: E402

load_dotenv()

logger = logging.getLogger("oracle2026.serveur")


# ---------------------------------------------------------------------------
# Travailleurs en arrière-plan (APScheduler) : plus de lancement manuel.
# Chaque tâche est blindée — une exception ne tue ni le scheduler ni l'API.
# ---------------------------------------------------------------------------

def tache_ingestion() -> None:
    """Cycle scraper (API + RSS + réconciliation + insertion), 24h/24.

    Le RSS est collecté à chaque passage ; l'appel API est borné à
    MAX_APPELS_API_JOUR appels/jour espacés (géré dans scraper_daemon).
    """
    try:
        code = run_scraper([])
        logger.info("Tâche ingestion terminée (code %s)", code)
    except Exception:
        logger.exception("Tâche ingestion en échec")


def tache_moteur_ia() -> None:
    """Pronostics llama3 pour les matchs que la base a (in)validés."""
    try:
        code = run_ia()
        logger.info("Tâche moteur IA terminée (code %s)", code)
    except Exception:
        logger.exception("Tâche moteur IA en échec")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Rend visibles les logs des tâches de fond (oracle2026.* et apscheduler)
    # dans la sortie uvicorn, sans toucher aux loggers propres d'uvicorn.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s : %(message)s",
    )
    planificateur = BackgroundScheduler(timezone="Asia/Dubai")
    # max_instances=1 : un cycle encore en cours n'est jamais doublé ;
    # coalesce : les exécutions manquées (machine en veille) sont fusionnées.
    planificateur.add_job(tache_ingestion, "interval", minutes=60,
                          id="ingestion", coalesce=True, max_instances=1)
    # 65 min (et non 60) : laisse au scraper le temps de finir avant l'IA
    planificateur.add_job(tache_moteur_ia, "interval", minutes=65,
                          id="moteur_ia", coalesce=True, max_instances=1)
    planificateur.start()
    logger.info("Planificateur démarré : ingestion / 60 min, moteur IA / 65 min")
    yield
    planificateur.shutdown(wait=False)


app = FastAPI(title="Oracle 2026 — API dashboard", version="0.2.0", lifespan=lifespan)

# CORS : le dashboard est ouvert en local (file:// ou petit serveur statique),
# donc origine imprévisible -> tout autoriser, en lecture seule (GET).
# ⚠ À restreindre à l'origine réelle si l'API est un jour exposée au réseau.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Frontend servi par la même application : indispensable pour l'accès distant
# (tunnel/téléphone), où file:// est impossible et où l'API doit être jointe
# sur la même origine que la page (URLs relatives côté JS).
# ---------------------------------------------------------------------------

app.mount("/frontend", StaticFiles(directory=os.path.join(RACINE, "frontend")), name="frontend")


@app.get("/", include_in_schema=False)
def accueil() -> FileResponse:
    """Le dashboard glassmorphism, servi à la racine."""
    return FileResponse(os.path.join(RACINE, "frontend", "index.html"))

# Trois vues sur la même requête : fenêtre courante (jour), matchs joués
# (historique), slots du calendrier complet (calendrier). Les fragments SQL
# sont choisis dans CE dictionnaire — jamais depuis l'entrée utilisateur.
FILTRES = {
    # Vue principale : TOUS les vrais matchs (programmés, en cours, terminés)
    # pour pouvoir faire défiler passé et avenir de la compétition.
    "jour": (
        "WHERE m.statut <> 'EN_ATTENTE'",
        "ORDER BY m.coup_envoi",
    ),
    "historique": (
        "WHERE m.statut = 'TERMINE'",
        "ORDER BY m.coup_envoi DESC LIMIT 60",
    ),
    "calendrier": (
        "WHERE m.statut = 'EN_ATTENTE'",
        "ORDER BY m.coup_envoi",
    ),
}


def signal_pari(match: dict) -> dict | None:
    """Combine confiance IA et indice de risque en signal actionnable.

    Règle (du plus restrictif au moins restrictif) :
      - indice de risque jamais calculé        -> À ÉVALUER (neutre)
      - risque >= 80 ou confiance < 0.45        -> À FUIR
      - confiance >= 0.70 et risque < 40        -> PARI FORT
      - confiance >= 0.55 et risque < 65        -> PARI POSSIBLE
      - sinon                                   -> PRUDENCE
    Pas de signal sans pronostic VALIDE sur un match à venir.
    """
    if (match["statut"] != "A_VENIR" or match["issue"] is None
            or match["statut_pronostic"] != "VALIDE"):
        return None
    # indice_maj_le NULL = aucun contexte n'a encore déclenché le calcul du
    # risque : on n'affiche PAS de "pari fort" sur un match non évalué.
    if match.get("indice_maj_le") is None:
        return {"libelle": "À ÉVALUER", "niveau": "neutre"}
    confiance = float(match["confiance"])
    risque = float(match["indice_risque"])
    if risque >= 80 or confiance < 0.45:
        return {"libelle": "À FUIR", "niveau": "fuir"}
    if confiance >= 0.70 and risque < 40:
        return {"libelle": "PARI FORT", "niveau": "fort"}
    if confiance >= 0.55 and risque < 65:
        return {"libelle": "PARI POSSIBLE", "niveau": "possible"}
    return {"libelle": "PRUDENCE", "niveau": "prudence"}


# Socle commun aux trois filtres, avec — via LATERAL — le pronostic le plus
# récent et le dernier évènement de contexte de chaque match.
SQL_MATCHS = """
    SELECT m.id,
           ed.nom            AS equipe_dom,
           ee.nom            AS equipe_ext,
           m.coup_envoi,
           m.stade,
           m.ville,
           m.phase,
           m.statut,
           m.indice_risque,
           m.indice_maj_le,
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
    {where}
    {order};
"""


# KPIs du bandeau : tous calculés sur la même fenêtre que /api/matchs_du_jour
# pour que les chiffres du haut correspondent aux cartes affichées en dessous.
SQL_KPIS = """
    WITH fenetre AS (
        SELECT id FROM matchs
        WHERE coup_envoi >= now() - INTERVAL '12 hours'
          AND coup_envoi <  now() + INTERVAL '48 hours'
    )
    SELECT
        (SELECT count(*) FROM contexte_actu
          WHERE match_id IN (SELECT id FROM fenetre))         AS evenements_nuit,
        (SELECT round(avg(confiance) * 100)
           FROM pronostics_llm
          WHERE statut = 'VALIDE'
            AND match_id IN (SELECT id FROM fenetre))         AS confiance_moyenne,
        (SELECT count(*) FROM pronostics_llm
          WHERE statut = 'OBSOLETE'
            AND match_id IN (SELECT id FROM fenetre))         AS pronostics_perimes;
"""


@app.get("/api/kpis")
def kpis() -> dict:
    """Indicateurs du bandeau : daemons, évènements, confiance moyenne, périmés."""
    try:
        conn = connecter_postgres()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Base de données injoignable : {exc}")
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SQL_KPIS)
            ligne = dict(cur.fetchone())
        return {
            # Statique pour l'instant : reflètera l'état réel des 3 volets
            # (API, RSS, IA) quand un heartbeat sera stocké en base.
            "daemons_actifs": "3/3",
            "evenements_nuit": ligne["evenements_nuit"],
            "confiance_moyenne": ligne["confiance_moyenne"],   # null si aucun VALIDE
            "pronostics_perimes": ligne["pronostics_perimes"],
        }
    finally:
        conn.close()


@app.get("/api/matchs_du_jour")
def matchs_du_jour(filtre: str = "jour") -> dict:
    """Matchs selon le filtre (jour | historique | calendrier), avec indice de
    risque, statut, dernier pronostic LLM et signal de pari."""
    if filtre not in FILTRES:
        raise HTTPException(status_code=400,
                            detail=f"Filtre inconnu : {filtre} (attendus : {', '.join(FILTRES)})")
    where, order = FILTRES[filtre]
    try:
        conn = connecter_postgres()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Base de données injoignable : {exc}")
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # .format() sur des fragments internes uniquement (cf. FILTRES)
            cur.execute(SQL_MATCHS.format(where=where, order=order))
            matchs = [dict(ligne) for ligne in cur.fetchall()]
        for match in matchs:
            match["signal"] = signal_pari(match)
        return {"filtre": filtre, "matchs": matchs}
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
