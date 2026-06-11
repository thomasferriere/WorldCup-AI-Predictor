#!/usr/bin/env python3
"""
moteur_ia.py — Oracle 2026 : moteur de décision (LLM local Ollama)
===================================================================

Rôle : la "phase 2" du README — lire les features consolidées d'un match
(équipes + contexte agrégé + indice de risque), construire un prompt
structuré, interroger le modèle LLM léger local (Ollama / llama3, port 11434),
puis écrire le pronostic dans `pronostics_llm`.

Pipeline :
    recuperer_matchs_a_analyser()         # PostgreSQL -> matchs + contexte
        -> generer_pronostic_ollama()     # POST /api/generate (llama3)
        -> sauvegarder_pronostic()        # INSERT INTO pronostics_llm

Points d'architecture :
  - seuls les matchs SANS pronostic VALIDE sont analysés : le trigger
    fn_invalider_pronostics côté base marque les pronostics OBSOLETE quand un
    nouvel évènement de contexte arrive — c'est donc la base qui décide de ce
    qui doit être (re)calculé, pas ce script ;
  - le trigger remplit aussi indice_risque_snapshot à l'insertion : on ne
    l'envoie pas nous-mêmes ;
  - le LLM tourne en local : aucun quota, aucune clé, aucune donnée sortante.

Lancement (cron à 08:30 GMT+4, juste après la fenêtre d'ingestion — README §8) :
    /usr/bin/python3 moteur_ia.py
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# Connexion PostgreSQL partagée avec le daemon d'ingestion (variables DB_*)
from scraper_daemon import connecter_postgres

load_dotenv()

# Ollama local — aucun secret nécessaire, mais l'URL reste surchargeable
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_TIMEOUT = 180   # un modèle 8B local peut être lent à la première requête

MODEL_VERSION = f"{OLLAMA_MODEL}-local"

logger = logging.getLogger("oracle2026.moteur_ia")


# -----------------------------------------------------------------------------
# ÉTAPE 1 — Lecture des matchs du jour et de leur contexte
# -----------------------------------------------------------------------------

# Matchs des 48 prochaines heures (le cron de 08:30 GMT+4 doit couvrir les
# matchs de la nuit suivante, qui tombent sur le jour calendaire suivant) à
# venir et sans pronostic VALIDE, avec leurs évènements de contexte agrégés en
# JSON. Un match sans contexte sort quand même (json '[]') : l'absence de
# contexte est elle-même une information (match "lisible").
SQL_MATCHS_A_ANALYSER = """
    SELECT m.id                                   AS match_id,
           ed.nom                                 AS equipe_dom,
           ed.classement_fifa                     AS clas_dom,
           ee.nom                                 AS equipe_ext,
           ee.classement_fifa                     AS clas_ext,
           m.coup_envoi,
           m.stade,
           m.ville,
           m.phase,
           m.indice_risque,
           COALESCE(
               json_agg(
                   json_build_object(
                       'type',        c.type_evenement,
                       'joueur',      c.joueur_nom,
                       'impact',      c.impact_score,
                       'fiabilite',   c.fiabilite_source,
                       'description', c.description
                   ) ORDER BY c.detecte_le DESC
               ) FILTER (WHERE c.id IS NOT NULL),
               '[]'
           )                                      AS evenements
    FROM matchs m
    JOIN equipes ed ON ed.id = m.equipe_dom_id
    JOIN equipes ee ON ee.id = m.equipe_ext_id
    LEFT JOIN contexte_actu c ON c.match_id = m.id
    WHERE m.statut = 'A_VENIR'
      AND m.coup_envoi >= now()
      AND m.coup_envoi <  now() + INTERVAL '48 hours'
      AND NOT EXISTS (
            SELECT 1 FROM pronostics_llm p
            WHERE p.match_id = m.id AND p.statut = 'VALIDE'
      )
    GROUP BY m.id, ed.nom, ed.classement_fifa, ee.nom, ee.classement_fifa
    ORDER BY m.coup_envoi;
"""


def recuperer_matchs_a_analyser(conn: Any) -> list[dict[str, Any]]:
    """Matchs des 48 prochaines heures, chacun avec ses évènements de contexte."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_MATCHS_A_ANALYSER)
        matchs = [dict(ligne) for ligne in cur.fetchall()]
    logger.info("%d match(s) en attente de pronostic", len(matchs))
    return matchs


# -----------------------------------------------------------------------------
# ÉTAPE 2 — Interrogation du LLM local (Ollama)
# -----------------------------------------------------------------------------

def generer_pronostic_ollama(match_data: dict[str, Any],
                             contexte_data: list[dict[str, Any]]) -> str:
    """Construit un prompt en texte brut et interroge llama3 via Ollama.

    Le format de réponse est imposé (4 lignes ISSUE/SCORE/CONFIANCE/
    JUSTIFICATION) pour pouvoir alimenter les colonnes typées de
    pronostics_llm. Retourne le texte brut généré par le modèle.
    """
    dom, ext = match_data["equipe_dom"], match_data["equipe_ext"]

    if contexte_data:
        lignes_contexte = "\n".join(
            f"- [{ev['type']}] (impact {ev['impact']}/100, fiabilité {ev['fiabilite']}/10) "
            f"{(ev.get('joueur') or '')} {ev['description']}".strip()
            for ev in contexte_data
        )
    else:
        lignes_contexte = "- Aucun évènement particulier : contexte stable."

    prompt = f"""Tu es un analyste expert des paris sportifs sur le football.
Analyse ce match de la Coupe du Monde 2026 et donne ton pronostic.

MATCH : {dom} (domicile) contre {ext} (extérieur)
Coup d'envoi : {match_data['coup_envoi']}
Lieu : {match_data.get('stade') or '?'}, {match_data.get('ville') or '?'} — {match_data['phase']}
Classement FIFA : {dom} = {match_data.get('clas_dom') or '?'}, {ext} = {match_data.get('clas_ext') or '?'}
Indice de risque calculé (0 = match lisible, 100 = très incertain) : {match_data['indice_risque']}

CONTEXTE D'AVANT-MATCH (scraping de la nuit) :
{lignes_contexte}

Réponds EXACTEMENT dans ce format, en 4 lignes, sans aucun autre texte :
ISSUE: 1, N ou 2 (1 = victoire {dom}, N = match nul, 2 = victoire {ext})
SCORE: score exact le plus probable, exemple 2-1
CONFIANCE: nombre entre 0 et 1, exemple 0.65 (baisse-la si l'indice de risque est élevé)
JUSTIFICATION: une seule phrase en français résumant ton raisonnement"""

    reponse = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=OLLAMA_TIMEOUT,
    )
    reponse.raise_for_status()
    return reponse.json()["response"]


def extraire_champs_pronostic(texte: str) -> dict[str, Any]:
    """Parse la réponse du LLM vers les colonnes de pronostics_llm.

    Le modèle local ne respecte pas toujours parfaitement le format : chaque
    champ a donc une valeur de repli prudente (nul, confiance basse) plutôt
    que de faire échouer l'insertion.
    """
    issue = re.search(r"ISSUE\s*:\s*([1N2])", texte, re.IGNORECASE)
    score = re.search(r"SCORE\s*:\s*(\d{1,2}\s*-\s*\d{1,2})", texte, re.IGNORECASE)
    confiance = re.search(r"CONFIANCE\s*:\s*([01](?:[.,]\d+)?)", texte, re.IGNORECASE)
    justification = re.search(r"JUSTIFICATION\s*:\s*(.+)", texte, re.IGNORECASE)

    valeur_confiance = 0.30
    if confiance:
        valeur_confiance = min(max(float(confiance.group(1).replace(",", ".")), 0.0), 1.0)

    return {
        "issue": issue.group(1).upper() if issue else "N",
        "score_estime": score.group(1).replace(" ", "") if score else None,
        "confiance": round(valeur_confiance, 3),
        "justification": (justification.group(1).strip() if justification else texte.strip())[:2000],
    }


# -----------------------------------------------------------------------------
# ÉTAPE 3 — Sauvegarde du pronostic
# -----------------------------------------------------------------------------

SQL_INSERT_PRONOSTIC = """
    INSERT INTO pronostics_llm
          (match_id, issue, score_estime, confiance, justification, model_version)
    VALUES (%(match_id)s, %(issue)s, %(score_estime)s, %(confiance)s,
            %(justification)s, %(model_version)s);
"""


def sauvegarder_pronostic(conn: Any, match_id: int, texte_genere: str) -> dict[str, Any]:
    """Insère le pronostic dans pronostics_llm et retourne les champs extraits.

    indice_risque_snapshot et statut sont remplis côté base (trigger/défaut) :
    on n'envoie que ce que ce script connaît.
    """
    champs = extraire_champs_pronostic(texte_genere)
    with conn.cursor() as cur:
        cur.execute(SQL_INSERT_PRONOSTIC, {
            "match_id": match_id,
            "model_version": MODEL_VERSION,
            **champs,
        })
    return champs


# -----------------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s : %(message)s",
    )

    try:
        conn = connecter_postgres()
    except Exception:
        logger.exception("Connexion PostgreSQL impossible (variables DB_* du .env ?)")
        return 2

    code_sortie = 0
    try:
        matchs = recuperer_matchs_a_analyser(conn)
        for match in matchs:
            libelle = f"{match['equipe_dom']} - {match['equipe_ext']} (match {match['match_id']})"
            try:
                texte = generer_pronostic_ollama(match, match["evenements"])
                champs = sauvegarder_pronostic(conn, match["match_id"], texte)
                conn.commit()   # commit par match : un échec n'annule pas les autres
                logger.info("%s : %s %s (confiance %.2f)",
                            libelle, champs["issue"], champs["score_estime"] or "?",
                            champs["confiance"])
            except Exception:
                conn.rollback()
                logger.exception("Échec du pronostic pour %s", libelle)
                code_sortie = 1
    finally:
        conn.close()

    return code_sortie


if __name__ == "__main__":
    sys.exit(main())
