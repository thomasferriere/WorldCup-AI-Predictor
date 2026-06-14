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
import re
import sys
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

import config
# Connexion PostgreSQL partagée avec le daemon d'ingestion (variables DB_*)
from scraper_daemon import connecter_postgres

load_dotenv()

# Modèle d'IA local (réglages dans config.py)
MODEL_VERSION = f"{config.OLLAMA_MODEL}-local"

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
           m.indice_maj_le,
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
        lignes_contexte = "- Aucun évènement particulier signalé."

    # CDM 2026 : terrain neutre, sauf pour les trois pays hôtes.
    HOTES = {"États-Unis", "Canada", "Mexique"}
    if dom in HOTES and ext not in HOTES:
        note_terrain = f"{dom} est un pays organisateur : léger avantage du terrain. {ext} se déplace."
    elif ext in HOTES and dom not in HOTES:
        note_terrain = f"{ext} est un pays organisateur : léger avantage du terrain. {dom} se déplace."
    else:
        note_terrain = "Match sur terrain neutre : aucune équipe ne joue à domicile."

    rang_dom = match_data.get("clas_dom") or "non disponible"
    rang_ext = match_data.get("clas_ext") or "non disponible"

    # Le favori FIFA est déterminé EN CODE (rang le plus petit = meilleur) puis
    # annoncé au modèle : le 8B local lit mal la convention inversée et se
    # contredit. On lui livre la conclusion, il garde l'arbitrage final.
    rd, re_ = match_data.get("clas_dom"), match_data.get("clas_ext")
    if rd and re_:
        if rd < re_:
            ligne_favori = f"Sur le seul classement FIFA, {dom} est FAVORI (mieux classé : {rd} contre {re_})."
        elif re_ < rd:
            ligne_favori = f"Sur le seul classement FIFA, {ext} est FAVORI (mieux classé : {re_} contre {rd})."
        else:
            ligne_favori = "Les deux équipes sont à égalité au classement FIFA."
    elif rd and not re_:
        ligne_favori = f"Seul {dom} a un classement FIFA connu ({rd}) : léger crédit à {dom}."
    elif re_ and not rd:
        ligne_favori = f"Seul {ext} a un classement FIFA connu ({re_}) : léger crédit à {ext}."
    else:
        ligne_favori = "Aucun classement FIFA disponible : pas de favori sur ce critère."

    # indice_maj_le NULL = aucun contexte n'a déclenché le calcul : "0" ne veut
    # PAS dire "match lisible" mais "pas encore évalué". On le dit au modèle.
    if match_data.get("indice_maj_le") is None:
        ligne_risque = "non encore évalué (aucun évènement de contexte collecté)"
    else:
        ligne_risque = f"{match_data['indice_risque']} (0 = lisible, 100 = très incertain)"

    prompt = f"""Tu es un analyste football neutre et rigoureux.
Tu analyses un match de la COUPE DU MONDE 2026, organisée aux États-Unis, au
Canada et au Mexique. Ce n'est PAS la Coupe du Monde 2022 au Qatar.

RÈGLES STRICTES — à respecter impérativement :
- Fonde ton analyse UNIQUEMENT sur les données ci-dessous.
- N'invente JAMAIS un joueur, une blessure, un transfert, un résultat passé,
  un pays hôte ou un évènement qui ne figure pas explicitement dans le CONTEXTE.
- Ne mentionne un avantage du terrain QUE si la ligne « Terrain » l'indique.
- Si les données sont insuffisantes pour trancher, dis-le honnêtement et
  baisse ta confiance, plutôt que d'inventer une raison.

MATCH : {dom} (issue 1) contre {ext} (issue 2)
Terrain : {note_terrain}
Classement FIFA (1 = meilleure équipe du monde) : {dom} = {rang_dom}, {ext} = {rang_ext}
>>> {ligne_favori}
Indice de risque : {ligne_risque}

CONTEXTE D'AVANT-MATCH (seule source factuelle autorisée) :
{lignes_contexte}

Réponds EXACTEMENT dans ce format, en 4 lignes, sans aucun autre texte :
ISSUE: 1, N ou 2 (1 = victoire {dom}, N = match nul, 2 = victoire {ext})
SCORE: score exact probable, COHÉRENT avec l'ISSUE (ex. ISSUE N => score de nul)
CONFIANCE: nombre entre 0 et 1 (basse si données insuffisantes ou risque élevé)
JUSTIFICATION: une phrase en français, fondée UNIQUEMENT sur les données ci-dessus"""

    reponse = requests.post(
        f"{config.OLLAMA_URL}/api/generate",
        json={"model": config.OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=config.OLLAMA_TIMEOUT,
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

    valeur_issue = issue.group(1).upper() if issue else "N"
    valeur_score = score.group(1).replace(" ", "") if score else None

    # Cohérence issue/score : le score chiffré fait foi. llama3 annonce parfois
    # "Victoire X" avec un score nul (ex. 1-1) — on réaligne l'issue sur le
    # score et on dégrade la confiance pour signaler l'incohérence d'origine.
    if valeur_score:
        d, _, e = valeur_score.partition("-")
        issue_du_score = "1" if int(d) > int(e) else "2" if int(d) < int(e) else "N"
        if issue_du_score != valeur_issue:
            valeur_issue = issue_du_score
            valeur_confiance = min(valeur_confiance, 0.45)

    return {
        "issue": valeur_issue,
        "score_estime": valeur_score,
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
