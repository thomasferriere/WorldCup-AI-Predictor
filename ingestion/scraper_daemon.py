#!/usr/bin/env python3
"""
scraper_daemon.py — Oracle 2026 : daemon d'ingestion (SQUELETTE)
=================================================================

Rôle : collecter le contexte d'avant-match pendant la fenêtre nocturne
(01:00–09:00 GMT+4) et l'écrire de façon IDEMPOTENTE dans PostgreSQL
(table `contexte_actu`), ce qui déclenche les triggers métier
(recalcul de l'indice de risque, obsolescence des pronostics).

Trois sources, trois étapes :
  1. API RapidAPI "Free API Live Football Data" -> matchs du jour
     ⚠ quota plan gratuit : 100 requêtes/MOIS -> 1 seul appel par jour,
     verrouillé par un fichier d'état (.derniere_date_api).
     Clé secrète dans .env (jamais commitée), chargée via python-dotenv.
  2. Flux RSS d'actualité           -> rumeurs, conférences, news de dernière minute
  3. Préparation + UPSERT PostgreSQL (clé naturelle anti-doublon, cf. schema.sql)

Lancement (cron, cf. README §8) :
    /usr/bin/python3 scraper_daemon.py            # un cycle puis sortie
    /usr/bin/python3 scraper_daemon.py --force    # ignore la fenêtre nocturne (debug)

Supervision : le process est surveillé par NRPE (check_proc_daemon) et la
fraîcheur des données insérées par check_freshness — voir monitoring/nrpe.cfg.
Toute sortie va sur stdout/stderr, redirigée vers les logs par cron.

Dépendances (requirements.txt) :
    requests        # appels API REST
    python-dotenv   # chargement de la clé RapidAPI et des accès DB depuis .env
    feedparser      # parsing des flux RSS d'actualité
    psycopg2-binary # driver PostgreSQL
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any

import feedparser
import psycopg2
import requests
from dotenv import load_dotenv

# Charge .env (racine du projet) : la clé RapidAPI n'est JAMAIS dans le code
load_dotenv()

# -----------------------------------------------------------------------------
# Configuration — tout vient de l'environnement, rien en dur dans le code.
# -----------------------------------------------------------------------------

# Fuseau serveur : GMT+4 (cf. README — jamais d'heure "naïve")
TZ_SERVEUR = datetime.timezone(datetime.timedelta(hours=4), name="GMT+4")

# Fenêtre d'ingestion nocturne, en heure serveur
FENETRE_DEBUT = 1   # 01:00
FENETRE_FIN = 9     # 09:00 (exclu)

# QUOTA RapidAPI plan gratuit : 100 requêtes / MOIS -> 1 seul appel API / jour.
# Le cron relance ce script toutes les 5-15 min pendant la fenêtre nocturne :
# ce fichier d'état mémorise la date du dernier appel pour ne pas le répéter.
FICHIER_ETAT_API = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".derniere_date_api")

# Cache local des matchs du jour : l'appel API n'a lieu qu'une fois par jour,
# mais le cron repasse toutes les 5-15 min — les passages suivants relisent ce
# fichier pour pouvoir quand même réconcilier les nouveaux articles RSS.
FICHIER_MATCHS_JOUR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".matchs_du_jour.json")

# Connexion PostgreSQL : variables DB_* chargées depuis .env (cf. .env.example).
# L'utilisateur scraper n'a besoin que de INSERT/SELECT (cf. schema.sql).

logger = logging.getLogger("oracle2026.scraper")


# -----------------------------------------------------------------------------
# Modèle interne : un évènement de contexte, prêt à être inséré.
# Reflète exactement les colonnes de `contexte_actu` (database/schema.sql).
# -----------------------------------------------------------------------------

@dataclass
class EvenementContexte:
    match_id: int
    type_evenement: str           # valeur de l'ENUM type_evenement (ex. 'RUMEUR')
    equipe_id: int | None = None
    joueur_nom: str | None = None
    importance_joueur: int = 5    # 1 = remplaçant, 10 = star indiscutable
    impact_score: float = 0.0     # impact brut estimé [0-100], pondéré ensuite par le trigger
    description: str | None = None
    source: str | None = None     # URL / nom du média (fait partie de la clé naturelle)
    fiabilite_source: int = 5     # 1-10
    detecte_le: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(TZ_SERVEUR))


# -----------------------------------------------------------------------------
# ÉTAPE 0 — Garde-fou : ne travailler que pendant la fenêtre nocturne.
# -----------------------------------------------------------------------------

def dans_fenetre_nocturne(maintenant: datetime.datetime | None = None) -> bool:
    """Vrai si l'heure serveur (GMT+4) est dans la fenêtre 01:00–09:00.

    Hors fenêtre il n'y a quasiment pas de données nouvelles côté Amériques ;
    on sort immédiatement pour économiser les quotas API et éviter le ban IP.
    """
    heure = (maintenant or datetime.datetime.now(TZ_SERVEUR)).astimezone(TZ_SERVEUR).hour
    return FENETRE_DEBUT <= heure < FENETRE_FIN


def api_deja_appelee_aujourdhui() -> bool:
    """Garde-fou quota : vrai si l'appel API quotidien a déjà été consommé.

    Plan gratuit RapidAPI = 100 requêtes/mois ; le cron repasse toutes les
    5-15 min, donc sans ce verrou la nuit entière viderait le quota en 2 jours.
    """
    try:
        with open(FICHIER_ETAT_API, encoding="utf-8") as f:
            return f.read().strip() == datetime.datetime.now(TZ_SERVEUR).strftime("%Y%m%d")
    except FileNotFoundError:
        return False


def marquer_api_appelee() -> None:
    """Enregistre que l'appel API du jour a été effectué."""
    with open(FICHIER_ETAT_API, "w", encoding="utf-8") as f:
        f.write(datetime.datetime.now(TZ_SERVEUR).strftime("%Y%m%d"))


# -----------------------------------------------------------------------------
# ÉTAPE 1 — API de statistiques sportives (RapidAPI "Free API Live Football Data")
# Quota plan gratuit : 100 requêtes/mois -> UN SEUL appel par jour, protégé par
# api_deja_appelee_aujourdhui() dans executer_cycle().
# -----------------------------------------------------------------------------

def recuperer_matchs_du_jour():
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("❌ Clé API introuvable. Remplissez le fichier .env")
        return []

    url = "https://free-api-live-football-data.p.rapidapi.com/soccer-fixtures-by-date"
    date_aujourdhui = datetime.datetime.now().strftime("%Y%m%d")
    querystring = {"date": date_aujourdhui}
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "free-api-live-football-data.p.rapidapi.com"
    }

    try:
        print(f"[{datetime.datetime.now()}] Appel API pour la date : {date_aujourdhui}...")
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()

        matchs_cdm = []
        if "results" in data:
            for match in data["results"]:
                if "World Cup" in match.get("league_name", "") or match.get("league_id") == 1:
                    matchs_cdm.append(match)

        print(f"✅ Succès : {len(matchs_cdm)} match(s) de Coupe du Monde trouvé(s).")
        return matchs_cdm

    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de l'appel API : {e}")
        return []


def sauvegarder_matchs_caches(matchs: list[dict[str, Any]]) -> None:
    """Mémorise les matchs du jour pour les passages cron suivants (quota oblige)."""
    with open(FICHIER_MATCHS_JOUR, "w", encoding="utf-8") as f:
        json.dump(matchs, f, ensure_ascii=False)


def charger_matchs_caches() -> list[dict[str, Any]]:
    """Relit les matchs du jour mémorisés par le premier passage de la nuit."""
    try:
        with open(FICHIER_MATCHS_JOUR, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# -----------------------------------------------------------------------------
# ÉTAPE 2 — Flux RSS d'actualité
# -----------------------------------------------------------------------------

# Mots déclencheurs : un article n'est retenu que s'il évoque un risque sportif…
MOTS_CLES_ALERTE = ["blessure", "forfait", "tension", "polémique"]

# …ou un pays participant à la CDM 2026 (extrait — à synchroniser avec la
# table `equipes` une fois la liste des 48 qualifiés chargée en base).
PAYS_PARTICIPANTS = [
    "France", "Brésil", "Argentine", "Canada", "Mexique", "États-Unis",
    "Angleterre", "Espagne", "Allemagne", "Portugal", "Pays-Bas", "Belgique",
    "Croatie", "Maroc", "Sénégal", "Japon", "Uruguay", "Colombie", "Équateur",
]


def parser_flux_rss() -> list[dict[str, Any]]:
    """Récupère gratuitement les rumeurs/actualités via les flux RSS sportifs.

    Pour chaque article : extraction du titre et du résumé, puis filtre textuel
    basique — on ne garde que les articles mentionnant un mot d'alerte
    (MOTS_CLES_ALERTE) ou un pays participant (PAYS_PARTICIPANTS).

    Retourne une liste de dictionnaires :
        {titre, resume, lien, source, publie_le, mots_cles}
    `mots_cles` liste les déclencheurs trouvés — utile ensuite pour classifier
    l'évènement (RUMEUR, BLESSURE…) et le rattacher à un match via les noms
    d'équipes. `lien` servira de clé naturelle anti-doublon (colonne `source`
    de contexte_actu).
    """
    flux_rss = [
        # L'Équipe football — l'ancien chemin /rss/actu_rss_Football.xml renvoie 404
        "https://dwh.lequipe.fr/api/edito/rss?path=/Football/",
        "https://rmcsport.bfmtv.com/rss/football/coupe-du-monde/",
    ]

    declencheurs = MOTS_CLES_ALERTE + PAYS_PARTICIPANTS
    articles_pertinents: list[dict[str, Any]] = []

    for url in flux_rss:
        try:
            flux = feedparser.parse(url)
        except Exception:
            # Un flux en panne ne doit pas bloquer les autres
            logger.exception("Flux RSS illisible : %s", url)
            continue

        if getattr(flux, "bozo", False) and not flux.entries:
            logger.warning("Flux RSS invalide ou inaccessible : %s", url)
            continue

        for entree in flux.entries:
            titre = (entree.get("title") or "").strip()
            # Le résumé RSS contient souvent du HTML : on le retire pour
            # obtenir du texte propre (filtre + futur prompt LLM).
            resume = re.sub(r"<[^>]+>", " ", entree.get("summary") or "")
            resume = re.sub(r"\s+", " ", resume).strip()

            texte = f"{titre} {resume}".lower()
            mots_trouves = [m for m in declencheurs if m.lower() in texte]
            if not mots_trouves:
                continue

            articles_pertinents.append({
                "titre": titre,
                "resume": resume,
                "lien": entree.get("link", ""),
                "source": url,
                "publie_le": entree.get("published") or entree.get("updated") or "",
                "mots_cles": mots_trouves,
            })

        logger.info("Flux %s : %d entrée(s) lue(s)", url, len(flux.entries))

    logger.info("RSS : %d article(s) pertinent(s) après filtrage", len(articles_pertinents))
    return articles_pertinents


# -----------------------------------------------------------------------------
# ÉTAPE 3 — Réconciliation matchs API <-> articles RSS
# -----------------------------------------------------------------------------

# L'API renvoie les noms d'équipes en anglais, les flux RSS sont en français :
# table de correspondance pour que le rapprochement fonctionne dans les 2 sens.
NOMS_EQUIPES_FR = {
    "france": "France", "brazil": "Brésil", "argentina": "Argentine",
    "canada": "Canada", "mexico": "Mexique", "usa": "États-Unis",
    "united states": "États-Unis", "england": "Angleterre", "spain": "Espagne",
    "germany": "Allemagne", "portugal": "Portugal", "netherlands": "Pays-Bas",
    "belgium": "Belgique", "croatia": "Croatie", "morocco": "Maroc",
    "senegal": "Sénégal", "japan": "Japon", "uruguay": "Uruguay",
    "colombia": "Colombie", "ecuador": "Équateur",
}


def _noms_equipes(match: dict[str, Any]) -> set[str]:
    """Noms (en français, minuscules) des deux équipes d'un match API.

    Tolérant aux variantes de schéma de l'API (home_name / home.name / home_team).
    """
    noms = set()
    for cote in ("home", "away"):
        nom = (match.get(f"{cote}_name")
               or (match.get(cote) or {}).get("name")
               or match.get(f"{cote}_team")
               or "")
        if nom:
            noms.add(NOMS_EQUIPES_FR.get(nom.strip().lower(), nom.strip()).lower())
    return noms


def reconcilier_donnees(matchs_api: list[dict[str, Any]],
                        articles_rss: list[dict[str, Any]]) -> list[EvenementContexte]:
    """Rattache chaque article RSS aux matchs dont une équipe est citée.

    Pour chaque match du jour : si le nom de l'équipe domicile ou extérieur
    figure dans les `mots_cles` d'un article, l'article devient un
    EvenementContexte lié à ce match. Catégorisation basique :
      - "forfait"  -> BLESSURE_JOUEUR_MAJEUR (joueur out, impact fort)
      - "blessure" -> BLESSURE_JOUEUR_MINEUR (gravité inconnue à ce stade)
      - sinon      -> RUMEUR
    (valeurs de l'ENUM type_evenement du schéma — "BLESSURE" seul n'existe pas)
    L'impact fin est ensuite pondéré par le trigger fn_recalc_indice_risque.
    """
    evenements: list[EvenementContexte] = []

    for match in matchs_api:
        match_id = match.get("id") or match.get("match_id") or match.get("fixture_id")
        if not match_id:
            continue
        equipes = _noms_equipes(match)
        if not equipes:
            continue

        for article in articles_rss:
            mots = {m.lower() for m in article.get("mots_cles", [])}
            if not equipes & mots:
                continue

            if "forfait" in mots:
                type_ev, impact = "BLESSURE_JOUEUR_MAJEUR", 40.0
            elif "blessure" in mots:
                type_ev, impact = "BLESSURE_JOUEUR_MINEUR", 25.0
            else:
                type_ev, impact = "RUMEUR", 10.0

            evenements.append(EvenementContexte(
                match_id=int(match_id),
                type_evenement=type_ev,
                joueur_nom=None,            # TODO : extraction du nom du joueur (NLP léger)
                impact_score=impact,
                description=f"{article['titre']} — {article['resume'][:400]}",
                # le lien de l'article sert de clé naturelle anti-doublon
                source=(article.get("lien") or article.get("source", ""))[:160],
                fiabilite_source=7,         # médias sportifs mainstream
            ))

    logger.info("Réconciliation : %d évènement(s) issus de %d match(s) x %d article(s)",
                len(evenements), len(matchs_api), len(articles_rss))
    return evenements


# -----------------------------------------------------------------------------
# ÉTAPE 4 — Insertion PostgreSQL
# -----------------------------------------------------------------------------

def connecter_postgres() -> Any:
    """Ouvre la connexion PostgreSQL à partir des variables DB_* du .env."""
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "oracle2026"),
        user=os.getenv("DB_USER", "scraper"),
        password=os.getenv("DB_PASS", ""),
        host=os.getenv("DB_HOST", "127.0.0.1"),
        connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Asia/Dubai'")   # cohérence GMT+4 (cf. schema.sql)
    return conn


# Insertion idempotente dans contexte_actu :
#  - WHERE EXISTS  : ignore les évènements dont le match n'est pas (encore) en
#    base, plutôt que de faire échouer la transaction sur une violation de FK ;
#  - AND NOT EXISTS : déduplication NULL-safe. La contrainte uq_contexte_naturel
#    inclut joueur_nom, or il est NULL pour les articles RSS — et en SQL deux
#    NULL ne sont jamais "égaux", donc ON CONFLICT seul ne suffirait pas ;
#  - ON CONFLICT DO NOTHING : filet de sécurité contre les insertions
#    concurrentes (deux exécutions cron qui se chevauchent).
SQL_INSERT_EVENEMENT = """
    INSERT INTO contexte_actu
          (match_id, equipe_id, type_evenement, joueur_nom,
           importance_joueur, impact_score, description, source,
           fiabilite_source, detecte_le)
    SELECT %(match_id)s, %(equipe_id)s, %(type_evenement)s, %(joueur_nom)s,
           %(importance_joueur)s, %(impact_score)s, %(description)s,
           %(source)s, %(fiabilite_source)s, %(detecte_le)s
    WHERE EXISTS (SELECT 1 FROM matchs m WHERE m.id = %(match_id)s)
      AND NOT EXISTS (
            SELECT 1 FROM contexte_actu c
            WHERE c.match_id = %(match_id)s
              AND c.type_evenement = %(type_evenement)s
              AND c.joueur_nom IS NOT DISTINCT FROM %(joueur_nom)s
              AND c.source     IS NOT DISTINCT FROM %(source)s
      )
    ON CONFLICT ON CONSTRAINT uq_contexte_naturel DO NOTHING;
"""


def inserer_evenements(conn: Any, evenements: list[EvenementContexte]) -> int:
    """Écrit les évènements dans `contexte_actu` (idempotent, cf. SQL ci-dessus).

    Chaque ligne réellement insérée déclenche fn_recalc_indice_risque() côté
    base — aucune logique métier à dupliquer ici. Retourne le nombre de lignes
    insérées, à recouper avec le check NRPE de fraîcheur.
    """
    inseres = 0
    with conn.cursor() as cur:
        for ev in evenements:
            cur.execute(SQL_INSERT_EVENEMENT, {
                "match_id": ev.match_id,
                "equipe_id": ev.equipe_id,
                "type_evenement": ev.type_evenement,
                "joueur_nom": ev.joueur_nom,
                "importance_joueur": ev.importance_joueur,
                "impact_score": ev.impact_score,
                "description": ev.description,
                "source": ev.source,
                "fiabilite_source": ev.fiabilite_source,
                "detecte_le": ev.detecte_le,
            })
            inseres += cur.rowcount
    return inseres


# -----------------------------------------------------------------------------
# Orchestration d'un cycle complet
# -----------------------------------------------------------------------------

def executer_cycle() -> int:
    """Un cycle d'ingestion : API + RSS -> réconciliation -> contexte_actu."""

    # 1. Matchs du jour — UN appel API par jour maximum (quota 100/mois),
    #    puis cache local pour les passages cron suivants de la même nuit.
    matchs_api: list[dict[str, Any]] = []
    if api_deja_appelee_aujourdhui():
        matchs_api = charger_matchs_caches()
        logger.info("Appel API quotidien déjà consommé — %d match(s) relus depuis le cache",
                    len(matchs_api))
    else:
        try:
            matchs_api = recuperer_matchs_du_jour()
            marquer_api_appelee()
            sauvegarder_matchs_caches(matchs_api)
        except Exception:
            # Un volet en panne ne doit pas empêcher l'autre de produire des données
            logger.exception("Échec du volet API stats")

    # 2. Flux RSS — gratuit et illimité : c'est lui qui porte le contexte fin
    articles: list[dict[str, Any]] = []
    try:
        articles = parser_flux_rss()
    except Exception:
        logger.exception("Échec du volet RSS")

    # 3. Réconciliation : articles RSS rattachés aux matchs du jour
    evenements = reconcilier_donnees(matchs_api, articles)
    if not evenements:
        logger.info("Cycle terminé : aucun évènement à insérer")
        return 0

    # 4. Insertion idempotente
    try:
        conn = connecter_postgres()
    except Exception:
        logger.exception("Connexion PostgreSQL impossible (variables DB_* du .env ?)")
        return 2
    try:
        inseres = inserer_evenements(conn, evenements)
        conn.commit()
        logger.info("Cycle terminé : %d collecté(s), %d inséré(s) (doublons ignorés)",
                    len(evenements), inseres)
        return 0
    except Exception:
        conn.rollback()
        logger.exception("Échec de l'insertion — transaction annulée")
        return 2
    finally:
        conn.close()


def main(argv: list[str]) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s : %(message)s",
    )

    if "--force" not in argv and not dans_fenetre_nocturne():
        logger.info("Hors fenêtre nocturne (01h-09h GMT+4) — rien à faire, sortie propre")
        return 0

    return executer_cycle()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
