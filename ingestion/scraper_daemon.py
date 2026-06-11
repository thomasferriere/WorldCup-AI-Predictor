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
    python-dotenv   # chargement de la clé RapidAPI depuis .env
Dépendances prévues pour la suite :
    feedparser      # parsing RSS/Atom
    psycopg[binary] # driver PostgreSQL 3.x
"""

from __future__ import annotations

import datetime
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any

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

# Flux RSS d'actualité sportive à parser (séparés par des virgules)
RSS_FEEDS = [
    u for u in os.environ.get(
        "ORACLE_RSS_FEEDS",
        "https://www.example-foot.com/rss/cdm2026,"
        "https://www.example-mercato.com/rss/selections",
    ).split(",") if u
]

# Connexion PostgreSQL (l'utilisateur scraper n'a que INSERT/SELECT, cf. schema.sql)
PG_DSN = os.environ.get(
    "ORACLE_PG_DSN",
    "host=127.0.0.1 dbname=oracle2026 user=scraper",
)

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


def recuperer_contexte_match(match: dict[str, Any]) -> list[EvenementContexte]:
    """Transforme un match renvoyé par l'API en évènements de contexte.

    ⚠ QUOTA : avec 100 requêtes/mois, AUCUN appel API supplémentaire par match
    (pas de /lineups, /injuries ni /odds ici). On exploite uniquement les champs
    déjà présents dans la réponse quotidienne ; le contexte fin (blessures,
    rumeurs) vient du volet RSS, qui est gratuit et illimité.

    À implémenter :
      - faire correspondre le match API au `matchs.id` local
        (noms/codes des deux équipes + date de coup d'envoi) ;
      - normaliser les champs disponibles (statut, score, horaire) en
        EvenementContexte si pertinents.
    """
    raise NotImplementedError


# -----------------------------------------------------------------------------
# ÉTAPE 2 — Flux RSS d'actualité
# -----------------------------------------------------------------------------

def parser_flux_rss(urls: list[str]) -> list[EvenementContexte]:
    """Parcourt les flux RSS et transforme les entrées pertinentes en évènements.

    À implémenter :
      - `feedparser.parse(url)` pour chaque flux (gérer bozo/erreurs réseau
        sans interrompre les autres flux) ;
      - ne garder que les entrées publiées depuis le dernier passage
        (comparer entry.published à un curseur persistant, ex. table technique
        ou fichier d'état) ;
      - détecter le match et l'équipe concernés (mots-clés : noms d'équipes,
        joueurs connus de la table `equipes`) ;
      - classifier l'entrée -> type_evenement ('RUMEUR', 'METEO', 'SUSPENSION'…)
        et estimer impact_score + fiabilite_source selon le média ;
      - source = URL de l'article (sert de clé naturelle anti-doublon).
    """
    raise NotImplementedError


# -----------------------------------------------------------------------------
# ÉTAPE 3 — Préparation et insertion PostgreSQL
# -----------------------------------------------------------------------------

def connecter_postgres() -> Any:
    """Ouvre la connexion PostgreSQL (psycopg 3, autocommit désactivé).

    À implémenter :
      - `psycopg.connect(PG_DSN)` ;
      - SET TIME ZONE 'Asia/Dubai' sur la session (cohérence GMT+4) ;
      - un échec de connexion = sortie code 2 (visible de NRPE/cron).
    """
    raise NotImplementedError


def inserer_evenements(conn: Any, evenements: list[EvenementContexte]) -> int:
    """UPSERT idempotent des évènements dans `contexte_actu`.

    La contrainte uq_contexte_naturel (match_id, type_evenement, joueur_nom,
    source) garantit qu'une réexécution cron ne duplique jamais une donnée —
    et donc ne fait pas gonfler artificiellement l'indice de risque.

    Requête prévue :

        INSERT INTO contexte_actu
              (match_id, equipe_id, type_evenement, joueur_nom,
               importance_joueur, impact_score, description, source,
               fiabilite_source, detecte_le)
        VALUES (%(match_id)s, %(equipe_id)s, %(type_evenement)s, %(joueur_nom)s,
                %(importance_joueur)s, %(impact_score)s, %(description)s,
                %(source)s, %(fiabilite_source)s, %(detecte_le)s)
        ON CONFLICT ON CONSTRAINT uq_contexte_naturel DO NOTHING;

    NB : chaque INSERT réellement appliqué déclenche fn_recalc_indice_risque()
    côté base — aucune logique métier à dupliquer ici.

    Retourne le nombre de lignes effectivement insérées (cur.rowcount cumulé),
    journalisé pour recouper avec le check NRPE de fraîcheur.
    """
    raise NotImplementedError


# -----------------------------------------------------------------------------
# Orchestration d'un cycle complet
# -----------------------------------------------------------------------------

def executer_cycle() -> int:
    """Un cycle d'ingestion : API stats + RSS -> contexte_actu. Retourne le code de sortie."""
    evenements: list[EvenementContexte] = []

    # 1. API de statistiques sportives — UN appel par jour maximum (quota 100/mois)
    if api_deja_appelee_aujourdhui():
        logger.info("Appel API quotidien déjà consommé — volet API sauté (quota préservé)")
    else:
        try:
            matchs = recuperer_matchs_du_jour()
            marquer_api_appelee()
            for match in matchs:
                evenements.extend(recuperer_contexte_match(match))
        except NotImplementedError:
            logger.warning("Volet contexte API non implémenté — matchs récupérés mais non exploités")
        except Exception:
            # Un volet en panne ne doit pas empêcher l'autre de produire des données
            logger.exception("Échec du volet API stats")

    # 2. Flux RSS
    try:
        evenements.extend(parser_flux_rss(RSS_FEEDS))
    except NotImplementedError:
        logger.warning("Volet RSS non implémenté — ignoré pour ce cycle")
    except Exception:
        logger.exception("Échec du volet RSS")

    if not evenements:
        logger.info("Cycle terminé : aucun évènement à insérer")
        return 0

    # 3. Insertion idempotente
    conn = connecter_postgres()
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
