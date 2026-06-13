#!/usr/bin/env python3
"""
scraper_daemon.py — Oracle 2026 : daemon d'ingestion (SQUELETTE)
=================================================================

Rôle : collecter le contexte d'avant-match et l'écrire de façon IDEMPOTENTE
dans PostgreSQL (table `contexte_actu`), ce qui déclenche les triggers métier
(recalcul de l'indice de risque, obsolescence des pronostics). Le cycle tourne
24h/24 (planifié par APScheduler dans serveur_api.py) : le RSS est collecté à
chaque passage, l'appel API est borné à quelques fois par jour (quota).

Trois sources, trois étapes :
  1. API RapidAPI "Free API Live Football Data" -> matchs du jour
     ⚠ quota plan gratuit : 100 requêtes/MOIS -> max 3 appels/jour espacés,
     comptés dans un fichier d'état (.derniere_date_api).
     Clé secrète dans .env (jamais commitée), chargée via python-dotenv.
  2. Flux RSS d'actualité           -> rumeurs, conférences, news de dernière minute
  3. Préparation + UPSERT PostgreSQL (clé naturelle anti-doublon, cf. schema.sql)

Lancement :
    /usr/bin/python3 scraper_daemon.py            # un cycle puis sortie
    /usr/bin/python3 scraper_daemon.py --force    # force un appel API immédiat
                                                  # (ignore l'espacement, pas le plafond/jour)

Supervision : le process est surveillé par NRPE (check_proc_daemon) et la
fraîcheur des données insérées par check_freshness — voir monitoring/nrpe.cfg.

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

# QUOTA RapidAPI plan gratuit : 100 requêtes / MOIS. On vise jusqu'à 3 appels
# API par jour (3 × 30 = 90 < 100), espacés d'au moins quelques heures pour
# couvrir matin / midi / soir et rafraîchir les scores en cours de journée.
# Le RSS, lui, est gratuit et illimité : le cycle complet tourne 24h/24.
MAX_APPELS_API_JOUR = 3
INTERVALLE_MIN_API_H = 5   # heures minimum entre deux appels API
# Fichier d'état du quota : {"date": "AAAAMMJJ", "count": n, "dernier": ISO}
FICHIER_ETAT_API = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".derniere_date_api")

# Cache local des matchs du jour : l'appel API n'a lieu qu'une fois par jour,
# mais le cron repasse toutes les 5-15 min — les passages suivants relisent ce
# fichier pour pouvoir quand même réconcilier les nouveaux articles RSS.
FICHIER_MATCHS_JOUR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".matchs_du_jour.json")

# Réponse API brute, sauvegardée telle quelle : permet de re-parser/déboguer
# sans reconsommer une requête du quota mensuel.
FICHIER_REPONSE_API = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".derniere_reponse_api.json")

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
# ÉTAPE 0 — Garde-fou quota : étaler jusqu'à 3 appels API/jour.
# -----------------------------------------------------------------------------

def _lire_etat_api() -> dict:
    try:
        with open(FICHIER_ETAT_API, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def appel_api_autorise() -> bool:
    """Vrai si un appel API est permis maintenant : moins de MAX_APPELS_API_JOUR
    aujourd'hui ET au moins INTERVALLE_MIN_API_H depuis le dernier appel.

    Le cycle tourne toutes les heures ; ce verrou évite de vider le quota
    mensuel tout en laissant 2-3 rafraîchissements de scores répartis sur la
    journée (le contexte RSS, lui, est collecté à chaque cycle, gratuitement).
    """
    etat = _lire_etat_api()
    aujourdhui = datetime.datetime.now(TZ_SERVEUR).strftime("%Y%m%d")
    if etat.get("date") != aujourdhui:
        return True                                    # nouveau jour : compteur remis à zéro
    if etat.get("count", 0) >= MAX_APPELS_API_JOUR:
        return False                                   # plafond/jour : jamais dépassé, même --force
    if os.environ.get("ORACLE_FORCE_API") == "1":
        return True                                    # --force : ignore l'espacement
    dernier = etat.get("dernier")
    if dernier:
        ecart = datetime.datetime.now(TZ_SERVEUR) - datetime.datetime.fromisoformat(dernier)
        if ecart < datetime.timedelta(hours=INTERVALLE_MIN_API_H):
            return False
    return True


def marquer_api_appelee() -> None:
    """Incrémente le compteur d'appels du jour et horodate le dernier appel."""
    etat = _lire_etat_api()
    aujourdhui = datetime.datetime.now(TZ_SERVEUR).strftime("%Y%m%d")
    if etat.get("date") != aujourdhui:
        etat = {"date": aujourdhui, "count": 0}
    etat["count"] = etat.get("count", 0) + 1
    etat["dernier"] = datetime.datetime.now(TZ_SERVEUR).isoformat()
    with open(FICHIER_ETAT_API, "w", encoding="utf-8") as f:
        json.dump(etat, f)


# -----------------------------------------------------------------------------
# ÉTAPE 1 — API de statistiques sportives (RapidAPI "Free API Live Football Data")
# Quota plan gratuit : 100 requêtes/mois -> max 3 appels/jour espacés, protégé
# par appel_api_autorise() dans executer_cycle().
# -----------------------------------------------------------------------------

# --- SOURCE PRINCIPALE : API publique ESPN (gratuite, sans clé, sans quota) ---
# Endpoint scoreboard de la compétition "fifa.world" : tous les matchs renvoyés
# sont des matchs de Coupe du Monde, déjà filtrés par ESPN.
URL_ESPN = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard"


def _normaliser_match_espn(event: dict[str, Any]) -> dict[str, Any] | None:
    """Convertit un évènement ESPN vers la structure interne commune.

    Structure de sortie identique à celle de RapidAPI (home/away/status), pour
    que inserer_matchs_en_base() et reconcilier_donnees() fonctionnent tels quels.
    """
    try:
        comp = event["competitions"][0]
        equipes = {c["homeAway"]: c for c in comp["competitors"]}
        dom, ext = equipes["home"], equipes["away"]
    except (KeyError, IndexError):
        return None

    etat = (event.get("status") or {}).get("type") or {}
    state = etat.get("name", "")           # ex. STATUS_SCHEDULED / IN_PROGRESS / FULL_TIME
    commence = etat.get("state") in ("in", "post")
    fini = bool(etat.get("completed"))
    annule = "CANCEL" in state or "POSTPONE" in state

    def _score(c: dict) -> int | None:
        # Score pertinent seulement si le match a commencé
        if not commence:
            return None
        try:
            return int(c.get("score"))
        except (TypeError, ValueError):
            return None

    return {
        "id": f"espn-{event.get('id')}",
        "leagueId": LIGUE_ESPN_CDM,
        "home": {"name": dom["team"]["displayName"], "score": _score(dom)},
        "away": {"name": ext["team"]["displayName"], "score": _score(ext)},
        "status": {
            "utcTime": event["date"],       # ex. 2026-06-13T22:00Z (parsé en aval)
            "started": commence and not fini,
            "finished": fini,
            "cancelled": annule,
        },
    }


def recuperer_matchs_espn() -> list[dict[str, Any]]:
    """Matchs CDM via ESPN : aujourd'hui + demain (couvre les matchs de nuit).

    Gratuit et illimité — aucune gestion de quota. Lève en cas d'échec réseau
    pour que executer_cycle bascule sur le secours RapidAPI.
    """
    # -2 à +1 : finalise les scores des matchs récents (robuste même si le Mac
    # a dormi un jour ou deux) et anticipe les matchs de la nuit à venir.
    jours = [
        (datetime.datetime.now(TZ_SERVEUR) + datetime.timedelta(days=d)).strftime("%Y%m%d")
        for d in (-2, -1, 0, 1)
    ]
    matchs: dict[str, dict[str, Any]] = {}
    for jour in jours:
        reponse = requests.get(URL_ESPN, params={"dates": jour}, timeout=15)
        reponse.raise_for_status()
        for event in reponse.json().get("events", []):
            m = _normaliser_match_espn(event)
            if m:
                matchs[m["id"]] = m            # dédup par id (chevauchement des 2 jours)
    logger.info("ESPN : %d match(s) CDM récupérés (%s)", len(matchs), " + ".join(jours))
    return list(matchs.values())


# --- SOURCE DE SECOURS : RapidAPI (quota 100/mois) -------------------------
def recuperer_matchs_du_jour():
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("❌ Clé API introuvable. Remplissez le fichier .env")
        return []

    url = "https://free-api-live-football-data.p.rapidapi.com/football-get-matches-by-date"
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

        # Sauvegarde brute : re-parsing possible sans reconsommer le quota
        with open(FICHIER_REPONSE_API, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        # Structure réelle observée : {"status": ..., "response": {"matches": [...]}}
        # avec home/away = {"id", "name", "score"}. Filtre sur les ligues CDM.
        tous = (data.get("response") or {}).get("matches") or []
        matchs_cdm = [m for m in tous if m.get("leagueId") in LIGUES_CDM]

        print(f"✅ Succès : {len(matchs_cdm)} match(s) de Coupe du Monde trouvé(s) "
              f"(sur {len(tous)} matchs renvoyés).")
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
# …ou un pays participant : la liste est dérivée de NOMS_EQUIPES_FR (défini plus
# bas), calculée à l'exécution dans parser_flux_rss() pour éviter l'ordre d'import.


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

    declencheurs = MOTS_CLES_ALERTE + sorted(set(NOMS_EQUIPES_FR.values()))
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

# Sentinelle de ligue pour les matchs venant d'ESPN (endpoint déjà filtré sur
# la Coupe du Monde : tous les matchs renvoyés SONT des matchs CDM). Les ID
# numériques 894790/914609 restent ceux de RapidAPI (source de secours).
LIGUE_ESPN_CDM = "espn-fifa-world"
LIGUES_CDM = {LIGUE_ESPN_CDM, 894790, 914609}

# Les API renvoient les noms d'équipes en anglais, les flux RSS sont en
# français : table de correspondance pour le rapprochement dans les 2 sens.
NOMS_EQUIPES_FR = {
    "france": "France", "brazil": "Brésil", "argentina": "Argentine",
    "canada": "Canada", "mexico": "Mexique", "usa": "États-Unis",
    "united states": "États-Unis", "england": "Angleterre", "spain": "Espagne",
    "germany": "Allemagne", "portugal": "Portugal", "netherlands": "Pays-Bas",
    "belgium": "Belgique", "croatia": "Croatie", "morocco": "Maroc",
    "senegal": "Sénégal", "japan": "Japon", "uruguay": "Uruguay",
    "colombia": "Colombie", "ecuador": "Équateur",
    "south africa": "Afrique du Sud", "south korea": "Corée du Sud",
    "czechia": "Tchéquie", "austria": "Autriche", "nigeria": "Nigéria",
    "guatemala": "Guatemala",
    # Nations supplémentaires CDM 2026 (vues côté ESPN)
    "qatar": "Qatar", "switzerland": "Suisse", "haiti": "Haïti",
    "scotland": "Écosse", "australia": "Australie", "turkey": "Turquie",
    "türkiye": "Turquie", "norway": "Norvège", "italy": "Italie",
    "denmark": "Danemark", "poland": "Pologne", "saudi arabia": "Arabie saoudite",
    "iran": "Iran", "ghana": "Ghana", "cameroon": "Cameroun", "egypt": "Égypte",
    "tunisia": "Tunisie", "peru": "Pérou", "chile": "Chili", "panama": "Panama",
    "costa rica": "Costa Rica", "paraguay": "Paraguay", "ivory coast": "Côte d'Ivoire",
    "new zealand": "Nouvelle-Zélande", "jordan": "Jordanie", "uzbekistan": "Ouzbékistan",
    "cape verde": "Cap-Vert", "curacao": "Curaçao", "algeria": "Algérie",
    "sweden": "Suède", "bosnia-herzegovina": "Bosnie-Herzégovine",
    "bosnia and herzegovina": "Bosnie-Herzégovine",
}

# Codes FIFA des équipes connues (equipes.code_fifa est NOT NULL UNIQUE).
# Repli pour les autres : 3 premières lettres du nom, sans accents.
CODES_FIFA = {
    "France": "FRA", "Brésil": "BRA", "Argentine": "ARG", "Canada": "CAN",
    "Mexique": "MEX", "États-Unis": "USA", "Angleterre": "ENG", "Espagne": "ESP",
    "Allemagne": "GER", "Portugal": "POR", "Pays-Bas": "NED", "Belgique": "BEL",
    "Croatie": "CRO", "Maroc": "MAR", "Sénégal": "SEN", "Japon": "JPN",
    "Uruguay": "URU", "Colombie": "COL", "Équateur": "ECU",
    "Afrique du Sud": "RSA", "Corée du Sud": "KOR", "Tchéquie": "CZE",
    "Autriche": "AUT", "Nigéria": "NGA", "Guatemala": "GUA",
    "Qatar": "QAT", "Suisse": "SUI", "Haïti": "HAI", "Écosse": "SCO",
    "Australie": "AUS", "Turquie": "TUR", "Norvège": "NOR", "Italie": "ITA",
    "Danemark": "DEN", "Pologne": "POL", "Arabie saoudite": "KSA", "Iran": "IRN",
    "Ghana": "GHA", "Cameroun": "CMR", "Égypte": "EGY", "Tunisie": "TUN",
    "Pérou": "PER", "Chili": "CHI", "Panama": "PAN", "Costa Rica": "CRC",
    "Paraguay": "PAR", "Côte d'Ivoire": "CIV", "Nouvelle-Zélande": "NZL",
    "Jordanie": "JOR", "Ouzbékistan": "UZB", "Cap-Vert": "CPV", "Curaçao": "CUW",
    "Algérie": "ALG", "Suède": "SWE", "Bosnie-Herzégovine": "BIH",
}

# Toutes les nations connues, en minuscules — sert à détecter les articles
# "panorama" (qui citent plusieurs sélections) et à les écarter du rattachement.
NATIONS_CONNUES = {nom.lower() for nom in NOMS_EQUIPES_FR.values()}
# Au-delà de ce nombre de nations citées, l'article est un tour d'horizon
# générique (ex. « les favoris du Mondial »), pas un avant-match ciblé.
MAX_NATIONS_ARTICLE = 3


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


def _statut_depuis_api(status: dict[str, Any]) -> str:
    """Mappe les drapeaux de statut de l'API vers l'ENUM statut_match."""
    if status.get("cancelled"):
        return "REPORTE"
    if status.get("finished"):
        return "TERMINE"
    if status.get("started"):
        return "EN_COURS"
    return "A_VENIR"


def inserer_matchs_en_base(conn: Any, matchs_api: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Insère/actualise en base les matchs CDM de l'API et lie les identifiants.

    Sans cette étape, ni la réconciliation ni le moteur IA ne peuvent voir les
    matchs : contexte_actu et pronostics_llm pointent vers matchs.id (SERIAL
    local), pas vers l'id de l'API. Chaque match CDM traité reçoit donc ici une
    clé 'db_match_id' utilisée ensuite par reconcilier_donnees().

    Les matchs hors LIGUES_CDM sont ignorés (pas de pollution de la base avec
    les ligues mineures que l'API renvoie en vrac).
    """
    traites = []
    with conn.cursor() as cur:
        for m in matchs_api:
            if m.get("leagueId") not in LIGUES_CDM:
                continue

            # 1. Équipes (upsert minimal — stats FIFA enrichies plus tard)
            ids = {}
            for cote in ("home", "away"):
                nom_api = ((m.get(cote) or {}).get("name") or "").strip()
                if not nom_api:
                    break
                nom = NOMS_EQUIPES_FR.get(nom_api.lower(), nom_api)
                code = CODES_FIFA.get(nom, nom.upper().replace("É", "E")[:3])
                cur.execute(
                    """INSERT INTO equipes (nom, code_fifa, confederation)
                       VALUES (%s, %s, 'INCONNUE') ON CONFLICT DO NOTHING""",
                    (nom, code),
                )
                cur.execute("SELECT id FROM equipes WHERE nom = %s", (nom,))
                ligne = cur.fetchone()
                if ligne:
                    ids[cote] = ligne[0]
            if len(ids) != 2 or ids["home"] == ids["away"]:
                continue

            # 2. Match (clé naturelle : les 2 équipes + coup d'envoi)
            coup_envoi = datetime.datetime.fromisoformat(
                m["status"]["utcTime"].replace("Z", "+00:00"))
            statut = _statut_depuis_api(m.get("status") or {})
            cur.execute(
                """SELECT id FROM matchs
                   WHERE equipe_dom_id = %s AND equipe_ext_id = %s AND coup_envoi = %s""",
                (ids["home"], ids["away"], coup_envoi),
            )
            ligne = cur.fetchone()
            if ligne:
                db_id = ligne[0]
                # Cycle de vie : statut + score réel dès que le match a démarré
                cur.execute(
                    """UPDATE matchs SET statut = %s, score_dom = %s, score_ext = %s
                       WHERE id = %s""",
                    (statut, (m.get("home") or {}).get("score"),
                     (m.get("away") or {}).get("score"), db_id),
                )
            else:
                cur.execute(
                    """INSERT INTO matchs (equipe_dom_id, equipe_ext_id, coup_envoi,
                                           phase, statut)
                       VALUES (%s, %s, %s, 'Phase de groupes', %s) RETURNING id""",
                    (ids["home"], ids["away"], coup_envoi, statut),
                )
                db_id = cur.fetchone()[0]

            m["db_match_id"] = db_id
            traites.append(m)

    logger.info("Matchs CDM synchronisés en base : %d", len(traites))
    return traites


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
        # db_match_id (posé par inserer_matchs_en_base) = matchs.id local, la
        # seule clé que contexte_actu accepte ; les ids API servent de repli.
        match_id = (match.get("db_match_id") or match.get("match_id")
                    or match.get("id") or match.get("fixture_id"))
        if not match_id:
            continue
        equipes = _noms_equipes(match)
        if not equipes:
            continue

        for article in articles_rss:
            mots = {m.lower() for m in article.get("mots_cles", [])}
            # L'article doit citer une équipe de CE match…
            if not equipes & mots:
                continue
            # …et ne pas être un panorama générique (trop de nations citées).
            if len(mots & NATIONS_CONNUES) > MAX_NATIONS_ARTICLE:
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

    # 1. Matchs du jour — ESPN en principal (gratuit, illimité). En cas
    #    d'échec, secours RapidAPI (borné par son quota), puis cache local.
    matchs_api: list[dict[str, Any]] = []
    try:
        matchs_api = recuperer_matchs_espn()
        sauvegarder_matchs_caches(matchs_api)
    except Exception:
        logger.exception("ESPN indisponible — bascule sur le secours RapidAPI")
        if appel_api_autorise():
            try:
                matchs_api = recuperer_matchs_du_jour()
                marquer_api_appelee()
                sauvegarder_matchs_caches(matchs_api)
            except Exception:
                logger.exception("Secours RapidAPI en échec — relecture du cache")
                matchs_api = charger_matchs_caches()
        else:
            matchs_api = charger_matchs_caches()
            logger.info("Secours RapidAPI en pause (quota) — %d match(s) relus du cache",
                        len(matchs_api))

    # 2. Synchronisation des matchs CDM en base (donne les matchs.id locaux
    #    dont la réconciliation et le moteur IA ont besoin)
    try:
        conn = connecter_postgres()
    except Exception:
        logger.exception("Connexion PostgreSQL impossible (variables DB_* du .env ?)")
        return 2
    try:
        if matchs_api:
            try:
                matchs_api = inserer_matchs_en_base(conn, matchs_api)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("Échec de la synchronisation des matchs")

        # 3. Flux RSS — gratuit et illimité : c'est lui qui porte le contexte fin
        articles: list[dict[str, Any]] = []
        try:
            articles = parser_flux_rss()
        except Exception:
            logger.exception("Échec du volet RSS")

        # 4. Réconciliation : articles RSS rattachés aux matchs du jour
        evenements = reconcilier_donnees(matchs_api, articles)
        if not evenements:
            logger.info("Cycle terminé : aucun évènement à insérer")
            return 0

        # 5. Insertion idempotente
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

    # Plus de fenêtre nocturne : le cycle tourne 24h/24 (le RSS est gratuit, et
    # l'appel API est borné par appel_api_autorise()). --force est conservé pour
    # forcer un appel API immédiat (ignore l'espacement, jamais le plafond/jour).
    if "--force" in argv:
        # marqueur lu par appel_api_autorise via une variable d'env interne
        os.environ["ORACLE_FORCE_API"] = "1"
    return executer_cycle()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
