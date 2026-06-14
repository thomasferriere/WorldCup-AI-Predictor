"""
config.py — Oracle 2026 : réglages centralisés.

Tous les « boutons » qu'on peut vouloir ajuster sans fouiller le code vivent
ici : sources, seuils, cadences, modèle d'IA. Les données de référence
(noms d'équipes, codes FIFA, classements) restent dans leurs modules — ce ne
sont pas des réglages mais des tables.
"""

import os

# --- Source des matchs : API publique ESPN (gratuite, sans clé, sans quota) ---
URL_ESPN = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard"
# Jours interrogés autour d'aujourd'hui : -2 finalise les scores des matchs
# récents (robuste même si la machine a dormi), +1 anticipe la nuit à venir.
ESPN_FENETRE_JOURS = (-2, -1, 0, 1)

# --- Contexte presse (flux RSS, gratuits et illimités) ------------------------
FLUX_RSS = [
    # L'Équipe football — l'ancien chemin /rss/actu_rss_Football.xml renvoie 404
    "https://dwh.lequipe.fr/api/edito/rss?path=/Football/",
    "https://rmcsport.bfmtv.com/rss/football/coupe-du-monde/",
]
# Un article n'est retenu que s'il évoque un risque sportif…
MOTS_CLES_ALERTE = ["blessure", "forfait", "tension", "polémique"]
# …ou une nation participante. Au-delà de ce nombre de nations citées, l'article
# est un panorama générique (« les favoris du Mondial »), pas un avant-match.
MAX_NATIONS_ARTICLE = 3

# Impact brut [0-100] d'un évènement de presse, pondéré ensuite par le trigger.
IMPACT_FORFAIT = 40.0    # forfait d'un joueur -> BLESSURE_JOUEUR_MAJEUR
IMPACT_BLESSURE = 25.0   # blessure (gravité inconnue) -> BLESSURE_JOUEUR_MINEUR
IMPACT_RUMEUR = 10.0     # rumeur / tension / polémique -> RUMEUR

# --- Modèle d'IA local (Ollama) -----------------------------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_TIMEOUT = 180     # un modèle 8B local peut être lent au premier appel

# --- Planification (APScheduler) ----------------------------------------------
CADENCE_INGESTION_MIN = 60   # cycle ESPN + RSS
CADENCE_IA_MIN = 65          # pronostics (décalé pour laisser finir l'ingestion)

# --- Fenêtre d'affichage du dashboard (onglet « Aujourd'hui ») -----------------
# Exprimée en INTERVAL PostgreSQL : on garde les matchs récents visibles le matin.
AFFICHAGE_PASSE = "12 hours"
AFFICHAGE_FUTUR = "48 hours"

# --- Signal de pari : seuils croisant confiance IA et indice de risque ---------
SIGNAL_RISQUE_FUIR = 80      # risque >= -> À FUIR
SIGNAL_CONFIANCE_MIN = 0.45  # confiance < -> À FUIR
SIGNAL_FORT_CONFIANCE = 0.70
SIGNAL_FORT_RISQUE = 40
SIGNAL_POSSIBLE_CONFIANCE = 0.55
SIGNAL_POSSIBLE_RISQUE = 65
