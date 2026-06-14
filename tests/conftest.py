"""Configuration pytest : rend les modules du projet importables.

Les modules vivent dans ingestion/, database/ et à la racine (pas de package
installable). On ajoute ces dossiers au chemin d'import pour les tests, comme
le fait serveur_api.py en production.
"""

import os
import sys

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for sous_dossier in ("", "ingestion", "database"):
    chemin = os.path.join(RACINE, sous_dossier)
    if chemin not in sys.path:
        sys.path.insert(0, chemin)
