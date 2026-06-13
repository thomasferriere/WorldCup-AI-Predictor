#!/usr/bin/env python3
"""
seed_classements.py — Oracle 2026 : classements FIFA réels & force des équipes
==============================================================================

Problème résolu : les équipes créées par l'ingestion ESPN arrivaient sans
classement (`classement_fifa` NULL) et avec une force par défaut (50/50),
ce qui privait l'indice de risque et le moteur IA de tout signal pour
différencier les sélections.

Source : classement FIFA officiel publié le 11 juin 2026 (veille de la CDM) —
top 50 relevé sur ESPN, complété pour les participants hors top 50.
> À rafraîchir quand la FIFA publie un nouveau classement (≈ tous les 2 mois).

La force (0-100) est dérivée du rang : rang 1 ≈ 99, rang 50 ≈ 65, rang 83 ≈ 42.
Les deux composantes (offensive/défensive) reçoivent cette même valeur ; c'est
l'écart de force ENTRE les deux équipes qui pilote l'indice de risque.

Exécution (idempotente — n'écrit que sur les équipes déjà présentes) :
    .venv/bin/python database/seed_classements.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ingestion"))
from scraper_daemon import connecter_postgres  # noqa: E402

# Rang FIFA officiel (11/06/2026) par nom d'équipe tel que stocké en base.
CLASSEMENT_FIFA = {
    "Argentine": 1, "Espagne": 2, "France": 3, "Angleterre": 4, "Brésil": 5,
    "Portugal": 6, "Pays-Bas": 7, "Belgique": 8, "Allemagne": 9, "Croatie": 10,
    "Maroc": 11, "Italie": 12, "Colombie": 13, "Mexique": 14, "Sénégal": 15,
    "États-Unis": 16, "Uruguay": 17, "Japon": 18, "Suisse": 19, "Danemark": 20,
    "Corée du Sud": 21, "Équateur": 22, "Autriche": 23, "Turquie": 24,
    "Australie": 25, "Canada": 26, "Norvège": 28, "Panama": 29, "Pologne": 30,
    "Algérie": 33, "Égypte": 34, "Écosse": 35, "Nigéria": 37, "Paraguay": 38,
    "Tunisie": 40, "Côte d'Ivoire": 41, "Suède": 42, "Tchéquie": 43,
    "Cameroun": 44, "Costa Rica": 48, "Ouzbékistan": 49,
    "Iran": 20,
    # Participants hors top 50 (rangs réels relevés individuellement,
    # sources diverses ~juin 2026 ; légères divergences inter-sources possibles)
    "Qatar": 56, "Afrique du Sud": 60, "Arabie saoudite": 61,
    "Bosnie-Herzégovine": 64, "Cap-Vert": 67, "Curaçao": 82, "Haïti": 83,
    "Nouvelle-Zélande": 85,
}


def force_depuis_rang(rang: int) -> float:
    """Force 0-100 dérivée du rang FIFA (décroissante, plancher à 30)."""
    return round(max(30.0, 99.0 - (rang - 1) * 0.7), 1)


def main() -> int:
    conn = connecter_postgres()
    maj = 0
    with conn.cursor() as cur:
        for nom, rang in CLASSEMENT_FIFA.items():
            force = force_depuis_rang(rang)
            cur.execute(
                """UPDATE equipes
                      SET classement_fifa = %s,
                          force_offensive = %s,
                          force_defensive = %s
                    WHERE nom = %s""",
                (rang, force, force, nom),
            )
            maj += cur.rowcount
    conn.commit()
    conn.close()
    print(f"{maj} équipe(s) mise(s) à jour avec leur classement FIFA réel "
          f"(sur {len(CLASSEMENT_FIFA)} connues).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
