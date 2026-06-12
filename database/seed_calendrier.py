#!/usr/bin/env python3
"""
seed_calendrier.py — Oracle 2026 : injection des 104 slots de matchs CDM 2026
==============================================================================

Format 2026 (48 équipes) :
    72 matchs de groupes (12 groupes A-L de 4 équipes, 6 matchs par groupe)
  + 16 seizièmes + 8 huitièmes + 4 quarts + 2 demi-finales
  +  1 match pour la 3e place + 1 finale                            = 104

Les qualifications n'étant pas terminées, chaque slot oppose des équipes
placeholder ('Équipe A1', 'Vainqueur Huitième 1'…) avec le statut EN_ATTENTE.
Le daemon d'ingestion crée ses propres lignes quand les vrais matchs arrivent ;
les slots correspondants seront remplacés/purgés au fil de la compétition.

Prérequis (une fois, en tant que propriétaire de la base) :
    psql oracle2026 -c "ALTER TYPE statut_match ADD VALUE IF NOT EXISTS 'EN_ATTENTE';"

Exécution (idempotente — relançable sans doublons) :
    .venv/bin/python database/seed_calendrier.py
"""

import datetime
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ingestion"))
from scraper_daemon import connecter_postgres  # noqa: E402

UTC = datetime.timezone.utc
GROUPES = "ABCDEFGHIJKL"


def generer_slots() -> list[tuple[str, str, datetime.datetime, str]]:
    """Liste des 104 slots : (equipe_dom, equipe_ext, coup_envoi UTC, phase)."""
    slots: list[tuple[str, str, datetime.datetime, str]] = []

    def planifier(matchs: list[tuple[str, str, str]], premier_jour: datetime.datetime,
                  heures: list[int], par_jour: int) -> None:
        for i, (dom, ext, phase) in enumerate(matchs):
            jour = premier_jour + datetime.timedelta(days=i // par_jour)
            slots.append((dom, ext, jour.replace(hour=heures[i % par_jour]), phase))

    # --- Phase de groupes : 72 matchs, 4 par jour du 12 au 29 juin ------------
    rencontres = [(1, 2), (3, 4), (1, 3), (2, 4), (1, 4), (2, 3)]   # 3 journées
    groupes_matchs = [
        (f"Équipe {g}{a}", f"Équipe {g}{b}", f"Groupe {g}")
        for a, b in rencontres for g in GROUPES
    ]
    planifier(groupes_matchs, datetime.datetime(2026, 6, 12, tzinfo=UTC), [14, 17, 20, 23], 4)

    # --- Seizièmes de finale : 16 matchs, 4 par jour du 30 juin au 3 juillet --
    qualifies_dom = ([f"1er Groupe {g}" for g in GROUPES]
                     + [f"2e Groupe {g}" for g in "ABCD"])
    qualifies_ext = ([f"2e Groupe {g}" for g in "EFGHIJKL"]
                     + [f"Meilleur 3e n°{k}" for k in range(1, 9)])
    seiziemes = [(d, e, "Seizièmes de finale") for d, e in zip(qualifies_dom, qualifies_ext)]
    planifier(seiziemes, datetime.datetime(2026, 6, 30, tzinfo=UTC), [13, 16, 19, 22], 4)

    # --- Huitièmes : 8 matchs, 2 par jour du 4 au 7 juillet -------------------
    huitiemes = [(f"Vainqueur Seizième {2 * i + 1}", f"Vainqueur Seizième {2 * i + 2}",
                  "Huitièmes de finale") for i in range(8)]
    planifier(huitiemes, datetime.datetime(2026, 7, 4, tzinfo=UTC), [16, 20], 2)

    # --- Quarts : 4 matchs, 2 par jour les 9 et 10 juillet --------------------
    quarts = [(f"Vainqueur Huitième {2 * i + 1}", f"Vainqueur Huitième {2 * i + 2}",
               "Quarts de finale") for i in range(4)]
    planifier(quarts, datetime.datetime(2026, 7, 9, tzinfo=UTC), [16, 20], 2)

    # --- Demi-finales : 13 et 14 juillet --------------------------------------
    demis = [(f"Vainqueur Quart {2 * i + 1}", f"Vainqueur Quart {2 * i + 2}",
              "Demi-finales") for i in range(2)]
    planifier(demis, datetime.datetime(2026, 7, 13, tzinfo=UTC), [20], 1)

    # --- 3e place (17 juillet) et finale (19 juillet) --------------------------
    slots.append(("Perdant Demi-finale 1", "Perdant Demi-finale 2",
                  datetime.datetime(2026, 7, 17, 20, tzinfo=UTC), "Match pour la 3e place"))
    slots.append(("Vainqueur Demi-finale 1", "Vainqueur Demi-finale 2",
                  datetime.datetime(2026, 7, 19, 19, tzinfo=UTC), "Finale"))
    return slots


def main() -> int:
    slots = generer_slots()
    assert len(slots) == 104, f"{len(slots)} slots générés au lieu de 104"

    conn = connecter_postgres()
    cur = conn.cursor()

    # 1. Équipes placeholder — code_fifa NOT NULL UNIQUE : codes numériques
    #    séquentiels ('001'…), impossibles à confondre avec un vrai code FIFA.
    noms = []
    for dom, ext, _, _ in slots:
        for nom in (dom, ext):
            if nom not in noms:
                noms.append(nom)
    for i, nom in enumerate(noms, start=1):
        cur.execute(
            """INSERT INTO equipes (nom, code_fifa, confederation)
               VALUES (%s, %s, 'A_DETERMINER') ON CONFLICT DO NOTHING""",
            (nom, f"{i:03d}"),
        )
    cur.execute("SELECT nom, id FROM equipes WHERE nom = ANY(%s)", (noms,))
    ids = dict(cur.fetchall())

    # 2. Slots de matchs (idempotent : clé naturelle équipes + coup d'envoi)
    inseres = 0
    for dom, ext, coup_envoi, phase in slots:
        cur.execute(
            """INSERT INTO matchs (equipe_dom_id, equipe_ext_id, coup_envoi, phase, statut)
               SELECT %(dom)s, %(ext)s, %(quand)s, %(phase)s, 'EN_ATTENTE'
               WHERE NOT EXISTS (
                     SELECT 1 FROM matchs
                     WHERE equipe_dom_id = %(dom)s AND equipe_ext_id = %(ext)s
                       AND coup_envoi = %(quand)s)""",
            {"dom": ids[dom], "ext": ids[ext], "quand": coup_envoi, "phase": phase},
        )
        inseres += cur.rowcount

    conn.commit()
    cur.execute("SELECT count(*) FROM matchs WHERE statut = 'EN_ATTENTE'")
    total = cur.fetchone()[0]
    conn.close()
    print(f"Équipes placeholder : {len(noms)} · slots insérés : {inseres} · total EN_ATTENTE : {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
