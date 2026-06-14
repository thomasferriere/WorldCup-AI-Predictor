"""Tests du calcul du Signal de Pari (serveur_api.signal_pari)."""

import serveur_api


def _match(**kw):
    base = {
        "statut": "A_VENIR",
        "issue": "1",
        "statut_pronostic": "VALIDE",
        "indice_maj_le": "2026-06-13T12:00:00+04:00",
        "indice_risque": 30,
        "confiance": 0.75,
    }
    base.update(kw)
    return base


def test_pas_de_signal_sans_pronostic():
    assert serveur_api.signal_pari(_match(issue=None)) is None


def test_pas_de_signal_si_match_pas_a_venir():
    assert serveur_api.signal_pari(_match(statut="TERMINE")) is None


def test_neutre_si_risque_jamais_evalue():
    # indice_maj_le NULL = aucun contexte -> "À ÉVALUER", pas un pari fort.
    sig = serveur_api.signal_pari(_match(indice_maj_le=None, indice_risque=0))
    assert sig["niveau"] == "neutre"


def test_pari_fort():
    sig = serveur_api.signal_pari(_match(confiance=0.80, indice_risque=20))
    assert sig["niveau"] == "fort"


def test_a_fuir_si_risque_extreme():
    sig = serveur_api.signal_pari(_match(confiance=0.80, indice_risque=90))
    assert sig["niveau"] == "fuir"


def test_a_fuir_si_confiance_trop_basse():
    sig = serveur_api.signal_pari(_match(confiance=0.30, indice_risque=20))
    assert sig["niveau"] == "fuir"


def test_prudence_par_defaut():
    sig = serveur_api.signal_pari(_match(confiance=0.60, indice_risque=70))
    assert sig["niveau"] == "prudence"
