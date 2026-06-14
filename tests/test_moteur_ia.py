"""Tests du parsing des pronostics et de la dérivation de force FIFA."""

import moteur_ia
import seed_classements


# ---------------------------------------------------------------------------
# Extraction des champs depuis la réponse brute du LLM.
# ---------------------------------------------------------------------------

def test_extraction_format_propre():
    texte = ("ISSUE: 1\nSCORE: 2-1\nCONFIANCE: 0.72\n"
             "JUSTIFICATION: Le favori l'emporte logiquement.")
    champs = moteur_ia.extraire_champs_pronostic(texte)
    assert champs["issue"] == "1"
    assert champs["score_estime"] == "2-1"
    assert champs["confiance"] == 0.72
    assert "favori" in champs["justification"]


def test_extraction_realigne_issue_sur_score_incoherent():
    # Le modèle annonce une victoire (2) mais un score nul (1-1) :
    # le score fait foi -> issue N, et la confiance est dégradée.
    texte = "ISSUE: 2\nSCORE: 1-1\nCONFIANCE: 0.90\nJUSTIFICATION: bla"
    champs = moteur_ia.extraire_champs_pronostic(texte)
    assert champs["issue"] == "N"
    assert champs["confiance"] <= 0.45


def test_extraction_score_determine_issue():
    texte = "ISSUE: N\nSCORE: 0-2\nCONFIANCE: 0.5\nJUSTIFICATION: x"
    champs = moteur_ia.extraire_champs_pronostic(texte)
    assert champs["issue"] == "2"  # 0 < 2 -> victoire extérieur


def test_extraction_confiance_bornee_et_repli():
    # Pas de CONFIANCE lisible -> repli prudent ; valeurs hors [0,1] bornées.
    champs = moteur_ia.extraire_champs_pronostic("ISSUE: 1\nSCORE: 3-0")
    assert 0.0 <= champs["confiance"] <= 1.0


# ---------------------------------------------------------------------------
# Force dérivée du classement FIFA (seed_classements).
# ---------------------------------------------------------------------------

def test_force_decroit_avec_le_rang():
    assert seed_classements.force_depuis_rang(1) == 99.0
    assert seed_classements.force_depuis_rang(1) > seed_classements.force_depuis_rang(20)
    assert seed_classements.force_depuis_rang(20) > seed_classements.force_depuis_rang(80)


def test_force_a_un_plancher():
    # Même très bas, la force ne descend pas sous 30.
    assert seed_classements.force_depuis_rang(250) >= 30.0


def test_classement_couvre_le_top_5_attendu():
    c = seed_classements.CLASSEMENT_FIFA
    assert c["Argentine"] == 1
    assert c["France"] == 3
    assert c["Brésil"] == 5
