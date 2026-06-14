"""Tests de la logique pure du daemon d'ingestion (sans base ni réseau)."""

import datetime
import importlib

import scraper_daemon as sd


# ---------------------------------------------------------------------------
# Garde-fou quota API : max 3 appels/jour, espacés.
# ---------------------------------------------------------------------------

def _isoler_etat(tmp_path, monkeypatch):
    """Pointe le fichier d'état du quota vers un fichier temporaire."""
    monkeypatch.setattr(sd, "FICHIER_ETAT_API", str(tmp_path / "etat_api"))
    monkeypatch.delenv("ORACLE_FORCE_API", raising=False)


def test_quota_autorise_quand_vierge(tmp_path, monkeypatch):
    _isoler_etat(tmp_path, monkeypatch)
    assert sd.appel_api_autorise() is True


def test_quota_bloque_juste_apres_un_appel(tmp_path, monkeypatch):
    _isoler_etat(tmp_path, monkeypatch)
    sd.marquer_api_appelee()
    # Espacement minimal non écoulé -> refusé
    assert sd.appel_api_autorise() is False


def test_quota_force_ignore_l_espacement(tmp_path, monkeypatch):
    _isoler_etat(tmp_path, monkeypatch)
    sd.marquer_api_appelee()
    monkeypatch.setenv("ORACLE_FORCE_API", "1")
    assert sd.appel_api_autorise() is True


def test_quota_plafond_jamais_depasse_meme_force(tmp_path, monkeypatch):
    _isoler_etat(tmp_path, monkeypatch)
    for _ in range(sd.MAX_APPELS_API_JOUR):
        sd.marquer_api_appelee()
    monkeypatch.setenv("ORACLE_FORCE_API", "1")
    assert sd.appel_api_autorise() is False


# ---------------------------------------------------------------------------
# Statut ESPN -> ENUM statut_match.
# ---------------------------------------------------------------------------

def test_statut_depuis_api():
    assert sd._statut_depuis_api({"started": False, "finished": False}) == "A_VENIR"
    assert sd._statut_depuis_api({"started": True, "finished": False}) == "EN_COURS"
    assert sd._statut_depuis_api({"finished": True}) == "TERMINE"
    assert sd._statut_depuis_api({"cancelled": True}) == "REPORTE"


# ---------------------------------------------------------------------------
# Normalisation d'un évènement ESPN vers la structure interne.
# ---------------------------------------------------------------------------

def _event_espn(state="pre", completed=False, score_dom="0", score_ext="0"):
    return {
        "id": "777",
        "date": "2026-06-13T22:00Z",
        "status": {"type": {"name": "STATUS_SCHEDULED", "state": state, "completed": completed}},
        "competitions": [{"competitors": [
            {"homeAway": "home", "team": {"displayName": "Brazil"}, "score": score_dom},
            {"homeAway": "away", "team": {"displayName": "Morocco"}, "score": score_ext},
        ]}],
    }


def test_normaliser_espn_a_venir():
    m = sd._normaliser_match_espn(_event_espn())
    assert m["leagueId"] == sd.LIGUE_ESPN_CDM
    assert m["home"]["name"] == "Brazil"
    assert m["away"]["name"] == "Morocco"
    # Match non commencé : pas de score, statut A_VENIR
    assert m["home"]["score"] is None
    assert m["status"]["finished"] is False and m["status"]["started"] is False


def test_normaliser_espn_termine_porte_le_score():
    m = sd._normaliser_match_espn(_event_espn(state="post", completed=True, score_dom="2", score_ext="1"))
    assert m["status"]["finished"] is True
    assert m["home"]["score"] == 2 and m["away"]["score"] == 1


# ---------------------------------------------------------------------------
# Réconciliation : un article panorama (trop de nations) est écarté.
# ---------------------------------------------------------------------------

def _match(db_id, home, away):
    return {"db_match_id": db_id, "home": {"name": home}, "away": {"name": away}}


def test_reconciliation_rattache_article_cible():
    matchs = [_match(10, "Brazil", "Morocco")]
    articles = [{"titre": "Maroc : Hakimi incertain", "resume": "blessure",
                 "lien": "http://x/1", "mots_cles": ["blessure", "Maroc"]}]
    evts = sd.reconcilier_donnees(matchs, articles)
    assert len(evts) == 1
    assert evts[0].match_id == 10
    assert evts[0].type_evenement == "BLESSURE_JOUEUR_MINEUR"  # "blessure" sans "forfait"


def test_reconciliation_ecarte_article_panorama():
    matchs = [_match(10, "Brazil", "Morocco")]
    # 5 nations citées -> tour d'horizon générique, ne doit PAS être rattaché
    articles = [{"titre": "Les favoris du Mondial", "resume": "...",
                 "lien": "http://x/2",
                 "mots_cles": ["Brésil", "France", "Espagne", "Portugal", "Argentine"]}]
    evts = sd.reconcilier_donnees(matchs, articles)
    assert evts == []


def test_module_importable_sans_effet_de_bord():
    # Réimporter ne doit pas ouvrir de connexion ni d'appel réseau.
    importlib.reload(sd)
