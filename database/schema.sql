-- =============================================================================
--  ORACLE 2026 — Schéma relationnel & logique métier (PostgreSQL 16)
--  Cible : Coupe du Monde 2026
--  Fuseau serveur : GMT+4  (stockage en TIMESTAMPTZ, jamais d'heure "naïve")
-- =============================================================================
--  Convention : tout calcul métier dérivé (Indice de Risque, obsolescence des
--  pronostics) vit DANS la base via des triggers, afin de rester cohérent quelle
--  que soit la source de l'écriture (scraper, import, correction manuelle).
-- =============================================================================

BEGIN;

SET TIME ZONE 'Asia/Dubai';   -- +04, sans heure d'été

-- -----------------------------------------------------------------------------
--  Types énumérés
-- -----------------------------------------------------------------------------
CREATE TYPE statut_match      AS ENUM ('A_VENIR', 'EN_COURS', 'TERMINE', 'REPORTE');
CREATE TYPE issue_pari        AS ENUM ('1', 'N', '2');     -- victoire dom / nul / ext
CREATE TYPE statut_pronostic  AS ENUM ('VALIDE', 'OBSOLETE', 'EN_ATTENTE');
CREATE TYPE type_evenement    AS ENUM (
    'BLESSURE_JOUEUR_MAJEUR',   -- impact fort
    'BLESSURE_JOUEUR_MINEUR',
    'SUSPENSION',
    'COMPO_OFFICIELLE',
    'RUMEUR',
    'METEO',
    'MOUVEMENT_COTE'
);

-- =============================================================================
--  TABLE : Equipes
-- =============================================================================
CREATE TABLE equipes (
    id                SERIAL PRIMARY KEY,
    nom               VARCHAR(80)  NOT NULL UNIQUE,
    code_fifa         CHAR(3)      NOT NULL UNIQUE,        -- ex. 'FRA', 'ARG'
    confederation     VARCHAR(20)  NOT NULL,               -- UEFA, CONMEBOL, ...
    classement_fifa   SMALLINT     CHECK (classement_fifa BETWEEN 1 AND 250),
    -- "force" normalisée 0-100, utilisée comme feature de base par le LLM
    force_offensive   NUMERIC(5,2) DEFAULT 50 CHECK (force_offensive BETWEEN 0 AND 100),
    force_defensive   NUMERIC(5,2) DEFAULT 50 CHECK (force_defensive BETWEEN 0 AND 100),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- =============================================================================
--  TABLE : Matchs
-- =============================================================================
CREATE TABLE matchs (
    id                SERIAL PRIMARY KEY,
    equipe_dom_id     INT NOT NULL REFERENCES equipes(id) ON DELETE RESTRICT,
    equipe_ext_id     INT NOT NULL REFERENCES equipes(id) ON DELETE RESTRICT,
    coup_envoi        TIMESTAMPTZ NOT NULL,                -- stocké en TZ, affiché en GMT+4
    stade             VARCHAR(120),
    ville             VARCHAR(120),
    phase             VARCHAR(40)  NOT NULL DEFAULT 'Phase de groupes',
    statut            statut_match NOT NULL DEFAULT 'A_VENIR',
    score_dom         SMALLINT     CHECK (score_dom >= 0),
    score_ext         SMALLINT     CHECK (score_ext >= 0),
    -- Colonne dérivée, maintenue EXCLUSIVEMENT par trigger (voir plus bas) :
    indice_risque     NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (indice_risque BETWEEN 0 AND 100),
    indice_maj_le     TIMESTAMPTZ,                         -- dernière maj de l'indice
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_equipes_differentes CHECK (equipe_dom_id <> equipe_ext_id)
);

CREATE INDEX idx_matchs_coup_envoi ON matchs (coup_envoi);
CREATE INDEX idx_matchs_statut     ON matchs (statut);

-- =============================================================================
--  TABLE : Contexte_Actu  (alimentée par les daemons de scraping)
-- =============================================================================
CREATE TABLE contexte_actu (
    id                BIGSERIAL PRIMARY KEY,
    match_id          INT NOT NULL REFERENCES matchs(id)  ON DELETE CASCADE,
    equipe_id         INT          REFERENCES equipes(id) ON DELETE SET NULL,
    type_evenement    type_evenement NOT NULL,
    joueur_nom        VARCHAR(120),
    -- importance du joueur concerné (1 = remplaçant, 10 = star/titulaire indiscutable)
    importance_joueur SMALLINT     DEFAULT 5 CHECK (importance_joueur BETWEEN 1 AND 10),
    -- impact unitaire brut estimé par le scraper (sera pondéré par le trigger)
    impact_score      NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (impact_score BETWEEN 0 AND 100),
    description       TEXT,
    source            VARCHAR(160),                        -- URL / nom du média
    fiabilite_source  SMALLINT     DEFAULT 5 CHECK (fiabilite_source BETWEEN 1 AND 10),
    detecte_le        TIMESTAMPTZ  NOT NULL DEFAULT now(), -- horodatage scraping (GMT+4)
    -- clé naturelle anti-doublon : un même évènement (type+joueur+match) une seule fois
    CONSTRAINT uq_contexte_naturel UNIQUE (match_id, type_evenement, joueur_nom, source)
);

CREATE INDEX idx_contexte_match ON contexte_actu (match_id);
CREATE INDEX idx_contexte_type  ON contexte_actu (type_evenement);

-- =============================================================================
--  TABLE : Pronostics_LLM
-- =============================================================================
CREATE TABLE pronostics_llm (
    id                    BIGSERIAL PRIMARY KEY,
    match_id              INT NOT NULL REFERENCES matchs(id) ON DELETE CASCADE,
    issue                 issue_pari      NOT NULL,
    score_estime          VARCHAR(7),                       -- ex. '2-1'
    confiance             NUMERIC(4,3) NOT NULL CHECK (confiance BETWEEN 0 AND 1),
    justification         TEXT,
    model_version         VARCHAR(40) NOT NULL DEFAULT 'llm-light-v0',
    -- "photo" de l'indice de risque au moment du pronostic (rempli par trigger) :
    indice_risque_snapshot NUMERIC(5,2),
    statut                statut_pronostic NOT NULL DEFAULT 'VALIDE',
    genere_le             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_prono_match  ON pronostics_llm (match_id);
CREATE INDEX idx_prono_statut ON pronostics_llm (statut);

-- =============================================================================
--  TABLE : Journal_Risque  (audit des recalculs d'indice — traçabilité)
-- =============================================================================
CREATE TABLE journal_risque (
    id              BIGSERIAL PRIMARY KEY,
    match_id        INT NOT NULL REFERENCES matchs(id) ON DELETE CASCADE,
    ancien_indice   NUMERIC(5,2),
    nouvel_indice   NUMERIC(5,2),
    declencheur     VARCHAR(120),     -- ce qui a causé le recalcul
    cree_le         TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- #############################################################################
--  TRIGGER 1 — RECALCUL AUTOMATIQUE DE L'INDICE DE RISQUE
--  Déclenché à CHAQUE modification de contexte_actu (INSERT/UPDATE/DELETE).
--  Logique métier :
--    indice = base(force des équipes) + somme pondérée des évènements de contexte
--    Une "BLESSURE_JOUEUR_MAJEUR" sur un joueur très important pèse très lourd.
--    Le résultat est borné [0,100] et l'ancien/nouvel indice est journalisé.
-- #############################################################################
CREATE OR REPLACE FUNCTION fn_recalc_indice_risque()
RETURNS TRIGGER AS $$
DECLARE
    v_match_id     INT;
    v_ancien       NUMERIC(5,2);
    v_base         NUMERIC(5,2);
    v_contexte     NUMERIC(5,2);
    v_nouvel       NUMERIC(5,2);
    v_ecart_force  NUMERIC(5,2);
    v_declencheur  VARCHAR(120);
BEGIN
    -- Selon l'opération, on récupère le match concerné
    v_match_id := COALESCE(NEW.match_id, OLD.match_id);

    -- 1) Composante "base" : plus les deux équipes sont proches en niveau,
    --    plus le match est incertain (donc risqué à pronostiquer).
    SELECT ABS( (ed.force_offensive + ed.force_defensive)
              - (ee.force_offensive + ee.force_defensive) ) / 2
      INTO v_ecart_force
      FROM matchs m
      JOIN equipes ed ON ed.id = m.equipe_dom_id
      JOIN equipes ee ON ee.id = m.equipe_ext_id
     WHERE m.id = v_match_id;

    -- écart faible (0) -> base haute (~40) ; écart fort (100) -> base basse (0)
    v_base := GREATEST(0, 40 - COALESCE(v_ecart_force, 0) * 0.4);

    -- 2) Composante "contexte" : somme pondérée de tous les évènements actifs
    --    du match. Pondération = impact * importance_joueur * fiabilite_source,
    --    avec un multiplicateur métier selon le type d'évènement.
    SELECT COALESCE(SUM(
              c.impact_score
              * (c.importance_joueur / 10.0)
              * (c.fiabilite_source / 10.0)
              * CASE c.type_evenement
                    WHEN 'BLESSURE_JOUEUR_MAJEUR' THEN 1.8   -- poids fort
                    WHEN 'SUSPENSION'             THEN 1.5
                    WHEN 'COMPO_OFFICIELLE'       THEN 1.2
                    WHEN 'RUMEUR'                 THEN 0.6
                    WHEN 'METEO'                  THEN 0.8
                    WHEN 'MOUVEMENT_COTE'         THEN 1.0
                    ELSE 0.5
                END
           ), 0)
      INTO v_contexte
      FROM contexte_actu c
     WHERE c.match_id = v_match_id;

    -- 3) Indice final borné [0,100]
    v_nouvel := LEAST(100, ROUND(v_base + v_contexte, 2));

    -- 4) Lecture de l'ancien indice (pour journal + détection de saut)
    SELECT indice_risque INTO v_ancien FROM matchs WHERE id = v_match_id;

    -- 5) Écriture (l'UPDATE de matchs réveillera le TRIGGER 2)
    UPDATE matchs
       SET indice_risque = v_nouvel,
           indice_maj_le = now(),
           updated_at    = now()
     WHERE id = v_match_id;

    -- Match en cours de suppression (DELETE en cascade depuis matchs) :
    -- rien à recalculer ni à journaliser, sous peine de violer la FK de
    -- journal_risque vers un match qui n'existe plus.
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- 6) Traçabilité
    v_declencheur := COALESCE(NEW.type_evenement::TEXT, 'SUPPRESSION_CONTEXTE')
                     || COALESCE(' / ' || NEW.joueur_nom, '');
    INSERT INTO journal_risque(match_id, ancien_indice, nouvel_indice, declencheur)
    VALUES (v_match_id, v_ancien, v_nouvel, v_declencheur);

    RETURN NULL;  -- AFTER trigger : valeur de retour ignorée
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_recalc_indice_risque
AFTER INSERT OR UPDATE OR DELETE ON contexte_actu
FOR EACH ROW
EXECUTE FUNCTION fn_recalc_indice_risque();


-- #############################################################################
--  TRIGGER 2 — INVALIDATION AUTOMATIQUE DES PRONOSTICS PÉRIMÉS
--  Déclenché quand l'indice_risque d'un match change de façon significative.
--  Logique métier : si l'incertitude bondit (ex. nouvelle blessure majeure
--  après qu'un pronostic ait été émis), les pronostics VALIDE existants
--  deviennent OBSOLETE et devront être régénérés par l'orchestrateur LLM.
-- #############################################################################
CREATE OR REPLACE FUNCTION fn_invalider_pronostics()
RETURNS TRIGGER AS $$
DECLARE
    v_saut       NUMERIC(5,2);
    v_nb_touches INT;
BEGIN
    -- On ne réagit qu'aux vrais changements d'indice
    IF NEW.indice_risque IS DISTINCT FROM OLD.indice_risque THEN

        v_saut := ABS(NEW.indice_risque - OLD.indice_risque);

        -- Règle métier : un saut > 15 points OU un franchissement du seuil
        -- critique de 70 invalide les pronostics déjà calculés.
        IF v_saut > 15
           OR (OLD.indice_risque < 70 AND NEW.indice_risque >= 70) THEN

            UPDATE pronostics_llm
               SET statut = 'OBSOLETE'
             WHERE match_id = NEW.id
               AND statut   = 'VALIDE';

            GET DIAGNOSTICS v_nb_touches = ROW_COUNT;

            IF v_nb_touches > 0 THEN
                RAISE NOTICE 'Match %, indice % -> % : % pronostic(s) marqué(s) OBSOLETE',
                    NEW.id, OLD.indice_risque, NEW.indice_risque, v_nb_touches;
            END IF;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_invalider_pronostics
AFTER UPDATE OF indice_risque ON matchs
FOR EACH ROW
EXECUTE FUNCTION fn_invalider_pronostics();


-- #############################################################################
--  TRIGGER 3 (bonus) — GARDE-FOU & SNAPSHOT À L'INSERTION D'UN PRONOSTIC
--  BEFORE INSERT : on refuse un pronostic sur un match déjà terminé, et on
--  capture automatiquement l'indice de risque courant (snapshot) pour audit.
-- #############################################################################
CREATE OR REPLACE FUNCTION fn_pronostic_garde_fou()
RETURNS TRIGGER AS $$
DECLARE
    v_statut  statut_match;
    v_indice  NUMERIC(5,2);
BEGIN
    SELECT statut, indice_risque INTO v_statut, v_indice
      FROM matchs WHERE id = NEW.match_id;

    IF v_statut IS NULL THEN
        RAISE EXCEPTION 'Pronostic refusé : match % inexistant', NEW.match_id;
    END IF;

    IF v_statut = 'TERMINE' THEN
        RAISE EXCEPTION 'Pronostic refusé : le match % est déjà TERMINE', NEW.match_id;
    END IF;

    -- snapshot automatique de l'indice au moment du pronostic
    NEW.indice_risque_snapshot := v_indice;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_pronostic_garde_fou
BEFORE INSERT ON pronostics_llm
FOR EACH ROW
EXECUTE FUNCTION fn_pronostic_garde_fou();


-- #############################################################################
--  TRIGGER 4 (utilitaire) — horodatage updated_at sur matchs
-- #############################################################################
CREATE OR REPLACE FUNCTION fn_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_touch_matchs
BEFORE UPDATE ON matchs
FOR EACH ROW
EXECUTE FUNCTION fn_touch_updated_at();


-- =============================================================================
--  JEU D'ESSAI / DÉMONSTRATION DES TRIGGERS
-- =============================================================================
INSERT INTO equipes (nom, code_fifa, confederation, classement_fifa, force_offensive, force_defensive)
VALUES ('France', 'FRA', 'UEFA', 2, 88, 84),
       ('Canada', 'CAN', 'CONCACAF', 30, 62, 58);

INSERT INTO matchs (equipe_dom_id, equipe_ext_id, coup_envoi, stade, ville, phase)
VALUES (1, 2, TIMESTAMPTZ '2026-06-15 04:00:00+04', 'BMO Field', 'Toronto', 'Phase de groupes');
-- À ce stade : indice_risque = 0 (aucun contexte). On émet un pronostic.

INSERT INTO pronostics_llm (match_id, issue, score_estime, confiance, justification)
VALUES (1, '1', '2-0', 0.74, 'France nettement supérieure, contexte calme.');
-- Le snapshot de risque est capturé automatiquement (trigger 3).

-- 💥 Une blessure majeure tombe pendant la fenêtre nocturne :
INSERT INTO contexte_actu
    (match_id, equipe_id, type_evenement, joueur_nom, importance_joueur,
     impact_score, description, source, fiabilite_source)
VALUES
    (1, 1, 'BLESSURE_JOUEUR_MAJEUR', 'K. Mbappé', 10,
     60, 'Forfait de dernière minute confirmé', 'lequipe.fr', 9);
-- => Trigger 1 recalcule l'indice (hausse forte)
-- => Trigger 2 détecte le saut et passe le pronostic ci-dessus en OBSOLETE.

COMMIT;

-- -----------------------------------------------------------------------------
--  Vérification post-déploiement (à exécuter à la main) :
--    SELECT id, indice_risque, indice_maj_le FROM matchs;
--    SELECT statut, indice_risque_snapshot FROM pronostics_llm;
--    SELECT * FROM journal_risque ORDER BY cree_le DESC;
-- -----------------------------------------------------------------------------
