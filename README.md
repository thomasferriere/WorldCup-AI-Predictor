# ⚽ Oracle 2026 — Moteur de Pronostics Automatisés (Coupe du Monde 2026)

> Infrastructure data-driven pour l'agrégation de contexte sportif, le calcul d'indices de risque et la génération de pronostics assistés par LLM léger.

[![Statut](https://img.shields.io/badge/statut-fondation-blue)]()
[![Stack](https://img.shields.io/badge/stack-Python%20%7C%20PostgreSQL%20%7C%20Nagios-success)]()
[![Plateforme](https://img.shields.io/badge/host-macOS%20(GMT%2B4)-lightgrey)]()

---

## 1. Vision du projet

`Oracle 2026` est une plateforme qui **ingère automatiquement** le contexte d'avant-match (compositions, blessures, suspensions, météo, rumeurs de dernière minute, cotes), le **structure** dans une base relationnelle PostgreSQL, calcule des **indicateurs métier dérivés** (notamment un *Indice de Risque* par match), puis expose ces données à une **API de modèle LLM léger** qui produit le pronostic final.

Cette première phase ne contient **pas** le LLM décisionnel : elle livre la **fondation** (pipeline d'ingestion, base de données, triggers métier, frontend, supervision) sur laquelle le modèle viendra se brancher via un simple contrat d'API.

---

## 2. Architecture générale

```
                       macOS host — fuseau serveur : GMT+4
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│   ⏱  CRON / launchd (fenêtre nocturne 01:00 → 09:00 GMT+4)                 │
│        │                                                                   │
│        ▼                                                                   │
│   ┌─────────────────────┐     scraping     ┌───────────────────────────┐  │
│   │  DAEMONS D'INGESTION │ ───────────────► │  Sources externes          │ │
│   │  (scrapers Python)   │ ◄─────────────── │  flux cotes / news / lineup │ │
│   │  - scraper_lineups   │                  └───────────────────────────┘  │
│   │  - scraper_news      │                                                 │
│   │  - scraper_odds      │                                                 │
│   └─────────┬───────────┘                                                  │
│             │ INSERT / UPSERT                                              │
│             ▼                                                              │
│   ┌─────────────────────────────────────────────┐                         │
│   │            PostgreSQL  (couche stockage)      │                        │
│   │  Tables : Equipes · Matchs · Contexte_Actu    │                        │
│   │           Pronostics_LLM                      │                        │
│   │  ⚙ TRIGGERS métier (indice de risque, etc.)   │                        │
│   └─────────┬───────────────────────────┬─────────┘                        │
│             │ lecture features          │ écriture pronostic               │
│             ▼                            ▲                                  │
│   ┌──────────────────────┐    HTTP/JSON  │                                 │
│   │  Orchestrateur LLM    │ ─────────────┘                                 │
│   │  (API model gateway)  │  appel au modèle léger (phase 2)               │
│   └─────────┬────────────┘                                                 │
│             │ REST / JSON                                                  │
│             ▼                                                              │
│   ┌──────────────────────┐                                                 │
│   │  Frontend Dashboard   │  HTML5 + CSS (Glassmorphism / liquid glass)    │
│   │  (cartes translucides)│                                                │
│   └──────────────────────┘                                                 │
│                                                                            │
│   🔍 SUPERVISION : Nagios Core + agent NRPE surveillent en continu les     │
│      daemons de scraping, la fraîcheur des données et l'état PostgreSQL.   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Flux de données détaillé

### Étape 1 — Ingestion nocturne (daemons de scraping)

Le cœur du système est une **fenêtre d'ingestion nocturne**. La compétition se déroulant sur le **continent américain** (fuseaux ≈ GMT‑3 à GMT‑7) alors que le serveur tourne en **GMT+4**, les matchs en soirée locale américaine et surtout les **rumeurs de dernière minute** (compositions officielles publiées ~1h avant le coup d'envoi) tombent majoritairement entre **02:00 et 08:00 heure serveur**.

Trois daemons indépendants couvrent cette fenêtre :

| Daemon | Rôle | Cadence (GMT+4) |
|--------|------|-----------------|
| `scraper_lineups` | Compositions probables / officielles, blessures, suspensions | toutes les 15 min, 01:00–09:00 |
| `scraper_news` | Rumeurs, presse, conférences d'avant-match → table `Contexte_Actu` | toutes les 10 min, 01:00–09:00 |
| `scraper_odds` | Cotes des bookmakers, mouvements de ligne | toutes les 5 min, 01:00–09:00 |

Chaque daemon est **idempotent** (UPSERT sur clé naturelle) afin qu'une réexécution ne duplique jamais une donnée. Les écritures dans `Contexte_Actu` déclenchent automatiquement les **triggers SQL** (voir §5).

> ⚠️ **Pourquoi pas un service permanent 24/7 ?** Hors fenêtre américaine, il n'y a quasiment aucune donnée nouvelle. Concentrer la charge sur 01:00–09:00 réduit le risque de bannissement IP, économise les ressources de la machine macOS de test et simplifie la supervision (un scraper *doit* tourner la nuit, un silence la nuit = alerte critique).

### Étape 2 — Stockage (PostgreSQL)

Toutes les données convergent vers une base PostgreSQL unique. La logique métier dérivée (ex. **Indice de Risque**) n'est **pas** calculée dans le code Python mais **dans la base via des triggers**, garantissant que la valeur reste cohérente quelle que soit la source de l'écriture (scraper, import manuel, correction SQL).

### Étape 3 — Appel API vers le LLM (phase 2, déjà câblée)

Un **orchestrateur** lit les *features* consolidées d'un match (stats équipes + `Contexte_Actu` agrégé + `indice_risque`), construit un *prompt* structuré et appelle le **modèle LLM léger** via une API REST interne. La réponse (équipe favorite, score probable, niveau de confiance, justification) est écrite dans `Pronostics_LLM`. Le contrat d'API est volontairement minimal :

```http
POST /api/v1/predict
Content-Type: application/json

{ "match_id": 142, "features": { ... } }

→ 200 OK
{ "issue": "1", "score_estime": "2-1", "confiance": 0.71, "justification": "..." }
```

### Étape 4 — Affichage (frontend)

Le dashboard HTML5/CSS lit l'état courant (matchs du jour, indice de risque, dernier pronostic) et l'affiche sous forme de **cartes translucides en glassmorphism** (effet *liquid glass*), superposées à un arrière-plan visuel riche. Voir `frontend/`.

---

## 4. Pile technique

| Couche | Technologie | Justification |
|--------|-------------|---------------|
| Ingestion | Python 3.11 (`requests`, `httpx`, `BeautifulSoup`/`playwright`) | écosystème scraping mature |
| Ordonnancement | `cron` / `launchd` (macOS) | natif, pas de dépendance externe |
| Stockage | PostgreSQL 16 | triggers, contraintes, JSONB |
| Orchestration LLM | FastAPI (gateway) | contrat REST simple |
| Frontend | HTML5 + CSS pur | zéro build, glassmorphism natif |
| Supervision | Nagios Core + NRPE | surveillance des daemons vitaux |

---

## 5. La règle d'or : l'Indice de Risque

L'**Indice de Risque** d'un match (`Matchs.indice_risque`, 0 = match « lisible », 100 = très incertain) est **recalculé automatiquement par un trigger** à chaque insertion dans `Contexte_Actu`. Une blessure de joueur majeur, une suspension ou une rumeur à fort impact font grimper l'indice. Le LLM consomme cet indice comme *feature* de premier plan. Détails et code : [`database/schema.sql`](database/schema.sql).

---

## 6. Structure du dépôt

```
oracle-2026/
├── README.md                  # ce fichier
├── database/
│   └── schema.sql             # tables + triggers PostgreSQL
├── ingestion/
│   ├── scraper_lineups.py     # (à implémenter)
│   ├── scraper_news.py        # (à implémenter)
│   ├── scraper_odds.py        # (à implémenter)
│   └── crontab.gmt4           # planification nocturne GMT+4
├── frontend/
│   ├── index.html             # dashboard
│   └── style.css              # design system liquid glass
└── monitoring/
    ├── nagios/commands.cfg    # définitions de commandes
    ├── nagios/services.cfg    # définitions de services
    └── nrpe/nrpe_local.cfg    # checks côté hôte surveillé
```

---

## 7. Installation rapide (environnement de test macOS)

```bash
# 1. Base de données
brew install postgresql@16 && brew services start postgresql@16
createdb oracle2026
psql oracle2026 -f database/schema.sql

# 2. Environnement Python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Vérifier le fuseau du serveur (doit être GMT+4)
sudo systemsetup -gettimezone        # attendu : Asia/Dubai ou équivalent +04

# 4. Charger la planification nocturne
crontab ingestion/crontab.gmt4
```

---

## 8. Planification (`ingestion/crontab.gmt4`)

```cron
# Fuseau serveur : GMT+4. Fenêtre américaine = 01:00 → 09:00 heure locale.
# Format : min  heure  jour  mois  jourSemaine  commande

# News & rumeurs : toutes les 10 min entre 01h et 09h
*/10  1-8  *  6,7  *   /usr/bin/python3 $ORACLE/ingestion/scraper_news.py     >> $ORACLE/logs/news.log 2>&1

# Compositions : toutes les 15 min entre 01h et 09h
*/15  1-8  *  6,7  *   /usr/bin/python3 $ORACLE/ingestion/scraper_lineups.py  >> $ORACLE/logs/lineups.log 2>&1

# Cotes : toutes les 5 min entre 01h et 09h (mouvements rapides)
*/5   1-8  *  6,7  *   /usr/bin/python3 $ORACLE/ingestion/scraper_odds.py     >> $ORACLE/logs/odds.log 2>&1

# Consolidation + déclenchement des pronostics LLM à 08h30 (juste après la fenêtre)
30    8    *  6,7  *   /usr/bin/python3 $ORACLE/ingestion/run_predictions.py  >> $ORACLE/logs/predict.log 2>&1
```

> Les mois `6,7` (juin/juillet) cadrent la période de la compétition. Adapter au calendrier officiel.

---

## 9. Roadmap

- [x] Schéma relationnel + triggers métier
- [x] Frontend glassmorphism (fondation)
- [x] Supervision Nagios/NRPE
- [ ] Implémentation des 3 scrapers
- [ ] Branchement de l'API LLM léger (phase 2)
- [ ] Backtesting des pronostics vs résultats réels

---

## 10. Licence & avertissement

Projet à but technique/éducatif. Le scraping doit respecter les CGU des sources et la législation applicable. Les pronostics sont indicatifs et ne constituent pas une incitation au jeu.
