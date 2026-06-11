# 🔍 Supervision du pipeline — Nagios Core + NRPE

> Objectif : **aucune panne silencieuse** avant un match. Le risque réel n'est pas que le serveur tombe, mais qu'un scraper s'arrête *sans bruit* pendant la fenêtre nocturne (01:00–09:00 GMT+4) — on découvrirait alors au réveil que les pronostics reposent sur des données périmées.

Cette supervision repose sur deux briques :

- **Nagios Core** : le serveur de supervision (peut tourner sur la machine macOS de test ou sur un petit hôte dédié). Il décide *quand* vérifier et *qui* alerter.
- **NRPE** (*Nagios Remote Plugin Executor*) : un agent léger installé sur l'hôte qui héberge les scrapers + PostgreSQL. Nagios lui demande à distance d'exécuter des *checks* locaux (état d'un process, fraîcheur des données, place disque) et récupère le verdict.

```
   ┌───────────────┐   check_nrpe (TCP 5666)   ┌────────────────────────────┐
   │  Nagios Core   │ ────────────────────────► │  Hôte scrapers (macOS)      │
   │  (ordonnanceur)│ ◄──────────────────────── │  agent NRPE + plugins locaux │
   └───────┬───────┘   OK / WARNING / CRITICAL  └────────────────────────────┘
           │ notifie
           ▼
      e-mail / webhook
```

---

## 1. Codes de retour (rappel)

Tout plugin Nagios renvoie un **code de sortie** que NRPE relaie :

| Code | Statut | Signification |
|------|--------|---------------|
| `0`  | OK       | tout va bien |
| `1`  | WARNING  | seuil d'alerte franchi |
| `2`  | CRITICAL | panne / données absentes |
| `3`  | UNKNOWN  | check impossible à évaluer |

---

## 2. Installation (hôte surveillé — macOS)

```bash
# Agent NRPE + plugins standard
brew install nrpe monitoring-plugins

# Répertoire des plugins maison Oracle 2026
sudo mkdir -p /usr/local/oracle2026/plugins
```

Sur le **serveur Nagios** :

```bash
brew install nagios nrpe        # ou paquets de la distribution Linux
```

---

## 3. Plugins maison (côté hôte surveillé)

Le check le plus important n'est pas « le process tourne-t-il ? » mais **« des données fraîches sont-elles réellement arrivées ? »**. Un scraper peut tourner et ne rien remonter (site qui a changé, IP bannie). On interroge donc directement PostgreSQL.

### 3.1 `check_scraper_freshness` — fraîcheur des données

`/usr/local/oracle2026/plugins/check_scraper_freshness.sh`

```bash
#!/usr/bin/env bash
# Vérifie que des évènements récents ont bien été insérés dans contexte_actu.
# Usage : check_scraper_freshness.sh <minutes_warn> <minutes_crit>
# Tient compte de la fenêtre nocturne GMT+4 : hors fenêtre, on est tolérant.

WARN_MIN=${1:-30}
CRIT_MIN=${2:-60}
export PGDATABASE=oracle2026 PGUSER=nagios PGHOST=127.0.0.1

# Heure serveur (GMT+4)
HEURE=$(TZ='Asia/Dubai' date +%H)

# Âge (en minutes) de la dernière donnée scrappée
AGE=$(psql -tA -c "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MAX(detecte_le)))/60, 99999) FROM contexte_actu;")
AGE=${AGE%.*}   # tronque les décimales

# Hors fenêtre nocturne (09h -> 01h), peu/pas de données : on ne crie pas.
if [ "$HEURE" -ge 9 ] || [ "$HEURE" -lt 1 ]; then
    echo "OK - Hors fenêtre d'ingestion (${HEURE}h GMT+4), dernière donnée il y a ${AGE} min | age=${AGE}m"
    exit 0
fi

# Dans la fenêtre : on EXIGE de la fraîcheur
if [ "$AGE" -ge "$CRIT_MIN" ]; then
    echo "CRITICAL - Aucune donnée depuis ${AGE} min en pleine fenêtre nocturne ! | age=${AGE}m;${WARN_MIN};${CRIT_MIN}"
    exit 2
elif [ "$AGE" -ge "$WARN_MIN" ]; then
    echo "WARNING - Données vieilles de ${AGE} min | age=${AGE}m;${WARN_MIN};${CRIT_MIN}"
    exit 1
else
    echo "OK - Données fraîches (${AGE} min) | age=${AGE}m;${WARN_MIN};${CRIT_MIN}"
    exit 0
fi
```

### 3.2 `check_scraper_proc` — le daemon est-il vivant ?

`/usr/local/oracle2026/plugins/check_scraper_proc.sh`

```bash
#!/usr/bin/env bash
# Vérifie qu'un script de scraping donné n'est pas resté bloqué/zombie.
# Usage : check_scraper_proc.sh <nom_script.py>
SCRIPT=$1
COUNT=$(pgrep -fc "$SCRIPT")

if [ "$COUNT" -eq 0 ]; then
    # 0 process : normal entre deux exécutions cron — état informatif, pas critique
    echo "OK - $SCRIPT au repos (lancé par cron) | procs=0"
    exit 0
elif [ "$COUNT" -gt 3 ]; then
    # empilement : les exécutions cron ne se terminent plus -> blocage
    echo "CRITICAL - $COUNT instances de $SCRIPT empilées (blocage probable) | procs=$COUNT"
    exit 2
else
    echo "OK - $SCRIPT en cours ($COUNT) | procs=$COUNT"
    exit 0
fi
```

### 3.3 `check_pg_oracle` — santé de la base

On s'appuie sur le plugin standard `check_pgsql` (fourni par `monitoring-plugins`) pour vérifier la connectivité PostgreSQL.

```bash
chmod +x /usr/local/oracle2026/plugins/*.sh
```

---

## 4. Configuration de l'agent NRPE (hôte surveillé)

`/usr/local/etc/nrpe.cfg` (ou `/etc/nagios/nrpe.cfg` selon l'install) — section commandes :

```ini
## --- nrpe_local.cfg : commandes exécutables à distance par Nagios ---

# Autoriser le serveur Nagios à interroger cet agent
allowed_hosts=127.0.0.1,192.168.1.10      # IP du serveur Nagios
dont_blame_nrpe=1                          # autorise les arguments passés par check_nrpe

# Fraîcheur des données (warn 30 min, crit 60 min)
command[check_freshness]=/usr/local/oracle2026/plugins/check_scraper_freshness.sh 30 60

# État des trois daemons vitaux
command[check_proc_news]=/usr/local/oracle2026/plugins/check_scraper_proc.sh scraper_news.py
command[check_proc_lineups]=/usr/local/oracle2026/plugins/check_scraper_proc.sh scraper_lineups.py
command[check_proc_odds]=/usr/local/oracle2026/plugins/check_scraper_proc.sh scraper_odds.py

# PostgreSQL up + temps de réponse (warn 2s, crit 5s)
command[check_pg]=/usr/local/opt/monitoring-plugins/sbin/check_pgsql -H 127.0.0.1 -d oracle2026 -l nagios -w 2 -c 5

# Place disque pour les logs de scraping (warn 20%, crit 10% libres)
command[check_disk_logs]=/usr/local/opt/monitoring-plugins/sbin/check_disk -w 20% -c 10% -p /usr/local/oracle2026/logs
```

Relancer l'agent :

```bash
brew services restart nrpe        # macOS
# ou : systemctl restart nrpe     # Linux
```

Test rapide depuis le serveur Nagios :

```bash
/usr/local/opt/monitoring-plugins/sbin/check_nrpe -H 192.168.1.20 -c check_freshness
```

---

## 5. Configuration du serveur Nagios

### 5.1 Définition de la commande générique `check_nrpe`

`/usr/local/etc/nagios/objects/commands.cfg`

```cfg
###############################################################################
# commands.cfg — appel distant générique vers l'agent NRPE
###############################################################################
define command {
    command_name    check_nrpe
    command_line    $USER1$/check_nrpe -H $HOSTADDRESS$ -c $ARG1$
}

# Variante avec arguments (si dont_blame_nrpe=1)
define command {
    command_name    check_nrpe_args
    command_line    $USER1$/check_nrpe -H $HOSTADDRESS$ -c $ARG1$ -a $ARG2$
}
```

### 5.2 Définition de l'hôte

`/usr/local/etc/nagios/objects/oracle_host.cfg`

```cfg
define host {
    use                     linux-server          ; modèle hérité
    host_name               oracle-scraper-01
    alias                   Hote scrapers Oracle 2026
    address                 192.168.1.20
    max_check_attempts      3
    check_period            24x7
    notification_interval   30
    notification_period     24x7
    contacts                admin_oracle
}
```

### 5.3 Période critique : la fenêtre nocturne

`/usr/local/etc/nagios/objects/timeperiods.cfg`

```cfg
# Fenêtre d'ingestion nocturne (heure serveur GMT+4).
# Pendant cette plage, les checks sont resserrés et les alertes prioritaires.
define timeperiod {
    timeperiod_name     fenetre_nocturne_gmt4
    alias               Ingestion nocturne 01h-09h GMT+4
    sunday              01:00-09:00
    monday              01:00-09:00
    tuesday             01:00-09:00
    wednesday           01:00-09:00
    thursday            01:00-09:00
    friday              01:00-09:00
    saturday            01:00-09:00
}
```

### 5.4 Définitions des services

`/usr/local/etc/nagios/objects/oracle_services.cfg`

```cfg
###############################################################################
# SERVICE 1 — Fraîcheur des données (LE check vital)
# Vérification très fréquente pendant la fenêtre nocturne.
###############################################################################
define service {
    use                     generic-service
    host_name               oracle-scraper-01
    service_description     Fraicheur-Donnees-Scraping
    check_command           check_nrpe!check_freshness
    check_interval          3            ; toutes les 3 min
    retry_interval          1
    max_check_attempts      2
    check_period            fenetre_nocturne_gmt4
    notification_options    w,c,r        ; warning, critical, recovery
    notification_interval   10
    contacts                admin_oracle
}

###############################################################################
# SERVICE 2-4 — Daemons de scraping vitaux
###############################################################################
define service {
    use                     generic-service
    host_name               oracle-scraper-01
    service_description     Daemon-News
    check_command           check_nrpe!check_proc_news
    check_interval          5
    check_period            24x7
    contacts                admin_oracle
}

define service {
    use                     generic-service
    host_name               oracle-scraper-01
    service_description     Daemon-Compositions
    check_command           check_nrpe!check_proc_lineups
    check_interval          5
    check_period            24x7
    contacts                admin_oracle
}

define service {
    use                     generic-service
    host_name               oracle-scraper-01
    service_description     Daemon-Cotes
    check_command           check_nrpe!check_proc_odds
    check_interval          5
    check_period            24x7
    contacts                admin_oracle
}

###############################################################################
# SERVICE 5 — PostgreSQL (le stockage ne doit jamais lâcher)
###############################################################################
define service {
    use                     generic-service
    host_name               oracle-scraper-01
    service_description     PostgreSQL-Oracle2026
    check_command           check_nrpe!check_pg
    check_interval          5
    max_check_attempts      3
    contacts                admin_oracle
}

###############################################################################
# SERVICE 6 — Disque des logs (éviter la saturation pendant la compétition)
###############################################################################
define service {
    use                     generic-service
    host_name               oracle-scraper-01
    service_description     Disque-Logs
    check_command           check_nrpe!check_disk_logs
    check_interval          30
    contacts                admin_oracle
}
```

### 5.5 Contact & notifications

`/usr/local/etc/nagios/objects/contacts.cfg`

```cfg
define contact {
    contact_name                    admin_oracle
    alias                           Astreinte Oracle 2026
    host_notifications_enabled      1
    service_notifications_enabled   1
    service_notification_period     24x7
    host_notification_period        24x7
    service_notification_options    w,c,r
    host_notification_options       d,r
    service_notification_commands   notify-service-by-email
    host_notification_commands      notify-host-by-email
    email                           astreinte@oracle2026.local
}
```

> 💡 Pour une astreinte de nuit efficace, brancher en plus une commande de notification vers un webhook (Slack/Telegram) en doublon de l'e-mail : pendant la fenêtre 01:00–09:00, un e-mail seul risque de ne pas réveiller l'astreinte.

---

## 6. Validation & mise en service

```bash
# 1. Vérifier la syntaxe de toute la conf Nagios
nagios -v /usr/local/etc/nagios/nagios.cfg

# 2. Recharger
brew services restart nagios       # ou : systemctl reload nagios

# 3. Test de bout en bout d'un check distant
$USER1$/check_nrpe -H 192.168.1.20 -c check_proc_news
```

---

## 7. Récapitulatif de la couverture

| Risque | Check | Sévérité fenêtre nocturne |
|--------|-------|---------------------------|
| Scraper tourne mais ne remonte rien | `check_freshness` (interroge la BDD) | **CRITICAL** |
| Daemon empilé / bloqué | `check_proc_*` | CRITICAL si empilement |
| Base injoignable | `check_pg` | CRITICAL |
| Disque de logs plein | `check_disk_logs` | WARNING → CRITICAL |

La logique clé : **hors fenêtre nocturne**, l'absence de données est normale et ne déclenche rien ; **pendant la fenêtre**, le moindre silence devient critique — c'est exactement le moment où les compositions et rumeurs déterminent les pronostics.
