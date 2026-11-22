ML4FD : Machine Learning for fraud Detection
===

Détection automatique de fraude : du modèle à son exploitation
---

Code source du projet *ml4fd*, quatrième bloc du parcours Jedha *Lead Data Science & Engineering** (certification RNCP 38777, bloc BC04 : *construire,
déployer et piloter des solutions d'IA*).

Le projet prolonge la même application à travers plusieurs rôles : 
la détection automatique de fraude a d'abord été envisagée en tant qu'*architecte de données* dans le second bloc de la certification, 
puis en tant qu'*ingénieur de données* dans le troisième bloc, dans la réalisation d'un pipeline qui ingère les transactions par carte, les score et en rend compte.
Dans ce projet, la détection de fraude automatique est abordée du point de vue de l'ingénieur *MLOps*. 

La question posée est le traitement du modèle de scoring *après* sa première mise en production : 
comment sa performance est surveillée, comment il est réentraîné sans intervention humaine, et comment une nouvelle version atteint le service automatiquement.

> État : en construction. La plateforme de données est héritée du bbloic 3 et fonctionne ; 
> la boucle fermée se construit lot par lot. Voir *Avancement* ci-dessous.

---

## Ce que porte ce dépôt

Deux boucles, délibérément tenues séparées.

La **boucle du code** (B4-C4) est déclenchée par un *commit* : l'intégration
continue construit, analyse et teste chaque modification, et la livraison
continue publie les images. Son artefact est une version du service.

La **boucle du modèle** (B4-C5, B4-C6) est déclenchée par les données : la
surveillance mesure la dérive et la performance, un seuil déclenche le
réentraînement, et un modèle candidat n'est promu que s'il bat celui en service
sur les critères du cahier des charges. Son artefact est une version de modèle
dans le registre.

Elles se touchent en un seul point : le registre de modèles. Promouvoir un
modèle y est un changement d'état, pas un redéploiement --- le chemin de
scoring résout son modèle depuis le registre à chaque rafraîchissement.

## Architecture

Six services, un projet Docker Compose (`aia-ml4fd`) :

| Service | Rôle | Publié sur |
|---|---|---|
| `postgres` | trois bases, un rôle de connexion par service | `localhost:8432` |
| `airflow-init` | migre le schéma de métadonnées, puis s'arrête | --- |
| `airflow-scheduler` | exécute les DAG (LocalExecutor) | --- |
| `airflow-webserver` | l'interface d'Airflow | http://localhost:8082 |
| `mlflow` | serveur de suivi, registre de modèles, relais d'artefacts | http://localhost:8002 |
| `dashboard` | Streamlit, en lecture seule sur ce qu'il affiche | http://127.0.0.1:8502 |
| `producer` | la source simulée du flux de transactions | http://127.0.0.1:8802/current-transactions |
| `scoring-service` | le service de scoring, pour les tiers | http://127.0.0.1:8012/docs |

Tous les ports publiés sur la machine sont de la famille **8\*\*2**, pour que la
pile tourne à côté de celle de BC03, qui publie 5432, 8080, 5000 et 8501. Dans le
réseau Compose, chaque service garde son port propre (`mlflow:5000`,
`scoring-service:5000`, `producer:4000`…).

L'unique instance PostgreSQL porte trois bases, chacune avec son propriétaire :

- **`ml4fd-1`** --- les données métier : zone brute, quarantaine, transactions,
  vérité-terrain, *features*, scores, fraudes, rapports, exécutions ;
- **`airflow`** --- les métadonnées d'orchestration seulement, propriété de
  `fd_airflow`, à qui `CONNECT` est refusé sur la base métier ;
- **`mlflow`** --- le magasin de suivi et le registre de modèles, propriété de
  `fd_mlflow`.

Les séparer est ce qui empêche un DAG de détenir les droits du propriétaire sur
les tables de fraude, et ce qui permet de détruire et reconstruire les
métadonnées de l'ordonnanceur sans toucher aux données.

## Prérequis

- Docker Desktop (conteneurs Linux)
- Windows et PowerShell --- le lanceur de tâches est un script PowerShell
- Python 3.11 pour la suite de tests, conforme à `requires-python` et aux images

### Environnement de développement

Un environnement conda dédié, en 3.11, puis les dépendances que le projet
déclare lui-même :

```powershell
conda create -n aia-ml4fd python=3.11
conda activate aia-ml4fd
python -m pip install -e ".[dev]"
```

`pip` et non `conda` pour les dépendances, et ce n'est pas un détail :
`pyproject.toml` les déclare déjà, et un `environment.yml` en ferait une
seconde liste à tenir d'accord avec la première. Conda fournit l'interpréteur,
le projet fournit ses dépendances --- exactement ce que fait la chaîne
d'intégration, avec la même commande.

L'extra `dev` ajoute `pytest` et `ruff`. Le `-e` installe `ml4fd` en mode
éditable, ce qui rend `import ml4fd` résoluble sans passer par `python -m`.

VS Code retient cet environnement à l'ouverture du dossier :
`.vscode/settings.json` le désigne par `${userHome}/miniconda3/envs/aia-ml4fd`. Si
votre miniconda est installé ailleurs, c'est la ligne à corriger.

## Démarrage

Sous Windows 11, dans un terminal PowerShell :

```powershell
.\fd-manager.ps1 setup     # cree .env, genere le poivre et les mots de passe
.\fd-manager.ps1 up        # construit et demarre les six services
.\fd-manager.ps1 init      # applique le DDL (rejouable)
.\fd-manager.ps1 load      # charge l'historique etiquete
.\fd-manager.ps1 train     # entraine, evalue et enregistre un modele
.\fd-manager.ps1 ingest    # un cycle temps reel : collecte, scoring, notification
```

`setup` doit venir en premier : il écrit `.env`, qui n'est jamais versionné, et
sans le poivre la pseudonymisation serait réversible.

Lancez le script sans argument pour la liste complète des tâches et la
signification des codes de référence du dossier :

```powershell
.\fd-manager.ps1
```

## Arborescence

```
ml4fd/             le paquet : ingestion, features, scoring, entrainement, purge...
airflow/dags/      un DAG par cadence : ingest, daily, verdicts, train, purge
sql/               DDL, controles, features, rapports -- le SQL vit dans des fichiers
sql/initdb/        premier demarrage seulement : cree les deux bases de service
dashboard/         les pages Streamlit, en lecture seule
mlflow/            l'image du serveur de suivi
tools/             points d'entree en ligne de commande des taches et des controles
tests/             la suite : sans base, sans Airflow, sans reseau
governance.yml     donnees personnelles, retention, roles -- la politique ecrite
docker-compose.yml la configuration deployee
fd-manager.ps1     le lanceur de taches, en place d'un Makefile
```

## Tests

```powershell
.\fd-manager.ps1 test      # python -m pytest -q
.\fd-manager.ps1 lint      # ruff
.\fd-manager.ps1 check     # acceptation, tracabilite, tests, concordance
```

La suite s'exécute sans base de données, sans Airflow et sans accès réseau : ce
qui ne peut pas être vérifié ainsi est vérifié sur le source lui-même. Deux
tests sont ignorés tant que la matrice de traçabilité n'est pas générée.

## Intégration continue

`.github/workflows/ci.yml` s'exécute à chaque poussée et à chaque *pull
request* : Python 3.11, `pip install -e ".[dev]"`, `ruff check`, un contrôle
des fins de ligne de `sql/initdb/*.sh`, puis la suite. Un contrôle en échec ne
refuse pas une fusion à lui seul --- c'est une règle de protection de branche
sur `main` qui s'en charge.

## Secrets et gouvernance

`.env` n'est jamais versionné, et `.env.example` documente chaque variable
qu'il doit porter. Le poivre du HMAC de pseudonymisation, le mot de passe du
tableau de bord et les deux mots de passe des bases de service sont générés par
`setup`.

Chaque rôle détient ce que son usage exige, et rien de plus : le tableau de
bord lit les huit tables que ses pages affichent et ne peut pas écrire ;
`fd_airflow` possède ses métadonnées et ne peut pas atteindre les données
métier. `governance.yml` porte la politique écrite --- champs personnels,
durées de conservation, rôles.

## Avancement

| Lot | Objet                                                          | Critère d'acceptation                                                                                                                                                                 | État     |
| --- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| B4-L0  | Reprise de la base de code de BC03                              | Aucune marque de lot non résolue dans les sources : la matrice sépare ce que BC04 a produit de ce qu'il a reçu                                                             | fait     |
| B4-L1  | Trois bases, un rôle / service                                 | Le rôle d'Airflow échoue sur un `SELECT` dans les tables de fraude, et les DAG passent toujours par leurs rôles propres                                                               | fait     |
| B4-L2  | Dépôt & intégration continue                                   | Un *commit* qui casse un test ne peut pas être fusionné, et la page du dépôt le montre                                                                                                | fait     |
| B4-L3  | Porte de promotion : aliasing `champion`, comparaison appariée | un entraînement délibérément dégradé produit une version enregistrée mais **non promue** ; le scoring sert toujours l'ancien champion, et la décision est en base avec son motif      | fait     |
| B4-L4  | Surveillance et réentraînement automatique                     | dérive injectée, alerte émise, réentraînement déclenché, champion promu, nouveau modèle servi --- sans intervention, et en moins de cinq minutes                                      | fait     |
| B4-L5  | Découplage du service de scoring                               | un tiers obtient un score par HTTP sans rien connaître de la pile ; l'appel est retrouvable en base ; une promotion change la version annoncée par le point de santé sans redémarrage | fait     |
| B4-L6  | Livraison continue des images                                  | le service en production se relie à un *commit* précis par l'étiquette de son image, et la version précédente se remet en service                                                     | fait     |

Les critères sont résumés ici ; leur énoncé complet, les lots de travail et les
pièges connus vivent dans le plan de travail, sous `step-0/ws/`.

## Références

- \[R1\] [Jedha Certification --- AIA/BC04](https://app.jedha.co/certifications/rncp38777bc04)
- \[R2\] [*Automatic Fraud Detection* Project --- AIA/BC03](https://app.jedha.co/course/etl-with-airflow-lds/automatic-fraud-detection-project-lds)
- \[R3\] [Fraudulent Payments full dataset](https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv)
- \[R4\] [Real-time payment API](https://huggingface.co/spaces/sdacelo/real-time-fraud-detection)
