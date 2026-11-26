---
version: v02
locale: fr
tags:
  - _RNCP38777
  - _2026
  - aia
  - aia/bc04
  - ml4fd-project
---

*MACHINE LEARNING FOR FRAUD DETECTION* PROJECT
===

# 0. Introduction

> *Nicolas Pichon --- AIA RNCP 38777 / BC04 / Requirements / v02 --- 2026/11/26.*
---

## 0.1. Contexte

L'objectif de ce document est de présenter l'exercice de réalisation d'une solution d'apprentissage automatique pour la détection automatique de fraude bancaire \[R2\].

## 0.2. Enoncé

Il s'agit de prolonger les exercices de détection automatique de fraude par carte bancaire présentés en BC02 et BC03. Après avoir abordé le sujet en tant qu'*architecte de données* pour le système de données de *Stripe*, dans BC02, puis en tant qu'*ingénieur de données* dans BC03, je propose de continuer la mise en place du système de détection de fraude du point de vue du *Data Scientist* et du *MLOps* : 
- découpler le service de scoring de fraude et l'application en production,
- automatiser la surveillance des performances du modèle en production,
- automatiser le réentrainement du modèle en cas de détection de dérive,
- automatiser le déploiement d'un  nouveau modèle.

### 0.2.7. Livrables

#### 0.2.7.1. Synopsis

| #      | Livrable (fr)               |     |     |
| ------ | --------------------------- | --- | --- |
| **D1** | Présentation de la solution |     |     |
| **D2** | Code source                 |     |     |

---

## 0.3. Compréhension

### 0.3.3. Identification des exigences complémentaires

Les exigences ci-dessous sont celles qu'introduisent les compétences B4-C1 à B4-C6 spécifiées dans \[R1\] (cf. annexe ci-dessous), indépendamment de tout énoncé de projet. Elles tiennent le rôle que tenaient les *exigences complémentaires* du dossier BC03 : couvrir complètement les compétences du bloc. Dès qu'un sujet sera arrêté, les exigences qu'il porte explicitement devront être extraites séparément, et les exigences ci-dessous réduites à ce qu'il ne couvre pas.

| #           | Exigence                                                                                                                                        | Source              | Type |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ---- |
|             | **B4-C1 : Rédiger le cahier des charges**                                                                                                       |                     |      |
| **B4-EC01** | Produire un cahier des charges écrit et versionné, opposable à la réalisation (1)                                                               | B4-C1               | D    |
| **B4-EC02** | Énoncer le besoin de l'organisation : activité, problème métier, décision que la solution d'IA informe, population desservie                    | B4-C1/ \[R1, §Ex.\] | D    |
| **B4-EC03** | Chiffrer les besoins techniques : nature et volumétrie des données, débit d'appels attendu, latence maximale de réponse, disponibilité visée    | B4-C1               | D    |
| **B4-EC04** | Chiffrer les besoins économiques : coût d'exploitation visé, gain attendu, et seuil de performance à partir duquel la solution est rentable (2) | B4-C1               | D    |
| **B4-EC05** | Fixer des critères d'acceptation mesurables : métriques du modèle, seuils, et mode de mesure de chacun (3)                                      | B4-C1               | D    |
| **B4-EC06** | Nommer le référentiel d'accessibilité retenu et le niveau visé, et énumérer les surfaces de la solution qui y sont soumises (4)                 | B4-C1               | D    |
| **B4-EC07** | Spécifier les exigences d'accessibilité applicables à chaque surface restituant un résultat de la solution à un utilisateur humain              | B4-C1/ B4-EC06      | D/F  |

| #           | Exigence                                                                                                                                             | Source         | Type |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ---- |
|             | **B4-C2 : Créer l'algorithme**                                                                                                                       |                |      |
| **B4-EC08** | Caractériser le jeu de données d'entraînement : volume, schéma, déséquilibre des classes, antériorité disponible, fuites potentielles                | B4-C2          | D    |
| **B4-EC09** | Justifier le choix de la famille d'algorithme par les caractéristiques du jeu d'entraînement (5)                                                     | B4-C2/ B4-EC08 | D    |
| **B4-EC10** | Documenter le protocole d'entraînement et d'évaluation : découpage, validation, métriques, hyperparamètres retenus                                   | B4-C2          | D    |
| **B4-EC11** | Évaluer le modèle contre les critères d'acceptation du cahier des charges, et conclure explicitement sur la conformité (6)                           | B4-C2/ B4-EC05 | D    |
| **B4-EC12** | Rendre l'entraînement reproductible : dépendances figées, graine fixée, version du jeu de données référencée                                         | B4-C2          | A    |
| **B4-EC13** | Produire le modèle comme un artefact persistant, versionné et retrouvable par le service de prédiction et par le réentraînement                      | B4-C2          | A    |
| **B4-EC14** | Partager le même code de préparation des *features* entre l'entraînement et le service de prédiction (7)                                             | B4-C2/ B4-C3   | A    |
| **B4-EC15** | Restituer la sortie du modèle sous une forme exploitable par une technologie d'assistance : valeur textuelle, sans dépendance à la couleur seule (8) | B4-C2/ B4-EC07 | F    |

| #           | Exigence                                                                                                                       | Source        | Type |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------ | ------------- | ---- |
|             | **B4-C3 : Adapter l'infrastructure par une API**                                                                               |               |      |
| **B4-EC16** | Exposer la solution d'IA par une API de prédiction déployée et interrogeable par un tiers                                      | B4-C3         | A    |
| **B4-EC17** | Documenter le contrat de l'API dans une spécification interrogeable : routes, schémas de requête et de réponse, codes d'erreur | B4-C3         | D    |
| **B4-EC18** | Raccorder l'API aux magasins de l'infrastructure : lecture des données d'entrée, écriture des prédictions (9)                  | B4-C3         | A    |
| **B4-EC19** | Persister chaque appel et sa réponse : horodatage, entrée, score, décision, version du modèle servi                            | B4-C3/ B4-C6  | F    |
| **B4-EC20** | Valider les données entrantes de l'API et retourner une erreur explicite en cas de requête non conforme                        | B4-C3         | F    |
| **B4-EC21** | Exposer un point de contrôle de santé du service et la version du modèle servi                                                 | B4-C3/ B4-C6  | F    |
| **B4-EC22** | Externaliser les secrets et restreindre l'accès à l'API                                                                        | B4-C3         | A    |
| **B4-EC23** | Déployer dans une configuration de production, *cloud* ou *on-premise*, hors carnet interactif (10)                            | B4-C3/ \[R1\] | DM   |

| #           | Exigence                                                                                                                | Source         | Type |
| ----------- | ----------------------------------------------------------------------------------------------------------------------- | -------------- | ---- |
|             | **B4-C4 : Concevoir les pipelines d'intégration et de déploiement continus**                                            |                |      |
| **B4-EC24** | Déclarer la chaîne d'intégration et de déploiement continus comme du code versionné dans le dépôt                       | B4-C4          | A    |
| **B4-EC25** | Déclencher l'intégration automatiquement à chaque modification du code : construction, analyse statique, tests          | B4-C4          | A    |
| **B4-EC26** | Bloquer le déploiement lorsqu'une étape d'intégration échoue (11)                                                       | B4-C4/ B4-EC25 | F    |
| **B4-EC27** | Déclencher le déploiement sans intervention manuelle, et le tracer jusqu'au *commit* et à la version de modèle déployés | B4-C4          | A    |
| **B4-EC28** | Tester la solution au-delà du code applicatif : contrat de l'API, préparation des *features*, chargement du modèle (12) | B4-C4/ B4-C2   | F    |
| **B4-EC29** | Gérer les secrets par le magasin de secrets de la chaîne, sans valeur en clair dans le dépôt                            | B4-C4/ B4-EC22 | A    |
| **B4-EC30** | Permettre le retour à la version précédente du service                                                                  | B4-C4          | F    |

| #           | Exigence                                                                                                                                 | Source         | Type |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ---- |
|             | **B4-C5 : Développer le réentraînement**                                                                                                 |                |      |
| **B4-EC31** | Implémenter le réentraînement comme un script exécutable sans intervention manuelle                                                      | B4-C5          | A    |
| **B4-EC32** | Déclencher le réentraînement par un ordonnanceur, et déclarer sa cadence                                                                 | B4-C5          | A    |
| **B4-EC33** | Déclarer et implémenter la politique de déclenchement : périodique, et/ou sur franchissement d'un seuil de dérive ou de performance (13) | B4-C5/ B4-C6   | F    |
| **B4-EC34** | Comparer le modèle candidat au modèle en service sur les critères du cahier des charges avant toute promotion                            | B4-C5/ B4-EC05 | F    |
| **B4-EC35** | Versionner chaque modèle produit avec le jeu de données, les paramètres et les métriques qui l'ont produit                               | B4-C5/ B4-EC13 | A    |
| **B4-EC36** | Implémenter la promotion du candidat et le retour au modèle précédent sans interruption du service de prédiction                         | B4-C5/ B4-EC30 | F    |
| **B4-EC37** | Journaliser chaque exécution de réentraînement : début, fin, état, volumétrie, métriques obtenues, décision de promotion                 | B4-C5          | F    |

| #           | Exigence                                                                                                                              | Source         | Type |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ---- |
|             | **B4-C6 : Piloter la performance**                                                                                                    |                |      |
| **B4-EC38** | Mettre en œuvre un outil de monitoring parmi ceux nommés dans \[R1\] : *Evidently* ou *Aporia* (14)                                   | B4-C6/ \[R1\]  | A    |
| **B4-EC39** | Surveiller la dérive des données d'entrée et la dérive des prédictions                                                                | B4-C6          | F    |
| **B4-EC40** | Surveiller la performance du modèle en production contre les critères d'acceptation du cahier des charges (15)                        | B4-C6/ B4-EC05 | F    |
| **B4-EC41** | Définir la source et le délai d'obtention de la vérité-terrain en production, et dater le verdict de sa date de connaissance (16)     | B4-C6/ B4-EC40 | A    |
| **B4-EC42** | Surveiller la qualité des données reçues en production : schéma, valeurs manquantes, valeurs aberrantes, fraîcheur                    | B4-C6          | F    |
| **B4-EC43** | Surveiller la performance technique du service contre les cibles du cahier des charges : latence, taux d'erreur, disponibilité, débit | B4-C6/ B4-EC03 | F    |
| **B4-EC44** | Alerter sur franchissement de seuil, sur un canal nommé, et désigner le destinataire de l'alerte                                      | B4-C6          | F    |
| **B4-EC45** | Conserver l'historique des mesures, afin de pouvoir constater une évolution dans le temps long (17)                                   | B4-C6          | A    |

| #        | Exigence                                                                                                                                      | Source           | Type |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- | ---- |
|          | **Autres sources**                                                                                                                            |                  |      |
| **B4-EC46** | Livrer une présentation de la solution d'IA répondant au cahier des charges, au format d'un outil de présentation                             | \[R1, §Livr.\]   | L    |
| **B4-EC47** | Héberger le code sur **GitHub**                                                                                                               | \[R1, §Livr.\]   | L    |
| **B4-EC48** | Tenir la présentation du ou des pipelines en 5 minutes, et prévoir 10 minutes de questions (18)                                               | \[R1, §Ex.\]     | L    |
| **B4-EC49** | Produire un rapport ou un tableau de bord de monitoring consultable et présentable devant le jury                                             | B4-C6/ \[R1, §Ex.\] | L    |
| **B4-EC50** | Tenir une matrice de traçabilité compétence → exigence → élément de la solution → emplacement du code (19)                                    | \[R1, §Ex.\]     | D    |
| **B4-EC51** | Démontrer la chaîne complète en fonctionnement sur un exemple : appel de l'API, intégration continue, déploiement, réentraînement, monitoring | \[R1, §Ex.\]     | DM   |

> _Légendes : F : fonctionnelle, L : livrable, A : architecturale, D : documentaire, DM : démonstration, Ex. : format de l'examen, Livr. : livrables attendus._

###### Remarques

- (1) B4-EC01 : le cahier des charges n'est pas seulement un livrable de B4-C1, c'est la *référence* à laquelle B4-C2 (« conforme aux spécifications du cahier des charges ») et B4-C6 (« respecte les spécifications du cahier des charges ») sont explicitement adossées. Deux compétences sur six se mesurent contre ce document : s'il n'énonce pas de critère mesurable, elles deviennent indémontrables.
- (2) B4-EC04 : B4-C1 nomme les besoins « techniques *et économiques* ». La dimension économique est propre à BC04 et n'avait pas d'équivalent en BC03. À défaut d'un chiffrage, la clause reste non couverte.
- (3) B4-EC05 : l'exigence porte sur la *mesurabilité*, pas sur le niveau atteint. Un critère dont le mode de mesure n'est pas spécifié ne peut pas être surveillé en production (B4-EC40), et ferme donc la boucle de B4-C6.
- (4) B4-EC06 : l'accessibilité est citée deux fois dans \[R1\] (B4-C1 et B4-C2), ce qui en fait une exigence structurante et non une mention de principe. Une API de prédiction n'a pas d'interface humaine : la clause porte donc sur les surfaces qui *restituent* un résultat à une personne (rapport, tableau de bord, notification, interface de démonstration). Le cahier des charges doit nommer ces surfaces pour que l'exigence devienne vérifiable.
- (5) B4-EC09 : prouve la clause « adapté aux données d'entraînement » de B4-C2. Le choix doit se déduire des caractéristiques relevées en B4-EC08, et non d'une préférence d'outil.
- (6) B4-EC11 : prouve la clause « conforme aux spécifications du cahier des charges » de B4-C2. La conclusion doit être explicite, y compris lorsqu'elle est négative.
- (7) B4-EC14 : évite l'écart entre entraînement et service (*train/serve skew*). Reprend l'exigence de réutilisabilité du *preprocessing* du dossier BC03 (B3-EE07), ici au titre de l'adaptation de l'infrastructure.
- (8) B4-EC15 : lecture de la clause « besoins spécifiques, notamment en termes d'accessibilité » de B4-C2, appliquée à la *sortie* de l'algorithme. Un score rendu uniquement par une couleur ou par une position graphique n'est pas restituable par un lecteur d'écran.
- (9) B4-EC18 : B4-C3 dit « *adapter* l'infrastructure de données de l'organisation », et non « construire un service isolé ». L'exigence porte donc sur le raccordement de l'API à des magasins existants, dans les deux sens.
- (10) B4-EC23 : \[R1\] dit « pour accueillir la solution d'IA *en production* », ce qui exclut la démonstration depuis un carnet interactif tout en acceptant l'*on-premise*. Reprise de B3-EC27 (du dossier BC03).
- (11) B4-EC26 : sans barrière de qualité, une chaîne d'intégration n'intègre rien : elle ne fait que déployer. La clause « automatiser le processus de déploiement » de B4-C4 suppose que l'automatisation décide, et pas seulement qu'elle exécute.
- (12) B4-EC28 : une chaîne qui ne teste que le code applicatif laisse passer les deux pannes propres à une solution d'IA : un modèle introuvable et des *features* calculées différemment. Lie B4-C4 à B4-C2.
- (13) B4-EC33 : B4-C5 dit « automatiser le processus de *Machine Learning* », ce qui est plus large que « réentraîner périodiquement ». La politique de déclenchement est le point de jonction entre B4-C5 et B4-C6 : c'est le monitoring qui fournit le signal de dérive, et le réentraînement qui y répond.
- (14) B4-EC38 : cas rare où le référentiel nomme les technologies acceptables (« comme *Aporia* ou *Evidently* »). La formulation « comme » laisse la porte ouverte à un équivalent, mais retenir l'un des deux outils cités supprime le besoin de justifier le choix devant le jury.
- (15) B4-EC40 : prouve la clause finale de B4-C6 (« s'assurer qu'elle respecte les spécifications du cahier des charges dans un environnement de production »). Distinct de B4-EC39 : la dérive se mesure sans vérité-terrain, la performance ne se mesure pas sans elle.
- (16) B4-EC41 : la performance en production n'est calculable qu'une fois le verdict réel connu, ce qui survient après la prédiction. Reprise de l'exigence de datation du verdict du dossier BC03 (B3-EI02). Sans cette décision, B4-EC40 n'est pas implémentable.
- (17) B4-EC45 : « piloter » suppose de comparer dans le temps, donc de conserver les mesures au-delà de la durée de la démonstration. Implique de paramétrer les durées supérieures à celle de la démonstration, comme en BC03 (B3-EI12).
- (18) B4-EC48 : 5 minutes contraignent le contenu de la présentation autant qu'une exigence fonctionnelle contraint le code. Les 10 minutes de questions portent sur « le dossier de projet *et* la présentation », ce qui fait du dossier une pièce opposable même s'il ne figure pas dans la liste des livrables.
- (19) B4-EC50 : \[R1\] prévoit « un questionnement complémentaire \[...\] pour évaluer les compétences qui ne sont pas couvertes par le projet ». La matrice sert à montrer ce que le projet couvre, et donc à circonscrire ce qui restera à démontrer à l'oral. Reprise de B3-EC07 ( dossier BC03).

---

## Annexe : Compétences à démontrer

Compétences à démontrer dans le réalisation de l'exercice. D'après \[R1\].

| **#**     | Compétence                                                                                                                                                                                                                                                           |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **B4-C1** | **Rédiger** un cahier des charges pour la solution d'intelligence artificielle, afin de répondre aux besoins techniques et économiques de l'organisation, en tenant compte de l'accessibilité pour les personnes en situation de handicap.                           |
| **B4-C2** | **Créer** un algorithme d'intelligence artificielle adapté aux données d'entraînement et conforme aux spécifications du cahier des charges, en veillant à répondre aux besoins spécifiques, notamment en termes d'accessibilité.                                     |
| **B4-C3** | **Adapter** l'infrastructure de données de l'organisation à travers la construction d'API pour accueillir la solution d'IA en production ;                                                                                                                           |
| **B4-C4** | **Concevoir** des pipelines d'intégration et déploiement continu pour automatiser le processus de déploiement d'une solution d'IA ;                                                                                                                                  |
| **B4-C5** | **Développer** des scripts de réentrainement des modèles pour automatiser le processus de *Machine Learning* ;                                                                                                                                                       |
| **B4-C6** | **Piloter** la performance de la solution d'IA dans l'infrastructure à travers la mise en place d'outils de monitoring (comme *Aporia* ou *Evidently*) pour s'assurer qu'elle respecte les spécifications du cahier des charges dans un environnement de production. |

---

## Références
- \[R1\] [Jedha Certification - AIA/BC04](https://app.jedha.co/certifications/rncp38777bc04)
- \[R2\] [*Automatic Fraud Detection* Project - AIA/BC03](https://app.jedha.co/course/etl-with-airflow-lds/automatic-fraud-detection-project-lds)
- \[R3\] [Fraudulent Payments full dataset](https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv)
- \[R4\] [Real-time payment API](https://huggingface.co/spaces/sdacelo/real-time-fraud-detection)

---
