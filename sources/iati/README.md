# IATI – Guide de mise à jour des données Comores

Ce dossier contient les scripts utilisés pour extraire, comparer et valider les données IATI liées aux Comores (code pays: `KM`).

Règle importante pour tout agent ou personne qui travaille ici :

- Avant toute modification, toute mise à jour ou toute relance des scripts, lire ce README jusqu’au bout.
- Les scripts ne sont pas automatisés. La mise à jour se fait manuellement en relançant les script(s) appropriés.
- La source de vérité pour la reconstruction des données est le script principal d’extraction.

---

## 1. Objectif

Le but est de produire un dataset exploitable pour publication sur comoresopendata.org, en s’appuyant sur les données IATI disponibles via :

- d-portal
- IATI Tables
- éventuellement le datastore IATI si des données manquent

Le dossier contient à la fois :

- l’extraction des données utiles,
- la comparaison entre sources,
- la vérification des écarts,
- le suivi des activités manquantes.

---

## 2. Fichiers présents

### Script principal

- `iati_data_extraction.py`
  - Extraction principale des activités et des secteurs.
  - Construire le dataset nettoyé pour publication.
  - C’est le script à relancer pour mettre à jour les données.

### Scripts de validation / comparaison

- `iati_tables_comparison.py`
  - Compare les données IATI Tables et d-portal.
  - Vérifie si les écarts viennent du FX, des filtres, ou des activités manquantes.

- `_benchmark_ids.py`
  - Compare les identifiants d’activité entre les deux sources.
  - Sert à voir si les mêmes projets sont présents des deux côtés.

- `_benchmark_validation.py`
  - Analyse multi-niveaux : totaux globaux, par année, contrôle d’activité.
  - C’est le script de validation la plus complète.

- `_check1_validation.py`
  - Validation du total D+E pour les activités non multinationales.
  - Référence : valeur d-portal de référence.

- `_track_missing_activities.py`
  - Identifie les activités présentes dans d-portal mais absentes de IATI Tables.
  - Génère un export Excel des activités manquantes.

- `_datastore_check.py`
  - Vérifie si les identifiants manquants existent dans le datastore IATI.
  - Permet de distinguer :
    - données non indexées dans IATI Tables,
    - données réellement absentes / retirées.

---

## 3. Workflow de mise à jour recommandé

### Étape 1 : relancer l’extraction principale

Commandes à lancer :

```bash
cd /Users/thedreamer/Desktop/my-comores-projects/comoresopendata-data/sources/iati
python3 iati_data_extraction.py
```

Ce script:

- interroge l’API d-portal;
- charge les activités Comores;
- récupère les secteurs, statuts, bailleurs, etc.;
- nettoie et enrichit les colonnes;
- produit le dataset principal.

C’est la base de la mise à jour des données.

### Étape 2 : vérifier la cohérence des résultats

Après l’extraction, lancer les validations :

```bash
python3 iati_tables_comparison.py
python3 _benchmark_validation.py
python3 _check1_validation.py
```

### Étape 3 : contrôler les écarts importants

Si des écarts apparaissent :

```bash
python3 _track_missing_activities.py
```

Puis, si besoin, vérifier dans le datastore :

```bash
IATI_KEY=your_key_here python3 _datastore_check.py
```

---

## 4. Ce qu’il faut interpréter

### Cas 1 : mêmes identifiants, montants différents

Cela suggère généralement :

- différence de taux de change,
- méthode de conversion différente,
- données mises à jour à des dates différentes.

### Cas 2 : activités absentes dans IATI Tables

Cela peut être dû à :

- indexation incomplète,
- publication récente,
- données non intégrées dans les tables,
- activité inexistante ou retirée.

### Cas 3 : écart supérieur à 5–10 %

Cela demande un contrôle manuel et ne doit pas être traité comme une simple différence de conversion.

---

## 5. Règles de sécurité et de qualité

Avant toute mise à jour :

- vérifier que les scripts tournent sans erreurs réseau,
- vérifier les filtres pays (`KM`),
- vérifier les filtres multi-pays / non-multinational,
- ne pas modifier les variables de comparaison sans validation préalable,
- ne pas modifier le script principal d’extraction sans comparaison préalable.

Le commentaire dans `iati_tables_comparison.py` est explicite :

> NE PAS MODIFIER le script d-portal avant validation.

Cette règle doit être respectée.

---

## 6. Rôle de l’agent / assistant lors d’une mise à jour

Lorsqu’un agent doit mettre à jour les données, il doit :

1. lire ce README,
2. lancer le script principal : `iati_data_extraction.py`,
3. relancer les scripts de validation pertinents,
4. vérifier les écarts avant de valider le résultat,
5. documenter les différences observées,
6. ne pas considérer le travail comme terminé si la comparaison montre un écart non expliqué.

---

## 7. Résumé de la procédure

Mise à jour standard :

```bash
python3 iati_data_extraction.py
python3 iati_tables_comparison.py
python3 _benchmark_validation.py
python3 _check1_validation.py
```

Contrôle des activités absentes :

```bash
python3 _track_missing_activities.py
```

Contrôle datastore si nécessaire :

```bash
IATI_KEY=your_key_here python3 _datastore_check.py
```

---

## 8. Conclusion

Ce dossier est conçu pour un processus de mise à jour manuelle, contrôlée et vérifiable.

Il ne fournit pas de pipeline automatique complet, mais il donne bien la manière correcte de :

- rafraîchir les données,
- valider la qualité,
- comparer les sources,
- détecter les écarts ou données manquantes.

En pratique, la commande la plus importante reste :

```bash
python3 iati_data_extraction.py
```

C’est elle qui met à jour les données de base.
