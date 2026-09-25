# IATI – Mise à jour des données Comores

Lire ce document avant chaque mise à jour.

## Objectif

Mettre à jour les données IATI des Comores (code pays: KM) pour publication et validation.

## Scripts importants

- `iati_data_extraction.py` = script principal de mise à jour des données
- `iati_tables_comparison.py` = compare IATI Tables vs d-portal
- `_check1_validation.py` = validation principale du total D+E
- `_benchmark_validation.py` = validation complète par niveau
- `_track_missing_activities.py` = activités absentes dans IATI Tables
- `_datastore_check.py` = vérifie les identifiants manquants dans le datastore IATI

## Procédure standard

1. Se placer dans le dossier du projet :

```bash
cd /Users/thedreamer/Desktop/my-comores-projects/comoresopendata-data/sources/iati
```

2. Lancer la mise à jour principale :

```bash
python3 iati_data_extraction.py
```

3. Vérifier la cohérence des données :

```bash
python3 _check1_validation.py
```

4. Si nécessaire, lancer les contrôles complémentaires :

```bash
python3 iati_tables_comparison.py
python3 _benchmark_validation.py
python3 _track_missing_activities.py
```

5. Si des activités manquent encore, vérifier le datastore :

```bash
IATI_KEY=your_key_here python3 _datastore_check.py
```

## Règles de validation

- Si l’écart est inférieur à 5 %, c’est généralement acceptable.
- Si l’écart est entre 5 % et 10 %, vérifier le taux de change et les données manquantes.
- Si l’écart dépasse 10 %, analyser avant de valider.
- Ne pas modifier le script principal sans validation préalable.

## Interprétation rapide

- Même identifiants, montants différents = problème de FX / méthode de conversion.
- Identifiants manquants = activité absente dans une source.
- Écart important = besoin de contrôle manuel.

## Conclusion

La mise à jour se fait manuellement en relançant le script principal et en validant ensuite les écarts.

Ce dossier n’est pas automatisé.
