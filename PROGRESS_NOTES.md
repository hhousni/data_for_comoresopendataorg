# Progress Notes — Données Comores

_Last updated: 23 March 2026_

---

## Statut actuel

### ✅ Fichier complété et commité
- **Fichier** : `outputs/flux_commerciaux_comores_exports_imports_par_section.xlsx`
- **Taille** : 986 KB — 15 420 lignes × 13 colonnes
- **Commit** : `b6440fd` sur `main`
- **Script** : `sources/comtrade/comtrade_extraction.py`

### Contenu du fichier
| Colonne | Description |
|--------|-------------|
| `annee` | Année (1995–2021) |
| `flux` | Export / Import |
| `section_code` | Code section douanière HS (WITS) |
| `section_libelle` | Libellé français |
| `partenaire_code_iso3` | ISO3 pays partenaire |
| `partenaire_pays` | Nom pays (français) |
| `valeur_usd` | Valeur exacte en USD |
| `valeur_eur` | Valeur en EUR (taux BCE annuel) |
| `valeur_kmf` | Valeur en KMF (taux fixe 491.968) |
| `taux_usd_eur` | Taux BCE utilisé |
| `reporter_code` | Toujours COM |
| `reporter_pays` | Toujours Union des Comores |
| `date_extraction` | Date d'extraction |

### Couverture
- **Années** : 1995–2021 (WITS ne dispose pas de données Comores au-delà de 2021)
- **Sections HS** : 16 sections (01-05 à 90-99)
- **Pays partenaires** : 212

---

## Droits de republication ✅

- Source : **UN Comtrade via WITS (Banque Mondiale)** — licence **CC BY 4.0**
- Republication gratuite autorisée, avec attribution obligatoire
- Mention à afficher : _"Source : Nations Unies — UN Comtrade, via l'API WITS (Banque Mondiale). Données en libre accès sous licence CC BY 4.0."_
- Le travail de traitement (conversion EUR/KMF, libellés français, nettoyage) appartient au projet

---

## Prochaine étape — Extension des données au-delà de 2021

WITS retourne HTTP 400 pour les Comores sur 2022–2025 (retard de déclaration à UN Comtrade).

**Option : API directe UN Comtrade (tier gratuit)**
1. Créer un compte sur [comtrade.un.org](https://comtrade.un.org)
2. Aller sur [comtradedeveloper.un.org](https://comtradedeveloper.un.org)
3. Products → Free APIs → Subscribe
4. Récupérer la clé dans Profile → Show keys

Une fois la clé obtenue, mettre à jour le script pour appeler :
```
https://comtradeapi.un.org/data/v1/get/C/A/HS
  ?reporterCode=174   ← code M49 des Comores
  &period=2022,2023,2024
  &subscription-key=<CLE>
```
Cela permettrait d'ajouter les années 2022–2024 et potentiellement une granularité HS6 (ex : vanille spécifiquement).

---

## Publication sur comoresopendata.org

Titre : **Flux commerciaux des Comores : exports et imports par section douanière**

Description suggérée :
> Données de commerce international des Comores extraites de WITS/Comtrade (Banque Mondiale). Couvre les exports et imports par section douanière HS et par pays partenaire, de 1995 à 2021. Valeurs en USD, EUR et KMF (franc comorien). Source : UN Comtrade via WITS API — taux de change BCE — taux fixe KMF/EUR.

---

## Commandes pour reprendre le travail

```bash
# Activer l'environnement et re-générer le fichier
cd /Users/thedreamer/Desktop/opendatacomores/comoresopendata-data
source /Users/thedreamer/Desktop/opendatacomores/open_data_v2/.venv/bin/activate
python3 sources/comtrade/comtrade_extraction.py

# Commiter après modification
git add outputs/ sources/
git commit -m "feat: update trade data"
git push origin main
```
