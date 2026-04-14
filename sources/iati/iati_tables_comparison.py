"""
IATI Tables — Prototype de comparaison pour les Comores (KM)
=============================================================
Source alternative : IATI Tables Datasette (officiel IATI Secretariat)
URL : https://datasette-tables.iatistandard.org/iati.json

Objectif : vérifier que les totaux (décaissements + dépenses) correspondent
           à ceux du script actuel basé sur d-portal.

NE PAS MODIFIER le script d-portal (iati_data_extraction.py) avant validation.

Différences clés vs d-portal :
  - Type de transaction : codes numériques ('3'=Disbursement, '4'=Expenditure)
    au lieu de lettres ('D', 'E')
  - Répartition multi-pays déjà faite dans transaction_breakdown (via percentage_used)
  - Conversion USD déjà intégrée avec taux IMF par date de transaction
  - Pas de clé API requise

Lancer :
    python3 sources/iati/iati_tables_comparison.py
"""

import time
import requests
import pandas as pd

COUNTRY_CODE   = "KM"
DATASETTE_URL  = "https://datasette-tables.iatistandard.org/iati.json"
KMF_RATE       = 491.96775   # Parité fixe : 1 EUR = 491.96775 KMF
PAGE_SIZE      = 2000        # Lignes par appel (LIMIT SQL)
# User-Agent requis pour passer Cloudflare (sans JS challenge)
HEADERS        = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


# ── Utilitaire de requête Datasette ──────────────────────────────────────
def query_datasette(sql: str, label: str = "") -> pd.DataFrame:
    """Interroge le Datasette IATI Tables et retourne un DataFrame.
    Gère la pagination via LIMIT/OFFSET SQL standard.
    """
    all_rows = []
    columns  = None
    offset   = 0

    while True:
        paginated_sql = f"{sql} LIMIT {PAGE_SIZE} OFFSET {offset}"
        try:
            r = requests.get(
                DATASETTE_URL,
                params={"sql": paginated_sql},
                timeout=60,
                headers=HEADERS,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  ⚠ Erreur réseau ({label}) : {e}")
            break

        data = r.json()
        if not data.get("ok"):
            print(f"  ⚠ Erreur API ({label}) : {data}")
            break

        rows    = data.get("rows", [])
        columns = data.get("columns", [])

        if not rows:
            break

        all_rows.extend(rows)
        print(f"  [{label}] {len(all_rows)} lignes récupérées …")

        if len(rows) < PAGE_SIZE:
            break   # dernière page

        offset += PAGE_SIZE
        time.sleep(0.3)

    if all_rows and columns:
        return pd.DataFrame(all_rows, columns=columns)
    elif columns:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# 1. ACTIVITÉS — récupérer via recipientcountry
# ═══════════════════════════════════════════════════════════════════════════
print("1. Récupération des activités Comores …")

activities_sql = f"""
    SELECT DISTINCT
        a.iatiidentifier           AS aid,
        a.title_narrative          AS title,
        a.description              AS description,
        a.reportingorg_narrative   AS reporting,
        a.reportingorg_ref         AS reporting_ref,
        a.activitystatus_code      AS status_code,
        a.activitystatus_codename  AS status_label,
        a.actualstart              AS date_debut,
        a.actualend                AS date_fin,
        rc.percentage              AS country_percent
    FROM activity a
    JOIN recipientcountry rc ON rc._link_activity = a._link
    WHERE rc.code = '{COUNTRY_CODE}'
"""

activities = query_datasette(activities_sql, label="activités")
print(f"  → {len(activities)} activités")

if activities.empty:
    print("\n⛔ Aucune activité récupérée. Vérifiez l'accès au Datasette.")
    print("   URL testée :", DATASETTE_URL)
    print("   Si l'URL est inaccessible, téléchargez le SQLite depuis https://tables.iatistandard.org/")
    raise SystemExit(1)

# country_percent : 100 si non renseigné (activité dédiée aux Comores)
activities["country_percent"] = (
    pd.to_numeric(activities["country_percent"], errors="coerce").fillna(100)
)
activities["projet_multi_pays"] = activities["country_percent"].apply(
    lambda x: "Non" if x == 100 else "Oui"
)


# ═══════════════════════════════════════════════════════════════════════════
# 2. TRANSACTIONS — via transaction_breakdown (répartition multi-pays déjà faite)
#    Types 3 = Disbursement, 4 = Expenditure  (≡ D+E dans d-portal)
# ═══════════════════════════════════════════════════════════════════════════
print("\n2. Récupération des transactions (Disbursement + Expenditure) …")

trans_sql = f"""
    SELECT
        tb.iatiidentifier           AS aid,
        tb.transactiontype_code     AS trans_code,
        tb.transactiontype_codename AS trans_label,
        tb.transactiondate_isodate  AS trans_date,
        tb.value_usd                AS trans_usd,
        tb.value_currency           AS trans_currency,
        tb.value                    AS trans_value_original,
        tb.value_valuedate          AS value_date,
        tb.sector_code,
        tb.sector_codename,
        tb.recipientcountry_code,
        tb.percentage_used
    FROM transaction_breakdown tb
    WHERE tb.recipientcountry_code = '{COUNTRY_CODE}'
      AND tb.transactiontype_code IN ('3', '4')
"""
# Note : dans IATI Tables, la répartition par country_percent est déjà appliquée
#        via percentage_used → pas besoin de multiplier manuellement.

transactions = query_datasette(trans_sql, label="transactions D+E")
print(f"  → {len(transactions)} lignes de transactions")

transactions["trans_usd"] = pd.to_numeric(transactions["trans_usd"], errors="coerce").fillna(0)


# ═══════════════════════════════════════════════════════════════════════════
# 3. BUDGETS — table budget (conversion USD non garantie dans IATI Tables)
# ═══════════════════════════════════════════════════════════════════════════
print("\n3. Récupération des budgets …")

budget_sql = f"""
    SELECT
        b._link_activity,
        b.value          AS budget_value,
        b.value_currency AS budget_currency,
        b.iatiidentifier AS aid
    FROM budget b
    WHERE b._link_activity IN (
        SELECT _link_activity FROM recipientcountry WHERE code = '{COUNTRY_CODE}'
    )
"""
# ⚠  IATI Tables ne fournit pas de colonne budget_usd pré-calculée
#    (contrairement aux transactions). La conversion devra être faite
#    manuellement si nécessaire, comme dans le script d-portal actuel.

budgets = query_datasette(budget_sql, label="budgets")
print(f"  → {len(budgets)} lignes de budget")
budgets["budget_value"] = pd.to_numeric(budgets["budget_value"], errors="coerce").fillna(0)


# ═══════════════════════════════════════════════════════════════════════════
# 4. AGRÉGATIONS
# ═══════════════════════════════════════════════════════════════════════════
print("\n4. Agrégation des montants par activité …")

# Décaissements + Dépenses (D+E / types 3+4)
spend_agg = (
    transactions.groupby("aid")["trans_usd"]
    .sum()
    .reset_index(name="montant_verse_usd_iati_tables")
)

# Budget par activité
if not budgets.empty:
    budget_agg = (
        budgets.groupby("aid")["budget_value"]
        .sum()
        .reset_index(name="budget_raw_iati_tables")
    )
else:
    budget_agg = pd.DataFrame(columns=["aid", "budget_raw_iati_tables"])


# ═══════════════════════════════════════════════════════════════════════════
# 5. DATASET FINAL DE COMPARAISON
# ═══════════════════════════════════════════════════════════════════════════
print("\n5. Construction du dataset de comparaison …")

dataset = (
    activities
    .merge(spend_agg,  on="aid", how="left")
    .merge(budget_agg, on="aid", how="left")
)
dataset["montant_verse_usd_iati_tables"] = dataset["montant_verse_usd_iati_tables"].fillna(0)

# Grand total
grand_total_usd = dataset["montant_verse_usd_iati_tables"].sum()
grand_total_kmf = grand_total_usd / 655.957 * KMF_RATE  # USD→EUR via cours approx, puis EUR→KMF
# Note : pour un calcul exact EUR/KMF, il faudrait la colonne value_eur
#        (non disponible dans transaction_breakdown sans taux EUR séparé)

print(f"\n{'='*60}")
print(f"  Activités Comores         : {len(dataset)}")
print(f"  Total Décaissements (USD) : ${grand_total_usd:>16,.0f}")
print(f"{'='*60}")

# Résumé par statut
print("\n  Répartition par statut :")
print(dataset.groupby("status_label")["montant_verse_usd_iati_tables"]
      .agg(nb_activites="count", total_usd="sum")
      .sort_values("total_usd", ascending=False)
      .to_string())

# Projets multi-pays vs dédiés
print("\n  Projets multi-pays vs dédiés :")
print(dataset["projet_multi_pays"].value_counts().to_string())


# ═══════════════════════════════════════════════════════════════════════════
# 6. EXPORT EXCEL DE COMPARAISON
# ═══════════════════════════════════════════════════════════════════════════
import os
os.makedirs("outputs", exist_ok=True)
output_file = "outputs/comores_iati_tables_comparison.xlsx"

export_cols = [
    "aid", "title", "reporting", "status_label",
    "date_debut", "date_fin",
    "montant_verse_usd_iati_tables",
    "budget_raw_iati_tables",
    "projet_multi_pays",
    "country_percent",
]
export_cols_existing = [c for c in export_cols if c in dataset.columns]

with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    dataset[export_cols_existing].to_excel(writer, sheet_name="Comparaison", index=False)

    notes = pd.DataFrame([
        {"point": "Source",            "valeur": "IATI Tables Datasette (datasette-tables.iatistandard.org)"},
        {"point": "Transactions",      "valeur": "Types 3 (Disbursement) + 4 (Expenditure) — équivalent D+E dans d-portal"},
        {"point": "Répartition pays",  "valeur": "Déjà appliquée via percentage_used dans transaction_breakdown"},
        {"point": "Conversion USD",    "valeur": "Taux IMF par date de transaction (pré-calculé dans value_usd)"},
        {"point": "Budget USD",        "valeur": "⚠ Non converti — budget_raw_iati_tables est en devise originale"},
        {"point": "Filtre pays",       "valeur": f"recipientcountry.code = '{COUNTRY_CODE}'"},
        {"point": "Date extraction",   "valeur": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")},
    ])
    notes.to_excel(writer, sheet_name="Notes_methodologie", index=False)

    wb  = writer.book
    hdr = wb.add_format({"bold": True, "fg_color": "#D7E4BC", "border": 1})
    for sheet, df in [("Comparaison", dataset[export_cols_existing]),
                      ("Notes_methodologie", notes)]:
        ws = writer.sheets[sheet]
        for col_num, col_name in enumerate(df.columns):
            ws.write(0, col_num, col_name, hdr)

print(f"\nFichier de comparaison créé : {output_file}")
print("\n⚡ Prochaine étape : comparer montant_verse_usd_iati_tables avec")
print("   montant_verse_usd du script d-portal pour valider l'équivalence.")
