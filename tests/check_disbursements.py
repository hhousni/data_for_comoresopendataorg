"""
Vérification rapide après export :
  → Total montant_verse_usd par bailleur, trié par ordre décroissant.
Usage : python3 tests/check_disbursements.py
"""
import pandas as pd
import sys
from pathlib import Path

OUTPUT = Path(__file__).parents[1] / "outputs" / "comores_aide_internationale_v2.xlsx"

if not OUTPUT.exists():
    print(f"❌  Fichier introuvable : {OUTPUT}")
    sys.exit(1)

df = pd.read_excel(OUTPUT, sheet_name="Données")

summary = (
    df.groupby("bailleur_de_fonds", dropna=False)
    .agg(
        nb_projets        = ("id_projet",         "count"),
        montant_verse_usd = ("montant_verse_usd",  "sum"),
        montant_verse_eur = ("montant_verse_eur",  "sum"),
        montant_verse_kmf = ("montant_verse_kmf",  "sum"),
    )
    .sort_values("montant_verse_usd", ascending=False)
    .reset_index()
)

# Keep top 4 + collapse the rest into "Autres"
TOP_N = 4
top    = summary.head(TOP_N)
others = summary.iloc[TOP_N:]
autres_row = pd.DataFrame([{
    "bailleur_de_fonds": "Autres",
    "nb_projets":        others["nb_projets"].sum(),
    "montant_verse_usd": others["montant_verse_usd"].sum(),
    "montant_verse_eur": others["montant_verse_eur"].sum(),
    "montant_verse_kmf": others["montant_verse_kmf"].sum(),
}])
summary = pd.concat([top, autres_row], ignore_index=True)

# ── Formatting ────────────────────────────────────────────────────────────
summary["montant_verse_usd_fmt"] = summary["montant_verse_usd"].apply(lambda x: f"${x:>15,.0f}")
summary["montant_verse_eur_fmt"] = summary["montant_verse_eur"].apply(lambda x: f"€{x:>15,.0f}")
summary["montant_verse_kmf_fmt"] = summary["montant_verse_kmf"].apply(lambda x: f"{x:>22,.0f} KMF")

total_usd = summary["montant_verse_usd"].sum()
total_eur = summary["montant_verse_eur"].sum()
total_kmf = summary["montant_verse_kmf"].sum()

# ── Print ─────────────────────────────────────────────────────────────────
col_w = 46
print()
print("=" * 110)
print(f"  Montants versés par bailleur de fonds — {OUTPUT.name}")
print("=" * 110)
print(f"  {'Bailleur':<{col_w}}  {'Projets':>7}  {'USD':>18}  {'EUR':>18}  {'KMF':>25}")
print("-" * 110)

for _, row in summary.iterrows():
    name = str(row["bailleur_de_fonds"])[:col_w]
    print(
        f"  {name:<{col_w}}  {int(row['nb_projets']):>7}  "
        f"{row['montant_verse_usd_fmt']}  "
        f"{row['montant_verse_eur_fmt']}  "
        f"{row['montant_verse_kmf_fmt']}"
    )

print("=" * 110)
print(
    f"  {'TOTAL':<{col_w}}  {int(summary['nb_projets'].sum()):>7}  "
    f"${total_usd:>17,.0f}  "
    f"€{total_eur:>17,.0f}  "
    f"{total_kmf:>22,.0f} KMF"
)
print("=" * 110)
print(f"\n  {len(df)} projets  |  {summary['bailleur_de_fonds'].nunique()} bailleurs\n")
