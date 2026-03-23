"""
Extraction des donnees de commerce international pour les Comores
==================================================================
Format de sortie : 2 feuilles Excel (inspire du dataset IATI comoresopendata.org)
  - Feuille "Donnees"    : tableau plat tidy (1 ligne = 1 observation)
  - Feuille "Metadonnees": dictionnaire des colonnes (colonne | source | description)

Sources :
  - WITS/Comtrade (Banque Mondiale) - API SDMX publique, sans cle
    Exports + imports par section douaniere et par pays partenaire, 2001-2022
  - World Bank Indicators API - publique, sans cle
    PIB, balance commerciale, remittances, inflation, solde courant

Pays : Union des Comores (COM / KM / M49:174)
Derniere mise a jour : 2026-03-23
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
COUNTRY_WITS  = "COM"
COUNTRY_WB    = "KM"
OUTPUT_DIR    = Path(__file__).parent.parent.parent / "outputs"
OUTPUT_FILE   = OUTPUT_DIR / "comores_commerce_international.xlsx"
WITS_BASE     = "https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-trade"
WB_BASE       = "https://api.worldbank.org/v2"
REQUEST_DELAY = 1.5

# ---------------------------------------------------------
# REFERENTIELS
# ---------------------------------------------------------
HS_SECTION_LABELS = {
    "01-05_Animal"    : "Produits animaux (ch.01-05)",
    "06-15_Vegetable" : "Produits vegetaux - vanille, girofle (ch.06-15)",
    "16-24_FoodProd"  : "Produits alimentaires transformes (ch.16-24)",
    "25-26_Minerals"  : "Mineraux, pierres (ch.25-26)",
    "27-27_Fuels"     : "Combustibles, hydrocarbures (ch.27)",
    "28-38_Chemicals" : "Produits chimiques - ylang-ylang (ch.28-38)",
    "39-40_PlastiRub" : "Plastiques et caoutchouc (ch.39-40)",
    "41-43_HidesSkin" : "Cuirs, peaux, fourrures (ch.41-43)",
    "44-49_Wood"      : "Bois, papier, edition (ch.44-49)",
    "50-63_TextCloth" : "Textiles et vetements (ch.50-63)",
    "64-67_Footwear"  : "Chaussures, coiffures (ch.64-67)",
    "68-71_StoneGlass": "Pierres, verre, bijoux (ch.68-71)",
    "72-83_Metals"    : "Metaux et ouvrages en metaux (ch.72-83)",
    "84-85_Machinery" : "Machines, equipements electriques (ch.84-85)",
    "86-89_Transport" : "Vehicules, bateaux, avions (ch.86-89)",
    "90-99_Misc"      : "Instruments, optique, divers (ch.90-99)",
    "Total"           : "TOTAL toutes sections",
}

PARTNER_LABELS = {
    "FRA": "France", "ARE": "Emirats Arabes Unis", "IND": "Inde",
    "SGP": "Singapour", "JPN": "Japon", "TUR": "Turquie",
    "CHN": "Chine", "ZAF": "Afrique du Sud", "KEN": "Kenya",
    "MYS": "Malaisie", "THA": "Thailande", "PAK": "Pakistan",
    "USA": "Etats-Unis", "GBR": "Royaume-Uni", "DEU": "Allemagne",
    "BEL": "Belgique", "NLD": "Pays-Bas", "ITA": "Italie",
    "SAU": "Arabie Saoudite", "MDG": "Madagascar", "TZA": "Tanzanie",
    "MUS": "Maurice", "MOZ": "Mozambique", "EGY": "Egypte",
    "IDN": "Indonesie", "AUS": "Australie", "BRA": "Bresil",
    "OMN": "Oman", "QAT": "Qatar", "KWT": "Koweit",
    "IRN": "Iran", "PRT": "Portugal", "ESP": "Espagne",
    "CHE": "Suisse", "TUN": "Tunisie", "MAR": "Maroc",
    "NOR": "Norvege", "SWE": "Suede", "CAN": "Canada",
    "WLD": "Monde (total)",
}

# ---------------------------------------------------------
# DICTIONNAIRE DES COLONNES (contenu de la feuille Metadonnees)
# ---------------------------------------------------------
METADATA = [
    ("annee",                "WITS/Comtrade",  "Annee de reference (2001-2022)"),
    ("flux",                 "WITS/Comtrade",  "Direction du flux : 'Export', 'Import' ou -- pour indicateurs macro"),
    ("type_donnee",          "WITS/Comtrade",  "Granularite : par_section (section douaniere), par_partenaire (pays) ou macro_worldbank"),
    ("section_code",         "WITS/Comtrade",  "Code section douaniere WITS (ex: 06-15_Vegetable). Pour macro_worldbank : code indicateur WB"),
    ("section_libelle",      "WITS/Comtrade",  "Libelle de la section douaniere ou de l indicateur macro"),
    ("partenaire_code_iso3", "WITS/Comtrade",  "Code ISO3 du pays partenaire (ex: FRA, IND). Vide si par_section ou macro_worldbank"),
    ("partenaire_pays",      "WITS/Comtrade",  "Nom du pays partenaire. Vide si par_section ou macro_worldbank"),
    ("valeur_usd_millers",   "WITS/Comtrade",  "Valeur en USD milliers (x1000) pour donnees commerciales. Pour indicateurs en % : valeur brute."),
    ("valeur_usd_millions",  "WITS/Comtrade",  "Valeur en USD millions pour donnees commerciales. Pour indicateurs en % : valeur brute."),
    ("reporter_code",        "WITS/Comtrade",  "Code ISO3 du pays declarant (toujours COM = Comores)"),
    ("reporter_pays",        "WITS/Comtrade",  "Nom du pays declarant (toujours Union des Comores)"),
    ("source_base",          "Systeme",        "Base source : WITS-CMT (UN Comtrade via WITS) ou World Bank WDI"),
    ("date_extraction",      "Systeme",        "Date d extraction depuis l API (YYYY-MM-DD)"),
]

# ---------------------------------------------------------
# HELPERS API
# ---------------------------------------------------------
def fetch_wits(url, label):
    try:
        print(f"  -> {label} ...", end=" ", flush=True)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        n_obs = len(root.findall(".//{*}Obs"))
        n_ser = len(root.findall(".//{*}Series"))
        print(f"OK  ({n_ser} series, {n_obs} obs)")
        return root
    except Exception as e:
        print(f"ERREUR: {e}")
        return None


def parse_wits(root):
    rows = []
    for series in root.findall(".//{*}Series"):
        s = dict(series.attrib)
        for obs in series.findall(".//{*}Obs"):
            row = dict(obs.attrib)
            row.update(s)
            rows.append(row)
    return rows


# ---------------------------------------------------------
# COLLECTE
# ---------------------------------------------------------
def collect_by_section(flow):
    indicator = "XPRT-TRD-VL" if flow == "X" else "MPRT-TRD-VL"
    label     = "Exports par section douaniere" if flow == "X" else "Imports par section douaniere"
    root = fetch_wits(
        f"{WITS_BASE}/reporter/{COUNTRY_WITS}/year/ALL/partner/WLD/product/all/indicator/{indicator}",
        label
    )
    time.sleep(REQUEST_DELAY)
    if root is None:
        return []

    date_ext = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for r in parse_wits(root):
        code = r.get("PRODUCTCODE", "")
        if code == "Total":
            continue
        rows.append({
            "annee"               : int(r.get("TIME_PERIOD", 0)),
            "flux"                : "Export" if flow == "X" else "Import",
            "type_donnee"         : "par_section",
            "section_code"        : code,
            "section_libelle"     : HS_SECTION_LABELS.get(code, code),
            "partenaire_code_iso3": "",
            "partenaire_pays"     : "",
            "valeur_usd_millers"  : float(r.get("OBS_VALUE", 0)),
            "valeur_usd_millions" : round(float(r.get("OBS_VALUE", 0)) / 1000, 4),
            "reporter_code"       : "COM",
            "reporter_pays"       : "Union des Comores",
            "source_base"         : r.get("DATASOURCE", "WITS-CMT"),
            "date_extraction"     : date_ext,
        })
    return rows


def collect_by_partner(flow):
    indicator = "XPRT-TRD-VL" if flow == "X" else "MPRT-TRD-VL"
    label     = "Exports par pays partenaire" if flow == "X" else "Imports par pays partenaire"
    root = fetch_wits(
        f"{WITS_BASE}/reporter/{COUNTRY_WITS}/year/ALL/partner/all/product/Total/indicator/{indicator}",
        label
    )
    time.sleep(REQUEST_DELAY)
    if root is None:
        return []

    date_ext = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for r in parse_wits(root):
        partner = r.get("PARTNER", "")
        if partner == "WLD":
            continue
        rows.append({
            "annee"               : int(r.get("TIME_PERIOD", 0)),
            "flux"                : "Export" if flow == "X" else "Import",
            "type_donnee"         : "par_partenaire",
            "section_code"        : "",
            "section_libelle"     : "",
            "partenaire_code_iso3": partner,
            "partenaire_pays"     : PARTNER_LABELS.get(partner, partner),
            "valeur_usd_millers"  : float(r.get("OBS_VALUE", 0)),
            "valeur_usd_millions" : round(float(r.get("OBS_VALUE", 0)) / 1000, 4),
            "reporter_code"       : "COM",
            "reporter_pays"       : "Union des Comores",
            "source_base"         : r.get("DATASOURCE", "WITS-CMT"),
            "date_extraction"     : date_ext,
        })
    return rows


def collect_wb_macro():
    indicators = {
        "TX.VAL.MRCH.CD.WT": "Exports marchandises totaux (USD courants)",
        "TM.VAL.MRCH.CD.WT": "Imports marchandises totaux (USD courants)",
        "NY.GDP.MKTP.CD"   : "PIB nominal (USD courants)",
        "NE.TRD.GNFS.ZS"   : "Commerce de biens et services (% du PIB)",
        "BN.CAB.XOKA.CD"   : "Solde du compte courant (USD courants)",
        "FP.CPI.TOTL.ZG"   : "Taux d inflation, prix a la consommation (% annuel)",
    }
    rows = []
    date_ext = datetime.now().strftime("%Y-%m-%d")
    for code, desc in indicators.items():
        url = f"{WB_BASE}/country/{COUNTRY_WB}/indicator/{code}?format=json&date=1990:2025&per_page=100"
        try:
            print(f"  -> WB {code} ...", end=" ", flush=True)
            r = requests.get(url, timeout=25)
            r.raise_for_status()
            d = r.json()
            data = d[1] if len(d) > 1 and d[1] else []
            pts = [(x["date"], x["value"]) for x in data if x["value"] is not None]
            print(f"OK  ({len(pts)} points)")
            is_usd = "USD" in desc
            for annee, val in pts:
                rows.append({
                    "annee"               : int(annee),
                    "flux"                : "--",
                    "type_donnee"         : "macro_worldbank",
                    "section_code"        : code,
                    "section_libelle"     : desc,
                    "partenaire_code_iso3": "",
                    "partenaire_pays"     : "",
                    "valeur_usd_millers"  : round(val / 1000, 4) if is_usd else round(val, 4),
                    "valeur_usd_millions" : round(val / 1e6, 4) if is_usd else round(val, 4),
                    "reporter_code"       : "COM",
                    "reporter_pays"       : "Union des Comores",
                    "source_base"         : "World Bank WDI",
                    "date_extraction"     : date_ext,
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"ERREUR: {e}")
    return rows


# ---------------------------------------------------------
# ECRITURE EXCEL : 2 feuilles (Donnees + Metadonnees)
# ---------------------------------------------------------
def write_excel(df, output_path):
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        # -- Feuille 1 : Donnees ------------------------------
        df.to_excel(writer, sheet_name="Donnees", index=False)
        ws_d = writer.sheets["Donnees"]

        h_font  = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
        h_fill  = PatternFill("solid", fgColor="1F3864")
        h_align = Alignment(horizontal="center", vertical="center")
        for cell in ws_d[1]:
            cell.font      = h_font
            cell.fill      = h_fill
            cell.alignment = h_align

        col_widths = {
            "annee": 8, "flux": 10, "type_donnee": 20,
            "section_code": 24, "section_libelle": 48,
            "partenaire_code_iso3": 22, "partenaire_pays": 30,
            "valeur_usd_millers": 20, "valeur_usd_millions": 20,
            "reporter_code": 16, "reporter_pays": 26,
            "source_base": 16, "date_extraction": 18,
        }
        for i, col in enumerate(df.columns, 1):
            ws_d.column_dimensions[get_column_letter(i)].width = col_widths.get(col, 18)
        ws_d.freeze_panes = "A2"

        # -- Feuille 2 : Metadonnees --------------------------
        df_meta = pd.DataFrame(METADATA, columns=["colonne", "source", "description"])
        df_meta.to_excel(writer, sheet_name="Metadonnees", index=False)
        ws_m = writer.sheets["Metadonnees"]

        m_font = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
        m_fill = PatternFill("solid", fgColor="2E75B6")
        for cell in ws_m[1]:
            cell.font      = m_font
            cell.fill      = m_fill
            cell.alignment = Alignment(horizontal="center")

        ws_m.column_dimensions["A"].width = 28
        ws_m.column_dimensions["B"].width = 20
        ws_m.column_dimensions["C"].width = 72
        ws_m.freeze_panes = "A2"
        for row in ws_m.iter_rows(min_row=2, max_col=3):
            row[2].alignment = Alignment(wrap_text=True)
        for i in range(2, len(df_meta) + 2):
            ws_m.row_dimensions[i].height = 36


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("\n" + "="*60)
    print("  EXTRACTION COMMERCE INTERNATIONAL - COMORES")
    print("  Format : 2 feuilles (Donnees + Metadonnees)")
    print("="*60)

    all_rows = []

    print("\n[1/5] Exports par section douaniere (WITS)")
    all_rows += collect_by_section("X")

    print("\n[2/5] Imports par section douaniere (WITS)")
    all_rows += collect_by_section("M")

    print("\n[3/5] Exports par pays partenaire (WITS)")
    all_rows += collect_by_partner("X")

    print("\n[4/5] Imports par pays partenaire (WITS)")
    all_rows += collect_by_partner("M")

    print("\n[5/5] Indicateurs macro World Bank")
    all_rows += collect_wb_macro()

    df = pd.DataFrame(all_rows)
    df = df.sort_values(["type_donnee", "flux", "annee"]).reset_index(drop=True)

    n_total   = len(df)
    n_section = int((df["type_donnee"] == "par_section").sum())
    n_partner = int((df["type_donnee"] == "par_partenaire").sum())
    n_macro   = int((df["type_donnee"] == "macro_worldbank").sum())
    years     = sorted(df[df["annee"] > 0]["annee"].unique())
    yr_range  = f"{min(years)}-{max(years)}" if years else "?"

    print(f"\n{'='*60}")
    print(f"  Total lignes      : {n_total:,}")
    print(f"    par_section     : {n_section:,}")
    print(f"    par_partenaire  : {n_partner:,}")
    print(f"    macro_worldbank : {n_macro:,}")
    print(f"  Couverture annees : {yr_range}")
    print(f"  Colonnes          : {len(df.columns)}")
    print(f"{'='*60}\n")

    write_excel(df, OUTPUT_FILE)

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"  OK : {OUTPUT_FILE.name}  ({size_kb:.0f} KB)")
    print(f"     Feuille Donnees     : {n_total:,} lignes x {len(df.columns)} colonnes")
    print(f"     Feuille Metadonnees : {len(METADATA)} colonnes documentees\n")


if __name__ == "__main__":
    main()
