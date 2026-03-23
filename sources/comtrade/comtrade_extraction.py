"""
Extraction des données de commerce international pour les Comores
=================================================================
Sources :
  - WITS (World Integrated Trade Solution, Banque Mondiale) — sans clé API
    → Données Comtrade/ONU via WITS SDMX API
    → Exports/imports par produit (HS2), par partenaire, toutes années disponibles
  - World Bank API — sans clé API
    → Commerce marchandises, remittances, PIB


Pays : Comores (code ISO3 = COM, code M49 = 174)
Dernière mise à jour du script : 2026-03-23
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
COUNTRY_WITS   = "COM"          # Code Comores dans WITS
COUNTRY_WB     = "KM"           # Code Comores dans World Bank API
OUTPUT_DIR     = Path(__file__).parent.parent.parent / "outputs"
OUTPUT_FILE    = OUTPUT_DIR / "comores_commerce.xlsx"
WITS_BASE      = "https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-trade"
WB_BASE        = "https://api.worldbank.org/v2"
REQUEST_DELAY  = 1.5            # secondes entre appels WITS (politesse)

# ─────────────────────────────────────────────
# LIBELLÉS HS2 (chapitres douaniers) en français
# ─────────────────────────────────────────────
HS2_LABELS = {
    "01": "Animaux vivants",
    "02": "Viandes et abats",
    "03": "Poissons et crustacés",
    "04": "Produits laitiers, œufs, miel",
    "05": "Autres produits d'origine animale",
    "06": "Plantes vivantes, floriculture",
    "07": "Légumes, plantes, tubercules",
    "08": "Fruits comestibles",
    "09": "Café, thé, épices (vanille, girofle)",
    "10": "Céréales",
    "11": "Produits de la minoterie",
    "12": "Graines et fruits oléagineux",
    "13": "Gommes, résines, extraits végétaux",
    "14": "Matières végétales à tresser",
    "15": "Graisses et huiles animales/végétales",
    "16": "Préparations de viande/poisson",
    "17": "Sucres et sucreries",
    "18": "Cacao et ses préparations",
    "19": "Préparations céréalières, pâtisseries",
    "20": "Préparations de légumes/fruits",
    "21": "Préparations alimentaires diverses",
    "22": "Boissons, liquides alcooliques, vinaigres",
    "23": "Résidus alimentaires, aliments animaux",
    "24": "Tabacs",
    "25": "Sel, soufre, terres, pierres",
    "26": "Minerais, scories, cendres",
    "27": "Combustibles minéraux, huiles minérales",
    "28": "Produits chimiques inorganiques",
    "29": "Produits chimiques organiques",
    "30": "Produits pharmaceutiques",
    "31": "Engrais",
    "32": "Extraits tannants, colorants, peintures",
    "33": "Huiles essentielles (ylang-ylang, parfums)",
    "34": "Savons, cires, agents de surface",
    "35": "Substances albumineuses, amidons",
    "36": "Matières explosives, allumettes",
    "37": "Produits photographiques",
    "38": "Produits chimiques divers",
    "39": "Matières plastiques",
    "40": "Caoutchouc",
    "41": "Peaux brutes, cuirs",
    "42": "Ouvrages en cuir",
    "43": "Pelleteries et fourrures",
    "44": "Bois, charbon de bois",
    "45": "Liège",
    "46": "Ouvrages de sparterie, vannerie",
    "47": "Pâtes de bois, papier recyclé",
    "48": "Papiers et cartons",
    "49": "Produits de l'édition, presse",
    "50": "Soie",
    "51": "Laine, poils fins",
    "52": "Coton",
    "53": "Autres fibres textiles végétales",
    "54": "Filaments synthétiques/artificiels",
    "55": "Fibres synthétiques/artificielles discontinues",
    "56": "Ouates, feutres, ficelles",
    "57": "Tapis, revêtements de sol textiles",
    "58": "Tissus spéciaux",
    "59": "Tissus imprégnés, stratifiés",
    "60": "Étoffes de bonneterie",
    "61": "Vêtements en bonneterie",
    "62": "Vêtements autres qu'en bonneterie",
    "63": "Autres articles textiles confectionnés",
    "64": "Chaussures",
    "65": "Coiffures",
    "66": "Parapluies, cannes",
    "67": "Plumes, fleurs artificielles",
    "68": "Ouvrages en pierre, plâtre, ciment",
    "69": "Produits céramiques",
    "70": "Verre et ouvrages en verre",
    "71": "Perles, pierres précieuses, métaux précieux",
    "72": "Fonte, fer et acier",
    "73": "Ouvrages en fonte, fer ou acier",
    "74": "Cuivre",
    "75": "Nickel",
    "76": "Aluminium",
    "78": "Plomb",
    "79": "Zinc",
    "80": "Étain",
    "81": "Autres métaux communs",
    "82": "Outils, coutellerie",
    "83": "Ouvrages divers en métaux communs",
    "84": "Chaudières, machines, appareils mécaniques",
    "85": "Machines électriques, appareils électroniques",
    "86": "Véhicules ferroviaires",
    "87": "Voitures, tracteurs, véhicules automobiles",
    "88": "Navigation aérienne, spatiale",
    "89": "Navigation maritime",
    "90": "Instruments optiques, photo, médicaux",
    "91": "Horlogerie",
    "92": "Instruments de musique",
    "93": "Armes et munitions",
    "94": "Meubles, literie, luminaires",
    "95": "Jouets, jeux, articles de sport",
    "96": "Ouvrages divers",
    "97": "Objets d'art, collections",
    "99": "Marchandises non classées ailleurs",
}

# Libellés sections douanières WITS (groupes de chapitres HS retournés par l'API)
HS_SECTION_LABELS = {
    "01-05_Animal"      : "Produits animaux (ch.01-05)",
    "06-15_Vegetable"   : "Produits végétaux — vanille, girofle (ch.06-15)",
    "15-15_Animal&Veg"  : "Graisses animales et végétales (ch.15)",
    "16-24_FoodProd"    : "Produits alimentaires transformés (ch.16-24)",
    "25-26_Minerals"    : "Minéraux, pierres (ch.25-26)",
    "27-27_Fuels"       : "Combustibles, hydrocarbures (ch.27)",
    "28-38_Chemicals"   : "Produits chimiques — ylang-ylang (ch.28-38)",
    "39-40_PlastiRub"   : "Plastiques et caoutchouc (ch.39-40)",
    "41-43_HidesSkin"   : "Cuirs, peaux, fourrures (ch.41-43)",
    "44-49_Wood"        : "Bois, papier, édition (ch.44-49)",
    "50-63_TextCloth"   : "Textiles et vêtements (ch.50-63)",
    "64-67_Footwear"    : "Chaussures, coiffures (ch.64-67)",
    "68-71_StoneGlass"  : "Pierres, verre, bijoux (ch.68-71)",
    "72-83_Metals"      : "Métaux et ouvrages en métaux (ch.72-83)",
    "84-85_Machinery"   : "Machines, équipements électriques (ch.84-85)",
    "86-89_Transport"   : "Véhicules, bateaux, avions (ch.86-89)",
    "90-99_Misc"        : "Instruments, armes, divers (ch.90-99)",
    "Total"             : "TOTAL",
}

# Principaux pays partenaires des Comores
COUNTRY_LABELS = {
    "FRA": "France", "ARE": "Émirats Arabes Unis", "IND": "Inde",
    "SGP": "Singapour", "JPN": "Japon", "TUR": "Turquie",
    "CHN": "Chine", "ZAF": "Afrique du Sud", "KEN": "Kenya",
    "MYS": "Malaisie", "THA": "Thaïlande", "PAK": "Pakistan",
    "USA": "États-Unis", "GBR": "Royaume-Uni", "DEU": "Allemagne",
    "BEL": "Belgique", "NLD": "Pays-Bas", "ITA": "Italie",
    "SAU": "Arabie Saoudite", "MDG": "Madagascar", "TZA": "Tanzanie",
    "MUS": "Maurice", "REU": "Réunion", "MOZ": "Mozambique",
    "EGY": "Égypte", "IDN": "Indonésie", "AUS": "Australie",
    "BRA": "Brésil", "OMN": "Oman", "QAT": "Qatar",
    "WLD": "Monde entier",
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def fetch_wits(url: str, label: str) -> ET.Element | None:
    """Appelle l'API WITS SDMX et retourne la racine XML ou None."""
    try:
        print(f"  → Fetching {label} ...", end=" ", flush=True)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        series = root.findall(".//{*}Series")
        obs    = root.findall(".//{*}Obs")
        print(f"✓  ({len(series)} séries, {len(obs)} observations)")
        return root
    except Exception as e:
        print(f"✗  ERREUR: {e}")
        return None


def parse_wits_xml(root: ET.Element, extra_col: str | None = None) -> pd.DataFrame:
    """
    Parse le XML WITS SDMX en DataFrame.
    Chaque ligne = une observation (Series attrs + TIME_PERIOD + OBS_VALUE).
    extra_col : si fourni, nom de l'attribut Series à inclure comme colonne (ex: 'PRODUCT', 'PARTNER')
    """
    rows = []
    for series in root.findall(".//{*}Series"):
        s_attrs = dict(series.attrib)
        for obs in series.findall(".//{*}Obs"):
            row = {
                "annee"    : int(obs.attrib.get("TIME_PERIOD", 0)),
                "valeur_usd_millers": float(obs.attrib.get("OBS_VALUE", 0)),
            }
            if extra_col and extra_col in s_attrs:
                row[extra_col] = s_attrs[extra_col]
            rows.append(row)
    return pd.DataFrame(rows)


def fetch_wb_indicator(indicator: str, label: str) -> pd.DataFrame:
    """Appelle l'API World Bank pour un indicateur Comores et retourne un DataFrame."""
    url = f"{WB_BASE}/country/{COUNTRY_WB}/indicator/{indicator}?format=json&date=1990:2025&per_page=100"
    try:
        print(f"  → WB: {label} ...", end=" ", flush=True)
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        d = r.json()
        data = d[1] if len(d) > 1 and d[1] else []
        rows = [
            {"annee": int(x["date"]), "valeur": x["value"]}
            for x in data if x["value"] is not None
        ]
        df = pd.DataFrame(rows).sort_values("annee")
        print(f"✓  ({len(df)} points)")
        return df
    except Exception as e:
        print(f"✗  ERREUR: {e}")
        return pd.DataFrame(columns=["annee", "valeur"])


def style_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
    """Applique un style minimal (largeur colonnes, header gras)."""
    ws = writer.sheets[sheet_name]
    for col_idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
        ws.column_dimensions[chr(65 + col_idx)].width = min(max_len, 40)


# ─────────────────────────────────────────────
# COLLECTE DES DONNÉES
# ─────────────────────────────────────────────

def collect_annual_trade():
    """Exports + imports totaux par année (toutes les années disponibles)."""
    root_x = fetch_wits(
        f"{WITS_BASE}/reporter/{COUNTRY_WITS}/year/ALL/partner/WLD/product/Total/indicator/XPRT-TRD-VL",
        "Exports totaux annuels"
    )
    time.sleep(REQUEST_DELAY)
    root_m = fetch_wits(
        f"{WITS_BASE}/reporter/{COUNTRY_WITS}/year/ALL/partner/WLD/product/Total/indicator/MPRT-TRD-VL",
        "Imports totaux annuels"
    )
    time.sleep(REQUEST_DELAY)

    if root_x is None or root_m is None:
        return pd.DataFrame()

    df_x = parse_wits_xml(root_x).rename(columns={"valeur_usd_millers": "exports_usd_millers"})
    df_m = parse_wits_xml(root_m).rename(columns={"valeur_usd_millers": "imports_usd_millers"})

    df = df_x.merge(df_m, on="annee", how="outer").sort_values("annee")
    df["balance_usd_millers"] = df["exports_usd_millers"] - df["imports_usd_millers"]
    df["exports_usd_millions"]  = (df["exports_usd_millers"] / 1000).round(2)
    df["imports_usd_millions"]  = (df["imports_usd_millers"] / 1000).round(2)
    df["balance_usd_millions"]  = (df["balance_usd_millers"] / 1000).round(2)
    return df[["annee", "exports_usd_millions", "imports_usd_millions", "balance_usd_millions"]]


def collect_by_product(flow: str) -> pd.DataFrame:
    """Exports ou imports par chapitre HS2 et par année."""
    indicator = "XPRT-TRD-VL" if flow == "X" else "MPRT-TRD-VL"
    label     = "Exports par produit" if flow == "X" else "Imports par produit"
    root = fetch_wits(
        f"{WITS_BASE}/reporter/{COUNTRY_WITS}/year/ALL/partner/WLD/product/all/indicator/{indicator}",
        label
    )
    time.sleep(REQUEST_DELAY)
    if root is None:
        return pd.DataFrame()

    df = parse_wits_xml(root, extra_col="PRODUCTCODE")
    if df.empty:
        return df

    # Exclut l'agrégat total
    df = df[df["PRODUCTCODE"] != "Total"].copy()
    df["section"]  = df["PRODUCTCODE"]
    df["libelle"]  = df["PRODUCTCODE"].map(HS_SECTION_LABELS).fillna(df["PRODUCTCODE"])
    df["valeur_usd_millions"] = (df["valeur_usd_millers"] / 1000).round(3)
    return df[["annee", "section", "libelle", "valeur_usd_millions"]].sort_values(
        ["annee", "valeur_usd_millions"], ascending=[True, False]
    )


def collect_by_partner(flow: str) -> pd.DataFrame:
    """Exports ou imports par pays partenaire et par année."""
    indicator = "XPRT-TRD-VL" if flow == "X" else "MPRT-TRD-VL"
    label     = "Exports par partenaire" if flow == "X" else "Imports par partenaire"
    root = fetch_wits(
        f"{WITS_BASE}/reporter/{COUNTRY_WITS}/year/ALL/partner/all/product/Total/indicator/{indicator}",
        label
    )
    time.sleep(REQUEST_DELAY)
    if root is None:
        return pd.DataFrame()

    df = parse_wits_xml(root, extra_col="PARTNER")
    if df.empty:
        return df

    # Exclut "WLD" (agrégat monde), garde les pays
    df = df[df["PARTNER"] != "WLD"].copy()
    df["pays"] = df["PARTNER"].map(COUNTRY_LABELS).fillna(df["PARTNER"])
    df["valeur_usd_millions"] = (df["valeur_usd_millers"] / 1000).round(3)
    return df[["annee", "PARTNER", "pays", "valeur_usd_millions"]].sort_values(
        ["annee", "valeur_usd_millions"], ascending=[True, False]
    )


def collect_wb_supplementary() -> pd.DataFrame:
    """Données macro complémentaires via World Bank API."""
    indicators = {
        "TX.VAL.MRCH.CD.WT"  : "exports_marchandises_usd",
        "TM.VAL.MRCH.CD.WT"  : "imports_marchandises_usd",
        "BX.TRF.PWKR.CD.DT"  : "remittances_recues_usd",
        "NY.GDP.MKTP.CD"      : "pib_usd",
        "NE.TRD.GNFS.ZS"      : "commerce_pct_pib",
        "BN.CAB.XOKA.CD"      : "solde_compte_courant_usd",
        "FP.CPI.TOTL.ZG"      : "inflation_pct",
    }
    dfs = []
    for code, col in indicators.items():
        df = fetch_wb_indicator(code, col)
        if not df.empty:
            df = df.rename(columns={"valeur": col})
            dfs.append(df.set_index("annee"))
        time.sleep(0.5)

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, axis=1).reset_index()
    combined = combined.rename(columns={"index": "annee"})
    combined = combined.sort_values("annee")

    # Ajoute des colonnes calculées lisibles
    if "exports_marchandises_usd" in combined.columns:
        combined["exports_M_usd"] = (combined["exports_marchandises_usd"] / 1e6).round(2)
    if "imports_marchandises_usd" in combined.columns:
        combined["imports_M_usd"] = (combined["imports_marchandises_usd"] / 1e6).round(2)
    if "remittances_recues_usd" in combined.columns:
        combined["remittances_M_usd"] = (combined["remittances_recues_usd"] / 1e6).round(2)
    if "pib_usd" in combined.columns:
        combined["pib_M_usd"] = (combined["pib_usd"] / 1e6).round(2)

    return combined


# ─────────────────────────────────────────────
# PIVOT : produits tops par année
# ─────────────────────────────────────────────

def make_product_pivot(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    """Crée un tableau pivotant sections douanières × années."""
    if df.empty:
        return df
    # Top N sections par valeur totale
    top_sections = (
        df.groupby("section")["valeur_usd_millions"]
        .sum()
        .nlargest(top_n)
        .index
    )
    df_top = df[df["section"].isin(top_sections)].copy()
    df_top["produit"] = df_top["libelle"]
    pivot = df_top.pivot_table(
        index="produit", columns="annee", values="valeur_usd_millions", aggfunc="sum"
    )
    pivot.columns = [str(c) for c in pivot.columns]
    pivot = pivot.fillna(0).sort_values(pivot.columns[-1], ascending=False)
    return pivot.reset_index()


def make_partner_pivot(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Top partenaires par valeur totale, sous forme pivot pays × années."""
    if df.empty:
        return df
    top_partners = (
        df.groupby("PARTNER")["valeur_usd_millions"]
        .sum()
        .nlargest(top_n)
        .index
    )
    df_top = df[df["PARTNER"].isin(top_partners)].copy()
    pivot = df_top.pivot_table(
        index="pays", columns="annee", values="valeur_usd_millions", aggfunc="sum"
    )
    pivot.columns = [str(c) for c in pivot.columns]
    pivot = pivot.fillna(0).sort_values(pivot.columns[-1], ascending=False)
    return pivot.reset_index()


# ─────────────────────────────────────────────
# FEUILLE RESUME
# ─────────────────────────────────────────────

def make_summary(df_trade: pd.DataFrame, df_wb: pd.DataFrame) -> pd.DataFrame:
    rows = [
        ("Source", "WITS/Comtrade (ONU) + World Bank API"),
        ("Pays", "Comores (Union des Comores)"),
        ("Date extraction", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("", ""),
        ("━━━ DONNÉES COMMERCE WITS/COMTRADE ━━━", ""),
    ]
    if not df_trade.empty:
        latest = df_trade.sort_values("annee").dropna(subset=["exports_usd_millions"]).iloc[-1]
        rows += [
            ("Dernière année disponible (WITS)", int(latest["annee"])),
            ("Exports totaux (M USD)", f"{latest['exports_usd_millions']:.2f}"),
            ("Imports totaux (M USD)", f"{latest['imports_usd_millions']:.2f}"),
            ("Balance commerciale (M USD)", f"{latest['balance_usd_millions']:.2f}"),
        ]
    if not df_wb.empty:
        rows.append(("", ""))
        rows.append(("━━━ DONNÉES BANQUE MONDIALE ━━━", ""))
        latest_wb = df_wb.sort_values("annee").iloc[-1]
        yr = int(latest_wb["annee"])
        if "pib_M_usd" in df_wb.columns:
            rows.append((f"PIB (M USD, {yr})", f"{latest_wb.get('pib_M_usd', 'N/A'):.1f}"))
        if "remittances_M_usd" in df_wb.columns:
            rows.append((f"Transferts diaspora reçus (M USD, {yr})", f"{latest_wb.get('remittances_M_usd', 'N/A'):.1f}"))
        if "commerce_pct_pib" in df_wb.columns:
            rows.append((f"Commerce / PIB (%, {yr})", f"{latest_wb.get('commerce_pct_pib', 'N/A'):.1f}"))
        if "inflation_pct" in df_wb.columns:
            rows.append((f"Inflation (%, {yr})", f"{latest_wb.get('inflation_pct', 'N/A'):.1f}"))
    rows += [
        ("", ""),
        ("━━━ PRODUITS CLÉS D'EXPORTATION ━━━", ""),
        ("09 - Épices", "Vanille (0905), Clous de girofle (0907), Ylang-ylang → chapitre 33"),
        ("Note", "Valeurs en USD milliers (WITS) ou USD (WB). Comores = petit pays, données complètes depuis ~2001."),
    ]
    return pd.DataFrame(rows, columns=["Indicateur", "Valeur"])


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("\n" + "="*60)
    print("  EXTRACTION DONNÉES COMMERCE — COMORES")
    print("="*60)

    # 1. Balance commerciale annuelle
    print("\n[1/6] Balance commerciale annuelle (WITS)")
    df_trade = collect_annual_trade()

    # 2. Exports par produit (HS2)
    print("\n[2/6] Exports par chapitre HS2 (WITS)")
    df_exp_prod = collect_by_product("X")

    # 3. Imports par produit (HS2)
    print("\n[3/6] Imports par chapitre HS2 (WITS)")
    df_imp_prod = collect_by_product("M")

    # 4. Exports par partenaire
    print("\n[4/6] Exports par pays partenaire (WITS)")
    df_exp_part = collect_by_partner("X")

    # 5. Imports par partenaire
    print("\n[5/6] Imports par pays partenaire (WITS)")
    df_imp_part = collect_by_partner("M")

    # 6. World Bank
    print("\n[6/6] Données macro World Bank")
    df_wb = collect_wb_supplementary()

    # ── Construire les onglets Excel ──────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Écriture du fichier Excel : {OUTPUT_FILE}")
    print(f"{'='*60}")

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:

        # Onglet 1 : Résumé
        df_summary = make_summary(df_trade, df_wb)
        df_summary.to_excel(writer, sheet_name="Resume", index=False)

        # Onglet 2 : Balance commerciale
        if not df_trade.empty:
            df_trade.to_excel(writer, sheet_name="Balance_commerciale", index=False)
            print(f"  ✓ Balance_commerciale : {len(df_trade)} années")

        # Onglet 3 : World Bank macro
        if not df_wb.empty:
            df_wb.to_excel(writer, sheet_name="Macro_WorldBank", index=False)
            print(f"  ✓ Macro_WorldBank : {len(df_wb)} années")

        # Onglet 4 : Exports par produit (détail)
        if not df_exp_prod.empty:
            df_exp_prod.to_excel(writer, sheet_name="Exports_par_produit", index=False)
            print(f"  ✓ Exports_par_produit : {len(df_exp_prod)} lignes")

        # Onglet 5 : Exports par produit (pivot)
        pivot_exp = make_product_pivot(df_exp_prod)
        if not pivot_exp.empty:
            pivot_exp.to_excel(writer, sheet_name="Exports_pivot_produit", index=False)
            print(f"  ✓ Exports_pivot_produit : {len(pivot_exp)} produits × {len(pivot_exp.columns)-1} années")

        # Onglet 6 : Imports par produit (détail)
        if not df_imp_prod.empty:
            df_imp_prod.to_excel(writer, sheet_name="Imports_par_produit", index=False)
            print(f"  ✓ Imports_par_produit : {len(df_imp_prod)} lignes")

        # Onglet 7 : Imports par produit (pivot)
        pivot_imp = make_product_pivot(df_imp_prod)
        if not pivot_imp.empty:
            pivot_imp.to_excel(writer, sheet_name="Imports_pivot_produit", index=False)
            print(f"  ✓ Imports_pivot_produit : {len(pivot_imp)} produits × {len(pivot_imp.columns)-1} années")

        # Onglet 8 : Top partenaires exports
        pivot_exp_part = make_partner_pivot(df_exp_part)
        if not pivot_exp_part.empty:
            pivot_exp_part.to_excel(writer, sheet_name="Top_dest_exports", index=False)
            print(f"  ✓ Top_dest_exports : {len(pivot_exp_part)} pays")

        # Onglet 9 : Top partenaires imports
        pivot_imp_part = make_partner_pivot(df_imp_part)
        if not pivot_imp_part.empty:
            pivot_imp_part.to_excel(writer, sheet_name="Top_orig_imports", index=False)
            print(f"  ✓ Top_orig_imports : {len(pivot_imp_part)} pays")

        # Onglet 10 : Métadonnées
        meta = pd.DataFrame([
            ("Fichier", str(OUTPUT_FILE.name)),
            ("Date extraction", datetime.now().strftime("%Y-%m-%d %H:%M")),
            ("Source principale", "WITS/Comtrade — https://wits.worldbank.org"),
            ("Source secondaire", "World Bank API — https://data.worldbank.org"),
            ("Pays", "Union des Comores (COM / KM / M49:174)"),
            ("Coverage years (WITS)", "2001–2022 (selon disponibilité Comtrade)"),
            ("Unité WITS", "USD milliers (×1000)"),
            ("Unité World Bank", "USD courants"),
            ("Granularité produit", "Chapitres HS2 (2 chiffres) — 22 sections douanières"),
            ("Dernière vérification", "2026-03-23"),
            ("Licence", "Données publiques — utilisation libre avec attribution"),
        ], columns=["Clé", "Valeur"])
        meta.to_excel(writer, sheet_name="Metadonnees", index=False)

    file_size = OUTPUT_FILE.stat().st_size / 1024
    print(f"\n✅ Fichier généré : {OUTPUT_FILE}")
    print(f"   Taille : {file_size:.0f} KB")
    print(f"   Feuilles : Balance, Macro, Exports/Imports par produit, par partenaire\n")


if __name__ == "__main__":
    main()
