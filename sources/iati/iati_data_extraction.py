"""
IATI d-portal - Données d'aide pour les Comores (KM)
=====================================================
Crée un dataset propre avec une ligne par activité.
Pour publication sur comoresopendata.org

Lancer dans la console Positron:
    %run script/d_portal.py

Variable créée:
    dataset
"""

import requests
import pandas as pd
import time

COUNTRY_CODE = "KM"
BASE_URL     = "https://d-portal.org/q.json"
KMF_RATE     = 491.96775  # Parité fixe: 1 EUR = 491.96775 KMF


def query(sql, french=False):
    params = {"sql": sql}
    if french:
        params["tongue"] = "fra"
    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        print(f"  ⚠ Erreur API: {data['error']}")
        return []
    return data.get("rows", [])


print("Récupération des activités ...")
# English version for fallback
activities_en = pd.DataFrame(query(f"""
    SELECT act.aid, act.reporting, act.reporting_ref, act.funder_ref,
           act.title, act.description, act.slug, act.status_code,
           act.day_start, act.day_end, act.day_length,
           act.commitment, act.spend, act.commitment_eur, act.spend_eur,
           country.country_code, country.country_percent
    FROM act
    JOIN country ON country.aid = act.aid
    WHERE country.country_code = '{COUNTRY_CODE}'
    LIMIT 5000
"""))

# French version
activities_fr = pd.DataFrame(query(f"""
    SELECT act.aid, act.title, act.description
    FROM act
    JOIN country ON country.aid = act.aid
    WHERE country.country_code = '{COUNTRY_CODE}'
    LIMIT 5000
""", french=True))

# Merge: use French title/description when available, fallback to English
activities = activities_en.merge(
    activities_fr.rename(columns={"title": "title_fr", "description": "description_fr"}),
    on="aid", how="left"
)
activities["title"]       = activities["title_fr"].fillna(activities["title"])
activities["description"] = activities["description_fr"].fillna(activities["description"])
activities = activities.drop(columns=["title_fr", "description_fr"])
print(f"  -> {len(activities)} activités")
time.sleep(0.5)


# ── Activité multinationale ───────────────────────────────────────────────
activities["activite_multinationale"] = activities["country_percent"].apply(
    lambda x: "Non" if pd.to_numeric(x, errors="coerce") == 100 else "Oui"
)

# ── Status labels (French) ────────────────────────────────────────────────
STATUS_LABELS = {
    1: "Identification",
    2: "En cours",
    3: "Finalisation",
    4: "Clôturé",
    5: "Annulé",
    6: "Suspendu",
}
activities["status_code"]  = pd.to_numeric(activities["status_code"], errors="coerce")
activities["statut_label"] = activities["status_code"].map(STATUS_LABELS)

# ── Funder classification ─────────────────────────────────────────────────
MULTILATERALS = {
    "World Health Organization", "WHO",
    "UNICEF", "United Nations Children's Fund",
    "United Nations Development Programme", "UNDP",
    "World Bank", "International Bank for Reconstruction and Development",
    "International Development Association", "IDA",
    "International Finance Corporation", "IFC",
    "African Development Bank", "AfDB",
    "Asian Development Bank", "ADB",
    "Inter-American Development Bank", "IADB",
    "European Commission", "European Union",
    "International Monetary Fund", "IMF",
    "United Nations", "UN",
    "Food and Agriculture Organization", "FAO",
    "International Fund for Agricultural Development", "IFAD",
    "World Food Programme", "WFP",
    "United Nations Population Fund", "UNFPA",
    "United Nations High Commissioner for Refugees", "UNHCR",
    "United Nations Office for the Coordination of Humanitarian Affairs", "OCHA",
    "United Nations Environment Programme", "UNEP",
    "United Nations Educational, Scientific and Cultural Organization", "UNESCO",
    "International Labour Organization", "ILO",
    "Global Fund", "Gavi", "GAVI Alliance",
    "Green Climate Fund", "GCF",
    "Global Environment Facility", "GEF",
    "Islamic Development Bank", "IsDB",
    "Arab Fund", "AFESD", "OPEC Fund", "OFID",
    "AFD", "Agence Française de Développement", "KFW", "KfW",
}

BILATERAL_PREFIXES = {
    "US-": "États-Unis",       "GB-": "Royaume-Uni",
    "FR-": "France",           "DE-": "Allemagne",
    "JP-": "Japon",            "CA-": "Canada",
    "AU-": "Australie",        "NL-": "Pays-Bas",
    "SE-": "Suède",            "NO-": "Norvège",
    "DK-": "Danemark",         "FI-": "Finlande",
    "BE-": "Belgique",         "CH-": "Suisse",
    "AT-": "Autriche",         "IT-": "Italie",
    "ES-": "Espagne",          "PT-": "Portugal",
    "IE-": "Irlande",          "LU-": "Luxembourg",
    "KR-": "Corée du Sud",     "NZ-": "Nouvelle-Zélande",
    "CN-": "Chine",            "IN-": "Inde",
    "BR-": "Brésil",           "ZA-": "Afrique du Sud",
    "SA-": "Arabie Saoudite",  "AE-": "Émirats Arabes Unis",
    "KW-": "Koweït",           "QA-": "Qatar",
    "TR-": "Turquie",          "RU-": "Russie",
}

NGO_KEYWORDS = [
    "NGO", "Foundation", "Oxfam", "Save the Children", "Care ",
    "Médecins", "Doctors Without Borders", "MSF", "Red Cross",
    "Red Crescent", "Plan International", "ActionAid", "Catholic",
    "Islamic Relief", "World Vision", "Caritas", "Mercy Corps",
    "Lutheran", "Baptist", "Church", "Alliance", "Society",
]

def classify_funder(ref, name):
    ref  = str(ref  or "").strip()
    name = str(name or "").strip()
    for m in MULTILATERALS:
        if m.lower() in name.lower():
            return "Multilatéral", "Multilatéral"
    if ref.startswith("XM-DAC-") or ref.startswith("XM-IATI-"):
        return "Multilatéral", "Multilatéral"
    for prefix, country in BILATERAL_PREFIXES.items():
        if ref.upper().startswith(prefix.upper()):
            return "Bilatéral", country
    for kw in NGO_KEYWORDS:
        if kw.lower() in name.lower():
            return "ONG", "ONG"
    return "Autre", "Autre"

activities[["type_bailleur", "pays_bailleur"]] = activities.apply(
    lambda row: pd.Series(classify_funder(row["reporting_ref"], row["reporting"])),
    axis=1
)
print(f"  Classification bailleurs:")
print(activities["type_bailleur"].value_counts().to_string())

# ── Donor region ──────────────────────────────────────────────────────────
DONOR_REGIONS = {
    "France": "Europe", "Royaume-Uni": "Europe", "Allemagne": "Europe",
    "Pays-Bas": "Europe", "Suède": "Europe", "Norvège": "Europe",
    "Danemark": "Europe", "Finlande": "Europe", "Belgique": "Europe",
    "Suisse": "Europe", "Autriche": "Europe", "Italie": "Europe",
    "Espagne": "Europe", "Portugal": "Europe", "Irlande": "Europe",
    "Luxembourg": "Europe", "Pologne": "Europe",
    "États-Unis": "Amérique du Nord", "Canada": "Amérique du Nord",
    "Japon": "Asie Pacifique", "Corée du Sud": "Asie Pacifique",
    "Australie": "Asie Pacifique", "Nouvelle-Zélande": "Asie Pacifique",
    "Chine": "Asie Pacifique", "Inde": "Asie Pacifique",
    "Arabie Saoudite": "Monde Arabe", "Émirats Arabes Unis": "Monde Arabe",
    "Koweït": "Monde Arabe", "Qatar": "Monde Arabe",
    "Brésil": "Amérique Latine", "Afrique du Sud": "Afrique",
    "Turquie": "Moyen-Orient", "Russie": "Europe de l'Est",
    "Multilatéral": "Multilatéral", "ONG": "ONG", "Autre": "Autre",
}
activities["region_bailleur"] = activities["pays_bailleur"].map(DONOR_REGIONS).fillna("Autre")

# ── Date conversion ───────────────────────────────────────────────────────
for col, new_col in [("day_start", "date_debut"), ("day_end", "date_fin")]:
    dt = pd.to_datetime(pd.to_numeric(activities[col], errors="coerce"),
                        unit="D", origin="1970-01-01")
    activities[new_col] = dt.dt.strftime("%Y-%m-%d")

# ── KMF conversion ────────────────────────────────────────────────────────
activities["commitment_eur"] = pd.to_numeric(activities["commitment_eur"], errors="coerce")
activities["spend_eur"]      = pd.to_numeric(activities["spend_eur"],      errors="coerce")
activities["engagement_kmf"] = activities["commitment_eur"] * KMF_RATE
activities["depense_kmf"]    = activities["spend_eur"]      * KMF_RATE

# ── Activity URL ──────────────────────────────────────────────────────────
activities["url_activite"] = "https://d-portal.org/ctrack.html?slug=" + activities["slug"].astype(str) + "#view=act"

print("Récupération des secteurs ...")
sectors = pd.DataFrame(query(f"""
    SELECT act.aid, s.sector_group, s.sector_code, s.sector_percent
    FROM act
    JOIN country ON country.aid = act.aid
    JOIN sector s ON s.aid = act.aid
    WHERE country.country_code = '{COUNTRY_CODE}'
    LIMIT 20000
"""))
print(f"  -> {len(sectors)} secteurs")
time.sleep(0.5)

# ── Sector labels (French) ────────────────────────────────────────────────
SECTOR_CODE_LABELS = {
    "11110": "Politique éducative et gestion administrative",
    "11120": "Installations éducatives et formation",
    "11130": "Formation des enseignants",
    "11182": "Recherche en éducation",
    "11220": "Enseignement primaire",
    "11230": "Alphabétisation de base",
    "11240": "Éducation de la petite enfance",
    "11320": "Enseignement secondaire",
    "11330": "Formation professionnelle",
    "11420": "Enseignement supérieur",
    "11430": "Formation technique et managériale avancée",
    "12110": "Politique sanitaire et gestion administrative",
    "12181": "Formation médicale",
    "12182": "Recherche médicale",
    "12191": "Services médicaux",
    "12220": "Soins de santé de base",
    "12230": "Infrastructure de santé de base",
    "12240": "Nutrition de base",
    "12250": "Lutte contre les maladies infectieuses",
    "12261": "Éducation sanitaire",
    "12262": "Lutte contre le paludisme",
    "12263": "Lutte contre la tuberculose",
    "12310": "Soins de santé reproductive",
    "12320": "Planification familiale",
    "12330": "Lutte contre les IST/VIH/SIDA",
    "12341": "VIH/SIDA",
    "14010": "Politique du secteur eau et gestion administrative",
    "14020": "Approvisionnement en eau et assainissement",
    "14030": "Eau potable et assainissement de base",
    "14031": "Eau potable de base",
    "14032": "Assainissement de base",
    "14040": "Développement fluvial",
    "14050": "Gestion des déchets",
    "15110": "Politique du secteur public et gestion administrative",
    "15111": "Gestion des finances publiques",
    "15112": "Décentralisation",
    "15113": "Lutte contre la corruption",
    "15114": "Mobilisation des recettes intérieures",
    "15130": "Développement juridique et judiciaire",
    "15150": "Élections",
    "15160": "Droits de l'homme",
    "15170": "Organisations pour l'égalité des femmes",
    "15180": "Lutte contre les violences faites aux femmes",
    "15210": "Réforme du système de sécurité",
    "15220": "Consolidation de la paix",
    "15250": "Déminage",
    "21010": "Politique des transports et gestion administrative",
    "21020": "Transport routier",
    "21030": "Transport ferroviaire",
    "21040": "Transport maritime",
    "21050": "Transport aérien",
    "23010": "Politique énergétique et gestion administrative",
    "23020": "Production d'énergie non renouvelable",
    "23030": "Production d'énergie renouvelable",
    "23040": "Transport et distribution d'électricité",
    "23068": "Énergie solaire",
    "23069": "Énergie éolienne",
    "31110": "Politique agricole et gestion administrative",
    "31120": "Développement agricole",
    "31130": "Ressources foncières agricoles",
    "31140": "Ressources en eau agricoles",
    "31150": "Intrants agricoles",
    "31161": "Production de cultures vivrières",
    "31163": "Élevage",
    "31166": "Vulgarisation agricole",
    "31182": "Recherche agricole",
    "31310": "Politique de la pêche et gestion administrative",
    "31320": "Développement de la pêche",
    "32110": "Politique industrielle et gestion administrative",
    "32120": "Développement industriel",
    "32130": "Développement des PME",
    "33110": "Politique commerciale et gestion administrative",
    "33120": "Facilitation du commerce",
    "41010": "Politique environnementale et gestion administrative",
    "41020": "Protection de la biosphère",
    "41030": "Biodiversité",
    "41050": "Prévention des inondations",
    "72010": "Aide matérielle d'urgence",
    "72040": "Aide alimentaire d'urgence",
    "73010": "Reconstruction et réhabilitation",
    "74010": "Prévention des catastrophes",
    "91010": "Coûts administratifs des donateurs",
    "92010": "Soutien aux ONG nationales",
    "92020": "Soutien aux ONG internationales",
    "99810": "Secteurs non spécifiés",
    "99820": "Transactions non ventilables par secteur",
}

SECTOR_GROUP_LABELS = {
    "111": "Éducation (non spécifiée)",
    "112": "Éducation de base",
    "113": "Enseignement secondaire",
    "114": "Enseignement post-secondaire",
    "121": "Santé générale",
    "122": "Santé de base",
    "123": "Maladies non transmissibles",
    "130": "Santé reproductive",
    "140": "Eau et assainissement",
    "151": "Gouvernance et société civile",
    "152": "Paix et sécurité",
    "160": "Autres infrastructures sociales",
    "210": "Transport et stockage",
    "220": "Communications",
    "230": "Énergie",
    "231": "Énergie renouvelable",
    "232": "Énergie non renouvelable",
    "240": "Services bancaires et financiers",
    "250": "Services aux entreprises",
    "311": "Agriculture",
    "312": "Sylviculture",
    "313": "Pêche",
    "321": "Industrie",
    "331": "Commerce",
    "410": "Protection de l'environnement",
    "430": "Multisectoriel",
    "510": "Appui budgétaire général",
    "520": "Aide alimentaire au développement",
    "600": "Actions relatives à la dette",
    "720": "Aide d'urgence",
    "730": "Reconstruction et réhabilitation",
    "740": "Prévention des catastrophes",
    "910": "Coûts administratifs",
    "920": "Soutien aux ONG",
    "998": "Non alloué",
}

sec_agg = sectors.groupby("aid").agg(
    codes_secteur        = ("sector_code",  lambda x: ", ".join(x.dropna().astype(str).unique())),
    labels_secteur       = ("sector_code",  lambda x: ", ".join(x.dropna().astype(str).map(lambda c: SECTOR_CODE_LABELS.get(c, c)).unique())),
    groupes_secteur      = ("sector_group", lambda x: ", ".join(x.dropna().astype(str).unique())),
    labels_groupe_secteur= ("sector_group", lambda x: ", ".join(x.dropna().astype(str).map(lambda g: SECTOR_GROUP_LABELS.get(g, g)).unique())),
).reset_index()

print("Récupération des budgets ...")
budgets = pd.DataFrame(query(f"""
    SELECT act.aid, b.budget_value, b.budget_currency, b.budget_usd, b.budget_eur
    FROM act
    JOIN country ON country.aid = act.aid
    JOIN budget b ON b.aid = act.aid
    WHERE country.country_code = '{COUNTRY_CODE}'
    LIMIT 20000
"""))
budgets["budget_eur"] = pd.to_numeric(budgets["budget_eur"], errors="coerce")
budgets["budget_usd"] = pd.to_numeric(budgets["budget_usd"], errors="coerce")
budgets["budget_kmf"] = budgets["budget_eur"] * KMF_RATE
print(f"  -> {len(budgets)} budgets")
time.sleep(0.5)

bud_agg = budgets.groupby("aid").agg(
    budget_valeur_originale = ("budget_value",    "sum"),
    budget_devise_originale = ("budget_currency", lambda x: ", ".join(x.dropna().astype(str).unique())),
    budget_usd              = ("budget_usd",      "sum"),
    budget_eur              = ("budget_eur",      "sum"),
    budget_kmf              = ("budget_kmf",      "sum"),
).reset_index()

print("Récupération des transactions (par lots) ...")
BATCH_SIZE = 50000
offset = 0
trans_batches = []

while True:
    batch = query(f"""
        SELECT act.aid, t.trans_usd, t.trans_value, t.trans_currency,
               t.trans_code, t.trans_flow_code, t.trans_finance_code
        FROM act
        JOIN country ON country.aid = act.aid
        JOIN trans t ON t.aid = act.aid
        WHERE country.country_code = '{COUNTRY_CODE}'
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """)
    if not batch:
        break
    trans_batches.append(pd.DataFrame(batch))
    print(f"  -> Lot {offset // BATCH_SIZE + 1}: {len(batch)} transactions récupérées")
    offset += BATCH_SIZE
    if len(batch) < BATCH_SIZE:
        break
    time.sleep(0.5)

transactions = pd.concat(trans_batches, ignore_index=True) if trans_batches else pd.DataFrame()
print(f"  -> {len(transactions)} transactions au total")
time.sleep(0.5)

# Flow type labels (French)
FLOW_TYPE_LABELS = {
    "10": "APD (Aide Publique au Développement)",
    "20": "Autres flux officiels",
    "30": "Dons privés",
    "35": "Marché privé",
    "40": "Non flux",
    "50": "Autres flux",
}

# Finance type labels (French)
FINANCE_TYPE_LABELS = {
    "110": "Don standard",
    "111": "Subventions aux investisseurs privés",
    "210": "Bonification d'intérêts",
    "310": "Souscription au capital",
    "410": "Prêt (hors réorganisation de la dette)",
    "421": "Prêt standard",
    "422": "Don remboursable",
    "431": "Prêt subordonné",
    "451": "Crédits à l'exportation garantis",
    "510": "Participation au capital",
    "610": "Annulation de dette: créances APD (P)",
    "620": "Rééchelonnement: créances APD (P)",
    "710": "Investissement direct étranger",
}

# Simplified financing instrument (French)
GRANT_CODES = {"110", "111", "210", "211", "310", "311", "422"}
LOAN_CODES  = {"410", "411", "412", "413", "414", "421", "431", "432", "433"}

def classify_instrument(codes_str):
    if not codes_str or str(codes_str) == "nan":
        return "Inconnu"
    codes   = {c.strip() for c in str(codes_str).split(",")}
    is_grant = bool(codes & GRANT_CODES)
    is_loan  = bool(codes & LOAN_CODES)
    if is_grant and is_loan: return "Mixte"
    elif is_grant:           return "Don"
    elif is_loan:            return "Prêt"
    else:                    return "Autre"

# Calcul sûr des sommes D/E/C (évite désalignement d'index dans groupby lambda)
_d = transactions[transactions["trans_code"] == "D"].groupby("aid")["trans_usd"].sum().reset_index(name="total_decaissement_usd")
_e = transactions[transactions["trans_code"] == "E"].groupby("aid")["trans_usd"].sum().reset_index(name="total_depense_usd")
_c = transactions[transactions["trans_code"] == "C"].groupby("aid")["trans_usd"].sum().reset_index(name="total_engagement_usd")

trans_agg = transactions.groupby("aid").agg(
    nombre_transactions     = ("trans_usd", "count"),
    codes_flux              = ("trans_flow_code",    lambda x: ", ".join(x.dropna().astype(str).unique())),
    labels_flux             = ("trans_flow_code",    lambda x: ", ".join(x.dropna().astype(str).map(lambda c: FLOW_TYPE_LABELS.get(c, c)).unique())),
    codes_financement       = ("trans_finance_code", lambda x: ", ".join(x.dropna().astype(str).unique())),
    labels_financement      = ("trans_finance_code", lambda x: ", ".join(x.dropna().astype(str).map(lambda c: FINANCE_TYPE_LABELS.get(c, c)).unique())),
).reset_index()

trans_agg = (trans_agg
    .merge(_d, on="aid", how="left")
    .merge(_e, on="aid", how="left")
    .merge(_c, on="aid", how="left")
)
trans_agg[["total_decaissement_usd", "total_depense_usd", "total_engagement_usd"]] = \
    trans_agg[["total_decaissement_usd", "total_depense_usd", "total_engagement_usd"]].fillna(0)

trans_agg["instrument_financement"] = trans_agg["codes_financement"].apply(classify_instrument)

print("Récupération des localisations ...")
locations = pd.DataFrame(query(f"""
    SELECT act.aid, l.location_name, l.location_latitude, l.location_longitude
    FROM act
    JOIN country ON country.aid = act.aid
    JOIN location l ON l.aid = act.aid
    WHERE country.country_code = '{COUNTRY_CODE}'
    LIMIT 10000
"""))
print(f"  -> {len(locations)} localisations")

loc_agg = locations.groupby("aid").agg(
    noms_localisation      = ("location_name",      lambda x: ", ".join(x.dropna().astype(str).unique())),
    latitudes              = ("location_latitude",  lambda x: ", ".join(x.dropna().astype(str).unique())),
    longitudes             = ("location_longitude", lambda x: ", ".join(x.dropna().astype(str).unique())),
).reset_index()

# ── Build final dataset ───────────────────────────────────────────────────
print("\nConstruction du dataset ...")

dataset = (
    activities
    .merge(sec_agg,   on="aid", how="left")
    .merge(bud_agg,   on="aid", how="left")
    .merge(trans_agg, on="aid", how="left")
    .merge(loc_agg,   on="aid", how="left")
)

# ── Rename all columns to French ──────────────────────────────────────────
dataset = dataset.rename(columns={
    "aid":             "identifiant",
    "reporting":       "organisation_rapporteur",
    "reporting_ref":   "reference_rapporteur",
    "funder_ref":      "reference_bailleur",
    "title":           "titre",
    "description":     "description",
    "slug":            "slug",
    "status_code":     "code_statut",
    "statut_label":    "statut",
    "day_start":       "jour_debut",
    "day_end":         "jour_fin",
    "day_length":      "duree_jours",
    "date_debut":      "date_debut",
    "date_fin":        "date_fin",
    "commitment":      "engagement_usd",
    "spend":           "depense_usd",
    "commitment_eur":  "engagement_eur",
    "spend_eur":       "depense_eur",
    "engagement_kmf":  "engagement_kmf",
    "depense_kmf":     "depense_kmf",
    "country_code":    "code_pays",
    "country_percent": "pourcentage_pays",
    "type_bailleur":   "type_bailleur",
    "pays_bailleur":   "pays_bailleur",
    "region_bailleur": "region_bailleur",
    "url_activite":    "url_activite",
})

# Clean numeric columns
for col in ["engagement_usd", "depense_usd", "engagement_eur", "depense_eur",
            "engagement_kmf", "depense_kmf", "budget_usd", "budget_eur", "budget_kmf",
            "total_decaissement_usd", "total_depense_usd", "total_engagement_usd"]:
    if col in dataset.columns:
        dataset[col] = pd.to_numeric(dataset[col], errors="coerce")


# ── Remove duplicates ────────────────────────────────────────────────────
dataset = dataset.drop_duplicates(subset="identifiant", keep="first")
print(f"  -> {len(dataset)} activités après dédoublonnage")

# ── Reorder columns ───────────────────────────────────────────────────────
column_order = [
    # 1. Identification
    "identifiant", "titre", "description", "url_activite",
    # 2. Statut et dates
    "code_statut", "statut", "date_debut", "date_fin", "duree_jours",
    # 3. Bailleur
    "organisation_rapporteur", "reference_rapporteur", "reference_bailleur",
    "type_bailleur", "pays_bailleur", "region_bailleur",
    # 4. Engagements et dépenses
    "engagement_usd", "engagement_eur", "engagement_kmf",
    "depense_usd", "depense_eur", "depense_kmf",
    # 5. Budget
    "budget_valeur_originale", "budget_devise_originale",
    "budget_usd", "budget_eur", "budget_kmf",
    # 6. Transactions
    "instrument_financement", "labels_flux", "labels_financement",
    "total_engagement_usd", "total_decaissement_usd", "total_depense_usd",
    "nombre_transactions",
    # 7. Secteurs
    "labels_groupe_secteur", "labels_secteur", "groupes_secteur", "codes_secteur",
    # 8. Localisation
    "noms_localisation", "latitudes", "longitudes",
    # 9. Technique
    "code_pays", "pourcentage_pays", "activite_multinationale", "slug", "jour_debut", "jour_fin",
    "indicateurs", "codes_flux", "codes_financement",
]

# Only keep columns that exist in the dataset
column_order = [c for c in column_order if c in dataset.columns]
dataset = dataset[column_order]


# ── Combiner codes et labels en une seule colonne ─────────────────────────

def combine_code_label(codes_str, labels_str):
    """Combine codes and labels into '123_Label' format."""
    if pd.isna(codes_str) or pd.isna(labels_str):
        return None
    codes  = [c.strip() for c in str(codes_str).split(",")]
    labels = [l.strip() for l in str(labels_str).split(",")]
    combined = [f"{c}_{l}" for c, l in zip(codes, labels)]
    return ", ".join(combined)

# Statut
dataset["statut"] = dataset.apply(
    lambda r: f"{int(r['code_statut'])}_{r['statut']}"
    if pd.notna(r['code_statut']) and pd.notna(r['statut']) else r['statut'],
    axis=1
)

# Secteurs
dataset["secteurs"] = dataset.apply(
    lambda r: combine_code_label(r["codes_secteur"], r["labels_secteur"]), axis=1
)

# Groupes secteur
dataset["groupes_secteur"] = dataset.apply(
    lambda r: combine_code_label(r["groupes_secteur"], r["labels_groupe_secteur"]), axis=1
)

# Flux
dataset["flux"] = dataset.apply(
    lambda r: combine_code_label(r["codes_flux"], r["labels_flux"]), axis=1
)

# Financement
dataset["financement"] = dataset.apply(
    lambda r: combine_code_label(r["codes_financement"], r["labels_financement"]), axis=1
)

# Drop redundant columns
dataset = dataset.drop(columns=[
    "code_statut",
    "codes_secteur", "labels_secteur",
    "labels_groupe_secteur",
    "codes_flux", "labels_flux",
    "codes_financement", "labels_financement",
    "slug", "jour_debut", "jour_fin",
    "reference_bailleur", "code_pays",
    "pourcentage_pays",
])

# Rename columns for clarity
dataset = dataset.rename(columns={
    "total_engagement_usd"   : "engagement_transactions_usd",
    "total_decaissement_usd" : "decaissement_transactions_usd",
    "total_depense_usd"      : "depense_transactions_usd",
    "reference_rapporteur"   : "id_bailleur",
    "duree_jours"            : "duree_projet_jours",
    "noms_localisation"      : "localisation",
})


dataset = dataset[[
    # 1. Identification
    "identifiant", "titre", "description", "url_activite",
    # 2. Statut et dates
    "statut", "date_debut", "date_fin", "duree_projet_jours",
    # 3. Bailleur
    "organisation_rapporteur", "id_bailleur",
    "type_bailleur", "pays_bailleur", "region_bailleur",
    # 4. Engagements et dépenses
    "engagement_usd", "engagement_eur", "engagement_kmf",
    "depense_usd", "depense_eur", "depense_kmf",
    # 5. Budget
    "budget_valeur_originale", "budget_devise_originale",
    "budget_usd", "budget_eur", "budget_kmf",
    # 6. Transactions
    "instrument_financement", "flux", "financement",
    "engagement_transactions_usd", "decaissement_transactions_usd",
    "depense_transactions_usd", "nombre_transactions",
    # 7. Secteurs
    "groupes_secteur", "secteurs",
    # 8. Localisation
    "localisation", "latitudes", "longitudes",
    # 9. Technique
    "activite_multinationale",
]]


metadata = pd.DataFrame([
    # 1. Identification
    {"colonne": "identifiant",                  "source": "IATI",    "description": "Identifiant unique de l'activité dans le système IATI"},
    {"colonne": "titre",                        "source": "IATI",    "description": "Titre de l'activité (français si disponible, anglais sinon)"},
    {"colonne": "description",                  "source": "IATI",    "description": "Description détaillée de l'activité (français si disponible, anglais sinon)"},
    {"colonne": "url_activite",                 "source": "Calculé", "description": "Lien vers la fiche complète de l'activité sur d-portal.org"},
    # 2. Statut et dates
    {"colonne": "statut",                       "source": "IATI",    "description": "Statut de l'activité au format code_label (ex: 2_En cours). Codes: 1_Identification, 2_En cours, 3_Finalisation, 4_Clôturé, 5_Annulé, 6_Suspendu"},
    {"colonne": "date_debut",                   "source": "Calculé", "description": "Date de début de l'activité au format YYYY-MM-DD (convertie depuis le format numérique IATI)"},
    {"colonne": "date_fin",                     "source": "Calculé", "description": "Date de fin de l'activité au format YYYY-MM-DD (convertie depuis le format numérique IATI)"},
    {"colonne": "duree_projet_jours",           "source": "IATI",    "description": "Durée totale du projet en jours"},
    # 3. Bailleur
    {"colonne": "organisation_rapporteur",      "source": "IATI",    "description": "Nom de l'organisation qui publie les données IATI pour cette activité"},
    {"colonne": "id_bailleur",                  "source": "IATI",    "description": "Identifiant de référence de l'organisation rapporteur dans le registre IATI"},
    {"colonne": "type_bailleur",                "source": "Calculé", "description": "Type de bailleur calculé à partir de l'identifiant et du nom: Bilatéral, Multilatéral, ONG, Autre"},
    {"colonne": "pays_bailleur",                "source": "Calculé", "description": "Pays d'origine du bailleur (ex: France, États-Unis). Multilatéral si organisation internationale"},
    {"colonne": "region_bailleur",              "source": "Calculé", "description": "Région d'origine du bailleur: Europe, Amérique du Nord, Asie Pacifique, Monde Arabe, Afrique, Multilatéral, ONG, Autre"},
    # 4. Engagements et dépenses
    {"colonne": "engagement_usd",               "source": "IATI",    "description": "Montant total engagé en Dollars américains (USD)"},
    {"colonne": "engagement_eur",               "source": "IATI",    "description": "Montant total engagé en Euros (EUR)"},
    {"colonne": "engagement_kmf",               "source": "Calculé", "description": "Montant total engagé en Francs comoriens (KMF), calculé via la parité fixe: 1 EUR = 491.96775 KMF"},
    {"colonne": "depense_usd",                  "source": "IATI",    "description": "Montant total dépensé en Dollars américains (USD)"},
    {"colonne": "depense_eur",                  "source": "IATI",    "description": "Montant total dépensé en Euros (EUR)"},
    {"colonne": "depense_kmf",                  "source": "Calculé", "description": "Montant total dépensé en Francs comoriens (KMF), calculé via la parité fixe: 1 EUR = 491.96775 KMF"},
    # 5. Budget
    {"colonne": "budget_valeur_originale",      "source": "IATI",    "description": "Montant total du budget dans la devise originale rapportée par le bailleur"},
    {"colonne": "budget_devise_originale",      "source": "IATI",    "description": "Devise originale du budget (ex: USD, EUR, GBP)"},
    {"colonne": "budget_usd",                   "source": "IATI",    "description": "Montant total du budget converti en Dollars américains (USD)"},
    {"colonne": "budget_eur",                   "source": "IATI",    "description": "Montant total du budget converti en Euros (EUR)"},
    {"colonne": "budget_kmf",                   "source": "Calculé", "description": "Montant total du budget en Francs comoriens (KMF), calculé via la parité fixe: 1 EUR = 491.96775 KMF"},
    # 6. Transactions
    {"colonne": "instrument_financement",       "source": "Calculé", "description": "Type simplifié de financement calculé à partir des codes de finance IATI: Don, Prêt, Mixte, Autre, Inconnu"},
    {"colonne": "flux",                         "source": "Calculé", "description": "Type de flux financier au format code_label (ex: 10_APD). Basé sur le code IATI trans_flow_code"},
    {"colonne": "financement",                  "source": "Calculé", "description": "Type de financement au format code_label (ex: 110_Don standard). Basé sur le code IATI trans_finance_code"},
    {"colonne": "engagement_transactions_usd",  "source": "IATI",    "description": "Montant total des transactions de type Engagement (C) en USD, calculé depuis la table des transactions"},
    {"colonne": "decaissement_transactions_usd","source": "IATI",    "description": "Montant total des transactions de type Décaissement (D) en USD — argent versé au destinataire"},
    {"colonne": "depense_transactions_usd",     "source": "IATI",    "description": "Montant total des transactions de type Dépense (E) en USD — argent effectivement dépensé sur le terrain"},
    {"colonne": "nombre_transactions",          "source": "IATI",    "description": "Nombre total de transactions financières enregistrées pour cette activité"},
    # 7. Secteurs
    {"colonne": "groupes_secteur",              "source": "Calculé", "description": "Groupes sectoriels au format code_label (ex: 121_Santé générale). Basé sur la classification OCDE CAD 3 chiffres"},
    {"colonne": "secteurs",                     "source": "Calculé", "description": "Secteurs au format code_label (ex: 12110_Politique sanitaire). Basé sur la classification OCDE CAD 5 chiffres"},
    # 8. Localisation
    {"colonne": "localisation",                 "source": "IATI",    "description": "Noms des lieux d'intervention de l'activité (peut contenir plusieurs valeurs séparées par des virgules)"},
    {"colonne": "latitudes",                    "source": "IATI",    "description": "Coordonnées géographiques latitude des lieux d'intervention (peut contenir plusieurs valeurs)"},
    {"colonne": "longitudes",                   "source": "IATI",    "description": "Coordonnées géographiques longitude des lieux d'intervention (peut contenir plusieurs valeurs)"},
    # 9. Technique
    {"colonne": "activite_multinationale",      "source": "Calculé", "description": "Indique si l'activité couvre plusieurs pays: Oui (Comores = partie du projet) ou Non (Comores = seul pays bénéficiaire)"},
])


with pd.ExcelWriter("backend/app/data_extraction/data/iati/comores_aide_internationale.xlsx", engine="openpyxl") as writer:
    dataset.to_excel(writer, sheet_name="Données", index=False)
    metadata.to_excel(writer, sheet_name="Métadonnées", index=False)
