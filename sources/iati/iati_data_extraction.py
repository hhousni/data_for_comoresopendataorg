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
           country.country_code, country.country_percent
    FROM act
    JOIN country ON country.aid = act.aid
    WHERE country.country_code = '{COUNTRY_CODE}'
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
activities["country_percent"] = pd.to_numeric(activities["country_percent"], errors="coerce").fillna(100)

# Exclude activities with 0% allocation — they have no real Comoros share
activities = activities[activities["country_percent"] > 0].copy()
print(f"  -> {len(activities)} activités après exclusion des country_percent = 0")

activities["activite_multinationale"] = activities["country_percent"].apply(
    lambda x: "Non" if x == 100 else "Oui"
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
# Fetch transactions directly by KM aid list — no JOIN to country, so no row multiplication
km_aids = activities["aid"].dropna().unique().tolist()
# Build batched IN clauses to stay within query limits
AID_BATCH = 200
BATCH_SIZE = 50000
trans_batches = []

for i in range(0, len(km_aids), AID_BATCH):
    aids_chunk = km_aids[i:i + AID_BATCH]
    ids_sql = ", ".join(f"'{a}'" for a in aids_chunk)
    offset = 0
    while True:
        batch = query(f"""
            SELECT t.aid, t.trans_day, t.trans_usd, t.trans_eur, t.trans_value, t.trans_currency,
                   t.trans_code, t.trans_flow_code, t.trans_finance_code
            FROM trans AS t
            WHERE t.aid IN ({ids_sql})
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """)
        if not batch:
            break
        trans_batches.append(pd.DataFrame(batch))
        offset += BATCH_SIZE
        if len(batch) < BATCH_SIZE:
            break
    time.sleep(0.1)

total_fetched = sum(len(b) for b in trans_batches)
print(f"  -> {total_fetched} transactions récupérées")

transactions = pd.concat(trans_batches, ignore_index=True) if trans_batches else pd.DataFrame()

if not transactions.empty:
    # Join country_percent from activities (already fetched, no duplication risk)
    pct = activities[["aid", "country_percent"]].drop_duplicates("aid")
    transactions = transactions.merge(pct, on="aid", how="left")

    # Adjust transaction amounts by country percentage for multi-country projects
    transactions['trans_usd'] = pd.to_numeric(transactions['trans_usd'], errors='coerce').fillna(0)
    transactions['trans_eur'] = pd.to_numeric(transactions['trans_eur'], errors='coerce').fillna(0)
    transactions['country_percent'] = pd.to_numeric(transactions['country_percent'], errors='coerce').fillna(100)

    transactions['trans_usd'] = transactions['trans_usd'] * (transactions['country_percent'] / 100)
    transactions['trans_eur'] = transactions['trans_eur'] * (transactions['country_percent'] / 100)
    transactions['trans_kmf'] = transactions['trans_eur'] * KMF_RATE

    print(f"  -> {len(transactions)} transactions au total (montants ajustés pour les projets multi-pays)")
else:
    print("  -> Aucune transaction trouvée.")

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
# D (Disbursement) + E (Expenditure) combined — matches d-portal methodology
disbursements = transactions[transactions["trans_code"].isin(["D", "E"])]
_d = disbursements.groupby("aid").agg(
    total_decaissement_usd=("trans_usd", "sum"),
    total_decaissement_eur=("trans_eur", "sum"),
    total_decaissement_kmf=("trans_kmf", "sum")
).reset_index()

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
trans_agg[["total_decaissement_usd", "total_decaissement_eur", "total_decaissement_kmf", "total_depense_usd", "total_engagement_usd"]] = \
    trans_agg[["total_decaissement_usd", "total_decaissement_eur", "total_decaissement_kmf", "total_depense_usd", "total_engagement_usd"]].fillna(0)

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
    "aid":                    "id_projet",
    "reporting":              "bailleur_de_fonds",
    "reporting_ref":          "reference_rapporteur",
    "funder_ref":             "reference_bailleur",
    "title":                  "titre",
    "description":            "description",
    "slug":                   "slug",
    "status_code":            "code_statut",
    "statut_label":           "statut",
    "day_start":              "jour_debut",
    "day_end":                "jour_fin",
    "day_length":             "duree_jours",
    "date_debut":             "date_debut",
    "date_fin":               "date_fin",
    "country_code":           "code_pays",
    "country_percent":        "pourcentage_pays",
    "type_bailleur":          "type_bailleur",
    "pays_bailleur":          "pays_bailleur",
    "region_bailleur":        "region_bailleur",
    "url_activite":           "lien_source",
    "activite_multinationale": "projet_multi_pays",
    "total_decaissement_usd": "montant_verse_usd",
    "total_decaissement_eur": "montant_verse_eur",
    "total_decaissement_kmf": "montant_verse_kmf",
    "total_engagement_usd":   "engagement_transactions_usd",
    "total_depense_usd":      "depense_transactions_usd",
    "noms_localisation":      "localisation",
    "budget_usd":             "budget_prevu_usd",
    "budget_eur":             "budget_prevu_eur",
    "budget_kmf":             "budget_prevu_kmf",
})

# Clean numeric columns
for col in ["engagement_usd", "depense_usd", "engagement_eur", "depense_eur",
            "engagement_kmf", "depense_kmf", "budget_usd", "budget_eur", "budget_kmf",
            "total_decaissement_usd", "total_depense_usd", "total_engagement_usd"]:
    if col in dataset.columns:
        dataset[col] = pd.to_numeric(dataset[col], errors="coerce")


# ── Remove duplicates ────────────────────────────────────────────────────
dataset = dataset.drop_duplicates(subset="id_projet", keep="first")
print(f"  -> {len(dataset)} activités après dédoublonnage")


# ── Combiner codes et labels en une seule colonne ─────────────────────────

def combine_code_label(codes_str, labels_str):
    """Combine codes and labels into '123_Label' format."""
    if pd.isna(codes_str) or pd.isna(labels_str):
        return None
    codes  = [c.strip() for c in str(codes_str).split(",")]
    labels = [l.strip() for l in str(labels_str).split(",")]
    # Handle cases where there's a mismatch
    if len(codes) != len(labels):
        return labels_str
    combined = [f"{c}_{l}" for c, l in zip(codes, labels)]
    return ", ".join(combined)

# Statut
dataset["statut"] = dataset.apply(
    lambda r: f"{int(r['code_statut'])}_{r['statut']}"
    if pd.notna(r['code_statut']) and pd.notna(r['statut']) else r['statut'],
    axis=1
)

# Secteurs
dataset["secteur_detail"] = dataset["labels_secteur"]

# Groupes secteur
dataset["secteur_principal"] = dataset["labels_groupe_secteur"]

# Flux
dataset["flux"] = dataset["labels_flux"]

# Financement
dataset["financement"] = dataset["labels_financement"]


# Drop redundant columns
columns_to_drop = [
    "code_statut", "codes_secteur", "labels_secteur", "labels_groupe_secteur",
    "codes_flux", "labels_flux", "codes_financement", "labels_financement",
    "slug", "jour_debut", "jour_fin", "reference_bailleur", "code_pays",
    "pourcentage_pays", "flux", "financement"
]
dataset = dataset.drop(columns=[col for col in columns_to_drop if col in dataset.columns])



# ── Reorder columns ───────────────────────────────────────────────────────
final_column_order = [
    # 1. Identification
    "id_projet", "titre", "description", "lien_source",
    # 2. Statut et dates
    "statut", "date_debut", "date_fin",
    # 3. Bailleur
    "bailleur_de_fonds",
    "type_bailleur", "pays_bailleur", "region_bailleur",
    # 4. Financement
    "instrument_financement",
    # 5. Montants versés (argent effectivement décaissé)
    "montant_verse_usd", "montant_verse_eur", "montant_verse_kmf",
    # 6. Budget prévu
    "budget_prevu_usd", "budget_prevu_eur", "budget_prevu_kmf",
    # 7. Secteurs
    "secteur_principal", "secteur_detail",
    # 8. Localisation
    "localisation", "latitudes", "longitudes",
    # 9. Contexte
    "projet_multi_pays",
]
# Ensure all columns in the final list exist in the dataset before selection
final_column_order_existing = [col for col in final_column_order if col in dataset.columns]
final_dataset = dataset[final_column_order_existing]


# ── d-portal Analysis ─────────────────────────────────────────────────────
print("\nAnalyse des décaissements par bailleur (similaire à d-portal)...")

# Convert trans_day to datetime, coercing errors
transactions['trans_day'] = pd.to_datetime(transactions['trans_day'], errors='coerce')

# Filter for disbursements+expenditures (D+E, matching d-portal methodology) and valid dates
disbursements = transactions[
    (transactions['trans_code'].isin(['D', 'E'])) & 
    (transactions['trans_day'].notna())
].copy()

# Merge with activities to get funder information
disbursements_with_funders = disbursements.merge(
    final_dataset[['id_projet', 'pays_bailleur', 'type_bailleur', 'region_bailleur', 'bailleur_de_fonds']],
    left_on='aid',
    right_on='id_projet',
    how='left'
)

# Group by funder and sum disbursements
funder_summary = disbursements_with_funders.groupby(
    ['region_bailleur', 'pays_bailleur', 'type_bailleur']
).agg(
    total_usd=('trans_usd', 'sum'),
    total_eur=('trans_eur', 'sum'),
    total_kmf=('trans_kmf', 'sum')
).reset_index()

# Sort and format for display
funder_summary = funder_summary.sort_values(by='total_usd', ascending=False)
funder_summary['Total Décaissements (USD)'] = funder_summary['total_usd'].map("${:,.0f}".format)
funder_summary['Total Décaissements (EUR)'] = funder_summary['total_eur'].map("€{:,.0f}".format)
funder_summary['Total Décaissements (KMF)'] = funder_summary['total_kmf'].map("{:,.0f} KMF".format)
funder_summary = funder_summary.drop(columns=['total_usd', 'total_eur', 'total_kmf'])


print(funder_summary.to_string(index=False))
grand_total_usd = disbursements_with_funders['trans_usd'].sum()
grand_total_eur = disbursements_with_funders['trans_eur'].sum()
grand_total_kmf = disbursements_with_funders['trans_kmf'].sum()
print(f"\nTotal Général des Décaisseements: ${grand_total_usd:,.0f} USD | €{grand_total_eur:,.0f} EUR | {grand_total_kmf:,.0f} KMF")

metadata = pd.DataFrame([
    {"colonne": "id_projet",          "source": "IATI",    "description": "Identifiant unique de l'activité dans le système IATI"},
    {"colonne": "titre",               "source": "IATI",    "description": "Titre de l'activité (français si disponible, anglais sinon)"},
    {"colonne": "description",         "source": "IATI",    "description": "Description détaillée de l'activité"},
    {"colonne": "lien_source",         "source": "Calculé", "description": "Lien vers la fiche complète de l'activité sur d-portal.org"},
    {"colonne": "statut",              "source": "IATI",    "description": "Statut du projet: 1=Identification, 2=En cours, 3=Finalisation, 4=Clôturé, 5=Annulé, 6=Suspendu"},
    {"colonne": "date_debut",          "source": "IATI",    "description": "Date de début du projet (format YYYY-MM-DD)"},
    {"colonne": "date_fin",            "source": "IATI",    "description": "Date de fin du projet (format YYYY-MM-DD)"},
    {"colonne": "bailleur_de_fonds",   "source": "IATI",    "description": "Nom de l'organisation qui finance et publie les données (ex: Banque Mondiale, USAID, AFD)"},
    {"colonne": "type_bailleur",       "source": "Calculé", "description": "Type de bailleur: Bilatéral (un pays), Multilatéral (organisation internationale), ONG, Autre"},
    {"colonne": "pays_bailleur",       "source": "Calculé", "description": "Pays d'origine du bailleur (ex: France, États-Unis). 'Multilatéral' si organisation internationale"},
    {"colonne": "region_bailleur",     "source": "Calculé", "description": "Région d'origine du bailleur: Europe, Amérique du Nord, Asie Pacifique, Monde Arabe, Multilatéral, ONG, Autre"},
    {"colonne": "instrument_financement", "source": "Calculé", "description": "Type de financement: Don (argent non remboursable), Prêt (argent à rembourser), Mixte, Autre"},
    {"colonne": "montant_verse_usd",   "source": "IATI",    "description": "Argent effectivement versé aux Comores en Dollars américains (USD). Pour les projets multi-pays, seule la part destinée aux Comores est comptabilisée"},
    {"colonne": "montant_verse_eur",   "source": "IATI",    "description": "Argent effectivement versé aux Comores en Euros (EUR)"},
    {"colonne": "montant_verse_kmf",   "source": "Calculé", "description": "Argent effectivement versé aux Comores en Francs comoriens (KMF). Parité fixe: 1 EUR = 491.96775 KMF"},
    {"colonne": "budget_prevu_usd",    "source": "IATI",    "description": "Budget total prévu (pas encore nécessairement versé) en Dollars américains (USD)"},
    {"colonne": "budget_prevu_eur",    "source": "IATI",    "description": "Budget total prévu en Euros (EUR)"},
    {"colonne": "budget_prevu_kmf",    "source": "Calculé", "description": "Budget total prévu en Francs comoriens (KMF). Parité fixe: 1 EUR = 491.96775 KMF"},
    {"colonne": "secteur_principal",   "source": "Calculé", "description": "Grand secteur d'intervention (ex: Santé générale, Éducation de base). Classification OCDE CAD 3 chiffres"},
    {"colonne": "secteur_detail",      "source": "Calculé", "description": "Sous-secteur détaillé (ex: Soins de santé de base, Enseignement primaire). Classification OCDE CAD 5 chiffres"},
    {"colonne": "localisation",        "source": "IATI",    "description": "Zones géographiques d'intervention déclarées par le bailleur (île, ville, région)"},
    {"colonne": "latitudes",           "source": "IATI",    "description": "Coordonnées latitude des lieux d'intervention (utilisées pour la cartographie)"},
    {"colonne": "longitudes",          "source": "IATI",    "description": "Coordonnées longitude des lieux d'intervention (utilisées pour la cartographie)"},
    {"colonne": "projet_multi_pays",   "source": "Calculé", "description": "Oui = les Comores font partie d'un projet régional/mondial (montant ajusté en conséquence). Non = projet dédié uniquement aux Comores"},
])

# ── Write Excel file ──────────────────────────────────────────────────────
import os
os.makedirs("outputs", exist_ok=True)
output_file = "outputs/comores_aide_internationale_v2.xlsx"

with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    final_dataset.to_excel(writer, sheet_name="Données", index=False)
    metadata.to_excel(writer, sheet_name="Métadonnées", index=False)

    workbook  = writer.book
    header_fmt = workbook.add_format({
        "bold": True, "text_wrap": True, "valign": "top",
        "fg_color": "#D7E4BC", "border": 1
    })
    for sheet_name, df in [("Données", final_dataset), ("Métadonnées", metadata)]:
        ws = writer.sheets[sheet_name]
        for col_num, value in enumerate(df.columns):
            ws.write(0, col_num, value, header_fmt)

print(f"\nFichier Excel créé : {output_file}")
print("Colonnes exportées :", final_dataset.columns.tolist())

