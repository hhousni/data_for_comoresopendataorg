#!/usr/bin/env python3
"""
Track activities present in d-portal but missing from IATI Tables for KM.

Outputs: outputs/missing_from_iati_tables.xlsx
Run periodically to see when IATI Tables closes the gap.
"""
import requests
import pandas as pd
from datetime import date

DPORTAL_BASE = "https://d-portal.org/q.json"
IATI_BASE    = "https://datasette-tables.iatistandard.org/iati.json"
IATI_HDR     = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
BATCH_IT     = 500   # IATI Tables — can handle larger IN clauses
BATCH_DP     = 50    # d-portal — smaller to avoid 431 header-too-large errors
OUT_FILE     = "outputs/missing_from_iati_tables.xlsx"


def dp(sql):
    r = requests.get(DPORTAL_BASE, params={"sql": sql}, timeout=120)
    r.raise_for_status()
    d = r.json()
    if "error" in d:
        raise RuntimeError(f"d-portal: {d['error']}")
    rows = d.get("rows", [])
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def it(sql, paginate=False):
    all_rows, cols, offset = [], None, 0
    while True:
        r = requests.get(IATI_BASE,
                         params={"sql": f"{sql} LIMIT 2000 OFFSET {offset}"},
                         timeout=120, headers=IATI_HDR)
        if not r.text.strip():
            raise RuntimeError("IATI Tables: empty response (timeout?)")
        d = r.json()
        if not d.get("ok"):
            raise RuntimeError(f"IATI Tables: {d.get('error')}")
        rows = d.get("rows", [])
        cols = d.get("columns", [])
        all_rows.extend(rows)
        if not paginate or len(rows) < 2000:
            break
        offset += 2000
    return pd.DataFrame(all_rows, columns=cols) if all_rows else pd.DataFrame(columns=cols or [])


# ── Step 1: IATI Tables identifier set ───────────────────────────────────────
print("Step 1 — IATI Tables: non-multinational KM identifiers …")
sole_df  = it("""
    SELECT _link_activity
    FROM   recipientcountry
    WHERE  code = 'KM'
    GROUP  BY _link_activity
    HAVING COUNT(*) = 1 AND MAX(coalesce(percentage, 100)) >= 99
""", paginate=True)
link_ids = sole_df["_link_activity"].tolist()

act_chunks = []
for i in range(0, len(link_ids), BATCH_IT):
    chunk   = link_ids[i:i + BATCH_IT]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    act_chunks.append(it(f"""
        SELECT iatiidentifier
        FROM   activity
        WHERE  _link IN ({in_list})
    """))
it_ids = set(pd.concat(act_chunks, ignore_index=True)["iatiidentifier"].dropna().str.strip())
print(f"  → {len(it_ids)} identifiers in IATI Tables\n")


# ── Step 2: d-portal full activity list with metadata ────────────────────────
print("Step 2 — d-portal: all non-multinational KM activities with metadata …")
dp_acts = dp(f"""
    SELECT act.aid, act.reporting, act.reporting_ref,
           act.title, act.status_code,
           act.day_start, act.day_end
    FROM   act
    JOIN   country ON country.aid = act.aid
    WHERE  country.country_code = 'KM'
      AND  (country.country_percent = 100 OR country.country_percent IS NULL)
""")
print(f"  → {len(dp_acts)} activities")

# Get D+E totals for all of them
print("  Fetching D+E totals …")
dp_ids_list = dp_acts["aid"].dropna().str.strip().unique().tolist()
trans_chunks = []
for i in range(0, len(dp_ids_list), BATCH_DP):
    chunk   = dp_ids_list[i:i + BATCH_DP]
    in_list = ", ".join(f"'{x.replace(chr(39), chr(39)*2)}'" for x in chunk)
    trans_chunks.append(dp(f"""
        SELECT t.aid,
               SUM(CASE WHEN t.trans_code = 'D' THEN t.trans_usd ELSE 0 END) AS disbursements_usd,
               SUM(CASE WHEN t.trans_code = 'E' THEN t.trans_usd ELSE 0 END) AS expenditures_usd,
               SUM(CASE WHEN t.trans_code IN ('D','E') THEN t.trans_usd ELSE 0 END) AS total_de_usd
        FROM   trans AS t
        WHERE  t.aid IN ({in_list})
        GROUP  BY t.aid
    """))
dp_trans = pd.concat(trans_chunks, ignore_index=True) if trans_chunks else pd.DataFrame()
for col in ["disbursements_usd", "expenditures_usd", "total_de_usd"]:
    dp_trans[col] = pd.to_numeric(dp_trans[col], errors="coerce").fillna(0)

dp_full = dp_acts.merge(dp_trans, on="aid", how="left").fillna(
    {"disbursements_usd": 0, "expenditures_usd": 0, "total_de_usd": 0})
print(f"  → {len(dp_full)} activities with transaction totals\n")


# ── Step 3: Flag what's missing from IATI Tables ─────────────────────────────
dp_full["in_iati_tables"] = dp_full["aid"].str.strip().isin(it_ids)
missing = dp_full[~dp_full["in_iati_tables"]].copy()
present = dp_full[ dp_full["in_iati_tables"]].copy()

missing_total = missing["total_de_usd"].sum()
present_total = present["total_de_usd"].sum()
overall_total = dp_full["total_de_usd"].sum()

print("═"*64)
print(f"  Total activities in d-portal (non-multinational KM) : {len(dp_full):>5,}")
print(f"  Present in IATI Tables                              : {len(present):>5,}  (${present_total:>14,.0f})")
print(f"  MISSING from IATI Tables                            : {len(missing):>5,}  (${missing_total:>14,.0f})")
print(f"  Coverage gap as % of d-portal total                 : {missing_total/overall_total*100:>5.1f}%")
print("═"*64)


# ── Step 4: Organise missing by publisher ────────────────────────────────────
by_pub = (missing.groupby("reporting")
                 .agg(n_activities=("aid","count"),
                      total_de_usd=("total_de_usd","sum"))
                 .reset_index()
                 .sort_values("total_de_usd", ascending=False))

print(f"\n  Top publishers missing from IATI Tables:")
print(f"  {'Publisher':<50} {'#':>4} {'Total D+E USD':>16}")
print(f"  {'-'*50} {'-'*4} {'-'*16}")
for _, row in by_pub.head(15).iterrows():
    print(f"  {str(row['reporting'])[:49]:<50} {int(row['n_activities']):>4}  ${row['total_de_usd']:>14,.0f}")


# ── Step 5: Export to Excel ───────────────────────────────────────────────────
print(f"\nExporting to {OUT_FILE} …")

missing_export = missing[[
    "aid", "reporting", "reporting_ref", "title",
    "status_code", "day_start", "day_end",
    "disbursements_usd", "expenditures_usd", "total_de_usd"
]].rename(columns={
    "aid":               "iati_identifier",
    "reporting":         "reporting_org",
    "reporting_ref":     "reporting_org_ref",
    "title":             "activity_title",
    "status_code":       "status_code",
    "day_start":         "start_date",
    "day_end":           "end_date",
    "disbursements_usd": "disbursements_usd",
    "expenditures_usd":  "expenditures_usd",
    "total_de_usd":      "total_de_usd",
}).sort_values("total_de_usd", ascending=False)

with pd.ExcelWriter(OUT_FILE, engine="xlsxwriter") as writer:
    # Sheet 1: missing activities detail
    missing_export.to_excel(writer, sheet_name="Missing activities", index=False)
    ws = writer.sheets["Missing activities"]
    ws.set_column("A:A", 45)
    ws.set_column("B:B", 40)
    ws.set_column("C:C", 20)
    ws.set_column("D:D", 60)
    ws.set_column("E:I", 18)

    # Sheet 2: by publisher
    by_pub.rename(columns={
        "reporting":     "publisher",
        "n_activities":  "n_activities",
        "total_de_usd":  "total_de_usd",
    }).to_excel(writer, sheet_name="By publisher", index=False)
    ws2 = writer.sheets["By publisher"]
    ws2.set_column("A:A", 50)
    ws2.set_column("B:C", 18)

    # Sheet 3: summary
    summary_rows = [
        ["Run date",                         str(date.today())],
        ["d-portal activities (non-multi KM)", len(dp_full)],
        ["Present in IATI Tables",             len(present)],
        ["Missing from IATI Tables",           len(missing)],
        ["Present D+E total (USD)",            round(present_total, 2)],
        ["Missing D+E total (USD)",            round(missing_total, 2)],
        ["Coverage gap (%)",                   round(missing_total / overall_total * 100, 2)],
    ]
    pd.DataFrame(summary_rows, columns=["Metric", "Value"]).to_excel(
        writer, sheet_name="Summary", index=False)
    writer.sheets["Summary"].set_column("A:A", 38)
    writer.sheets["Summary"].set_column("B:B", 22)

print(f"Done. {len(missing)} missing activities saved to {OUT_FILE}")
