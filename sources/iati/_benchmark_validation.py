#!/usr/bin/env python3
"""
BENCHMARK: IATI Tables vs d-portal — 3-level comparison for Comoros (KM)

Level 1 : Overall totals  (live d-portal query, not hardcoded reference)
Level 2 : Year-by-year breakdown  (spot whether gap is uniform or concentrated)
Level 3 : Top-10 activity spot-check  (same IATI identifier, both sources)
"""
import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
DPORTAL_BASE = "https://d-portal.org/q.json"
IATI_BASE    = "https://datasette-tables.iatistandard.org/iati.json"
IATI_HDR     = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
COUNTRY_CODE = "KM"
BATCH        = 500   # max IDs per IN clause


# ── Helpers ───────────────────────────────────────────────────────────────────
def dp(sql):
    """Query d-portal; returns DataFrame (rows are dicts)."""
    r = requests.get(DPORTAL_BASE, params={"sql": sql}, timeout=120)
    r.raise_for_status()
    d = r.json()
    if "error" in d:
        raise RuntimeError(f"d-portal error: {d['error']}")
    rows = d.get("rows", [])
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def it(sql, paginate=False):
    """Query IATI Tables; paginates only when paginate=True."""
    all_rows, cols, offset = [], None, 0
    PAGE = 2000
    while True:
        r = requests.get(IATI_BASE,
                         params={"sql": f"{sql} LIMIT {PAGE} OFFSET {offset}"},
                         timeout=120, headers=IATI_HDR)
        if not r.text.strip():
            raise RuntimeError(f"IATI Tables returned empty response (timeout?). SQL: {sql[:120]}")
        d = r.json()
        if not d.get("ok"):
            raise RuntimeError(f"IATI error: {d.get('error')}")
        rows = d.get("rows", [])
        cols = d.get("columns", [])
        all_rows.extend(rows)
        if not paginate or len(rows) < PAGE:
            break
        offset += PAGE
    return pd.DataFrame(all_rows, columns=cols) if all_rows else pd.DataFrame(columns=cols or [])


# ══════════════════════════════════════════════════════════════════════════════
# STEP 0 — Get the set of non-multinational KM activity link IDs
#          (same filter used in _check1_validation.py, so results are consistent)
# ══════════════════════════════════════════════════════════════════════════════
print("Step 0 — fetching non-multinational KM _link_activity IDs …")
sole_df  = it("""
    SELECT _link_activity
    FROM   recipientcountry
    WHERE  code = 'KM'
    GROUP  BY _link_activity
    HAVING COUNT(*) = 1 AND MAX(coalesce(percentage, 100)) >= 99
""", paginate=True)
link_ids = sole_df["_link_activity"].tolist()
print(f"  → {len(link_ids)} non-multinational links\n")

# Also fetch iatiidentifier + reporter for later use in spot-check
print("Step 0b — fetching iatiidentifier for those activities …")
act_chunks = []
for i in range(0, len(link_ids), BATCH):
    chunk   = link_ids[i:i + BATCH]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    act_chunks.append(it(f"""
        SELECT _link, iatiidentifier, reportingorg_narrative
        FROM   activity
        WHERE  _link IN ({in_list})
    """, paginate=False))
act_df = pd.concat(act_chunks, ignore_index=True) if act_chunks else pd.DataFrame()
print(f"  → {len(act_df)} activities matched\n")


# ══════════════════════════════════════════════════════════════════════════════
# LEVEL 1 — Overall D+E totals  (live queries, no hardcoded numbers)
# ══════════════════════════════════════════════════════════════════════════════
print("═"*68)
print("LEVEL 1 — Overall D+E totals (non-multinational, KM)")
print("═"*68)

# IATI Tables total
print("  IATI Tables: aggregating …")
it_total = 0.0
for i in range(0, len(link_ids), BATCH):
    chunk   = link_ids[i:i + BATCH]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    row = it(f"""
        SELECT SUM(value_usd) AS s
        FROM   transaction_breakdown
        WHERE  recipientcountry_code      = 'KM'
          AND  transactiontype_code IN ('3','4')
          AND  _link_activity IN ({in_list})
    """)
    it_total += float(row["s"].iloc[0] or 0)

# d-portal live total
# Non-multinational in d-portal ≡ country_percent = 100 (or NULL → defaults to 100)
print("  d-portal: aggregating (may take ~20 s) …")
dp_lvl1 = dp(f"""
    SELECT SUM(t.trans_usd)   AS total_usd,
           COUNT(DISTINCT t.aid) AS n_act
    FROM   trans    AS t
    JOIN   country  AS c ON c.aid = t.aid
    WHERE  c.country_code = '{COUNTRY_CODE}'
      AND  (c.country_percent = 100 OR c.country_percent IS NULL)
      AND  t.trans_code IN ('D','E')
""")
dp_total = float(dp_lvl1["total_usd"].iloc[0] or 0) if not dp_lvl1.empty else 0.0
dp_n_act = int(dp_lvl1["n_act"].iloc[0]    or 0) if not dp_lvl1.empty else 0

diff     = it_total - dp_total
diff_pct = diff / dp_total * 100 if dp_total else float("nan")

print(f"\n  {'Source':<20} {'Total USD':>18}  {'Activities':>10}")
print(f"  {'-'*20} {'-'*18}  {'-'*10}")
print(f"  {'IATI Tables':<20} ${it_total:>17,.0f}  {'N/A':>10}")
print(f"  {'d-portal (live)':<20} ${dp_total:>17,.0f}  {dp_n_act:>10,}")
print(f"\n  Gap: ${diff:>+,.0f}  ({diff_pct:>+.2f}%)")
if abs(diff_pct) < 2:
    verdict = "✅ < 2% — excellent match, likely pure FX rounding"
elif abs(diff_pct) < 5:
    verdict = "✅ < 5% — good match, explained by FX methodology or snapshot timing"
elif abs(diff_pct) < 10:
    verdict = "⚠  5–10% — moderate gap, worth investigating further"
else:
    verdict = "❌ > 10% — significant gap, requires investigation"
print(f"  {verdict}")


# ══════════════════════════════════════════════════════════════════════════════
# LEVEL 2 — Year-by-year breakdown
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "═"*68)
print("LEVEL 2 — Year-by-year breakdown (D+E, non-multinational, KM)")
print("═"*68)

# IATI Tables by year
# Fetch raw (date, value_usd) rows per batch; group by year in Python
# (avoids GROUP BY + large IN clause timing out on Datasette)
print("  IATI Tables: year breakdown (fetching raw rows, grouping in Python) …")
it_yr_chunks = []
for i in range(0, len(link_ids), BATCH):
    chunk   = link_ids[i:i + BATCH]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    it_yr_chunks.append(it(f"""
        SELECT transactiondate_isodate AS txdate, value_usd
        FROM   transaction_breakdown
        WHERE  recipientcountry_code      = 'KM'
          AND  transactiontype_code IN ('3','4')
          AND  _link_activity IN ({in_list})
          AND  transactiondate_isodate IS NOT NULL
    """, paginate=True))
it_yr_raw = pd.concat(it_yr_chunks, ignore_index=True)
it_yr_raw["value_usd"] = pd.to_numeric(it_yr_raw["value_usd"], errors="coerce").fillna(0)
it_yr_raw["year"]      = it_yr_raw["txdate"].str[:4]
it_yr = (it_yr_raw.groupby("year")["value_usd"]
                  .sum()
                  .reset_index()
                  .rename(columns={"value_usd": "it_usd"}))

# d-portal by year
# trans_day is stored as integer YYYYMMDD → divide by 10000 to get year
print("  d-portal: year breakdown …")
dp_yr = dp(f"""
    SELECT (t.trans_day / 10000)   AS year,
           SUM(t.trans_usd)         AS dp_usd
    FROM   trans   AS t
    JOIN   country AS c ON c.aid = t.aid
    WHERE  c.country_code = '{COUNTRY_CODE}'
      AND  (c.country_percent = 100 OR c.country_percent IS NULL)
      AND  t.trans_code IN ('D','E')
      AND  t.trans_day IS NOT NULL
    GROUP  BY (t.trans_day / 10000)
    ORDER  BY year
""")
if not dp_yr.empty:
    dp_yr["dp_usd"] = pd.to_numeric(dp_yr["dp_usd"], errors="coerce").fillna(0)
    dp_yr["year"]   = dp_yr["year"].astype(str)   # align type with IATI Tables

# Merge and display
yr_cmp = (pd.merge(it_yr, dp_yr, on="year", how="outer")
            .fillna(0)
            .sort_values("year"))
yr_cmp["gap_usd"] = yr_cmp["it_usd"] - yr_cmp["dp_usd"]
yr_cmp["gap_pct"] = (yr_cmp["gap_usd"]
                     / yr_cmp["dp_usd"].replace(0, float("nan"))) * 100

print(f"\n  {'Year':<6} {'IATI Tables':>15} {'d-portal':>15} {'Gap USD':>13} {'Gap%':>7}")
print(f"  {'-'*6} {'-'*15} {'-'*15} {'-'*13} {'-'*7}")
for _, row in yr_cmp.iterrows():
    flag = " ⚠" if abs(row["gap_pct"]) > 10 and row["dp_usd"] > 1e5 else ""
    print(f"  {str(row['year']):<6} "
          f"${row['it_usd']:>14,.0f} "
          f"${row['dp_usd']:>14,.0f} "
          f"${row['gap_usd']:>+12,.0f} "
          f"{row['gap_pct']:>+6.1f}%"
          f"{flag}")

uniform = yr_cmp[yr_cmp["dp_usd"] > 1e5]["gap_pct"].dropna()
if not uniform.empty:
    spread = uniform.std()
    print(f"\n  Gap std-dev across years: {spread:.1f}pp  "
          f"({'uniform → likely FX' if spread < 5 else 'variable → data freshness / missing txn'})")


# ══════════════════════════════════════════════════════════════════════════════
# LEVEL 3 — Top-10 activity spot-check
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "═"*68)
print("LEVEL 3 — Top-10 activity spot-check (same IATI identifier, both sources)")
print("═"*68)

# Per-activity totals from IATI Tables
print("  IATI Tables: per-activity totals …")
it_act_chunks = []
for i in range(0, len(link_ids), BATCH):
    chunk   = link_ids[i:i + BATCH]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    it_act_chunks.append(it(f"""
        SELECT _link_activity,
               SUM(value_usd) AS total_usd
        FROM   transaction_breakdown
        WHERE  recipientcountry_code      = 'KM'
          AND  transactiontype_code IN ('3','4')
          AND  _link_activity IN ({in_list})
        GROUP  BY _link_activity
    """))
it_act = pd.concat(it_act_chunks, ignore_index=True)
it_act["total_usd"] = it_act["total_usd"].astype(float)

# Join iatiidentifier
it_act = it_act.merge(
    act_df[["_link", "iatiidentifier", "reportingorg_narrative"]]
        .rename(columns={"_link": "_link_activity"}),
    on="_link_activity", how="left"
)
top10 = it_act.nlargest(10, "total_usd").reset_index(drop=True)

# d-portal spot-check per activity
print("  d-portal: querying each of the top 10 …")
spot = []
for _, row in top10.iterrows():
    iati_id = str(row["iatiidentifier"]).strip() if pd.notna(row["iatiidentifier"]) else ""
    if not iati_id:
        spot.append({"iati_id": "", "reporter": row["reportingorg_narrative"],
                     "it_usd": row["total_usd"], "dp_usd": None})
        continue
    safe_id = iati_id.replace("'", "''")
    dp_row = dp(f"""
        SELECT SUM(t.trans_usd) AS total_usd
        FROM   trans AS t
        WHERE  t.aid = '{safe_id}'
          AND  t.trans_code IN ('D','E')
    """)
    dp_val = (float(dp_row["total_usd"].iloc[0])
              if not dp_row.empty and pd.notna(dp_row["total_usd"].iloc[0])
              else 0.0)
    spot.append({"iati_id": iati_id,
                 "reporter": row["reportingorg_narrative"],
                 "it_usd": row["total_usd"],
                 "dp_usd": dp_val})

print(f"\n  {'#'} {'IATI Identifier':<40} {'IATI Tbl':>12} {'d-portal':>12} {'Gap%':>7}")
print(f"  {'-'} {'-'*40} {'-'*12} {'-'*12} {'-'*7}")
for idx, s in enumerate(spot, 1):
    dp_val = s["dp_usd"]
    if dp_val is None or dp_val == 0:
        gap_str = "  N/A" if dp_val is None else "+100.0%"
    else:
        gap_pct = (s["it_usd"] - dp_val) / dp_val * 100
        gap_str = f"{gap_pct:>+6.1f}%"
    iati_short = (s["iati_id"] or "N/A")[:39]
    dp_str = f"${dp_val:>11,.0f}" if dp_val is not None else "     NOT FOUND"
    print(f"  {idx} {iati_short:<40} ${s['it_usd']:>11,.0f} {dp_str} {gap_str}")

print("\n  Interpretation:")
missing = sum(1 for s in spot if s["dp_usd"] == 0.0 and s["iati_id"])
matched = sum(1 for s in spot if s["dp_usd"] and s["dp_usd"] > 0)
print(f"    Matched in d-portal   : {matched}/10")
print(f"    Zero/missing in dportal: {missing}/10")
if missing == 0:
    print("    ✅ All top-10 present in both sources — gap is FX rates, not missing activities")
else:
    print("    ⚠  Some activities missing in d-portal — partial data freshness gap")

print("\nBenchmark complete.")
