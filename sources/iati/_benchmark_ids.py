#!/usr/bin/env python3
"""
ID-level benchmark: compare the set of IATI identifiers between
IATI Tables and d-portal for non-multinational KM activities.

If the gap is purely FX rates → same IDs, different amounts.
If activities are missing from one source → IDs diverge.
"""
import requests
import pandas as pd

DPORTAL_BASE = "https://d-portal.org/q.json"
IATI_BASE    = "https://datasette-tables.iatistandard.org/iati.json"
IATI_HDR     = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
BATCH        = 500


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


# ── 1. IATI Tables: non-multinational KM identifiers ─────────────────────────
print("IATI Tables: fetching non-multinational KM activity IDs …")
sole_df  = it("""
    SELECT _link_activity
    FROM   recipientcountry
    WHERE  code = 'KM'
    GROUP  BY _link_activity
    HAVING COUNT(*) = 1 AND MAX(coalesce(percentage, 100)) >= 99
""", paginate=True)
link_ids = sole_df["_link_activity"].tolist()
print(f"  → {len(link_ids)} link IDs")

# Resolve to iatiidentifier
act_chunks = []
for i in range(0, len(link_ids), BATCH):
    chunk   = link_ids[i:i + BATCH]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    act_chunks.append(it(f"""
        SELECT iatiidentifier, reportingorg_narrative
        FROM   activity
        WHERE  _link IN ({in_list})
    """))
it_acts = pd.concat(act_chunks, ignore_index=True)
it_ids  = set(it_acts["iatiidentifier"].dropna().str.strip())
print(f"  → {len(it_ids)} unique iatiidentifiers\n")


# ── 2. d-portal: non-multinational KM identifiers ────────────────────────────
print("d-portal: fetching non-multinational KM activity IDs …")
dp_acts = dp(f"""
    SELECT act.aid
    FROM   act
    JOIN   country ON country.aid = act.aid
    WHERE  country.country_code = 'KM'
      AND  (country.country_percent = 100 OR country.country_percent IS NULL)
""")
dp_ids = set(dp_acts["aid"].dropna().str.strip()) if not dp_acts.empty else set()
print(f"  → {len(dp_ids)} unique activity IDs\n")


# ── 3. Set comparison ─────────────────────────────────────────────────────────
only_it  = it_ids - dp_ids     # in IATI Tables but NOT in d-portal
only_dp  = dp_ids - it_ids     # in d-portal but NOT in IATI Tables
in_both  = it_ids & dp_ids

print("═"*64)
print("ID SET COMPARISON")
print("═"*64)
print(f"  In both sources           : {len(in_both):>5,}")
print(f"  Only in IATI Tables       : {len(only_it):>5,}")
print(f"  Only in d-portal          : {len(only_dp):>5,}")
print()

if len(only_it) == 0 and len(only_dp) == 0:
    print("  ✅ Identical ID sets — gap is 100% FX rate methodology")
elif len(only_it) == 0 and len(only_dp) > 0:
    print("  ⚠  d-portal has activities IATI Tables does not → d-portal has more D+E")
elif len(only_it) > 0 and len(only_dp) == 0:
    print("  ⚠  IATI Tables has activities d-portal does not → IATI Tables has more D+E")
else:
    print("  ⚠  Both sources have exclusive activities — partial data divergence")


# ── 4. Value of exclusive activities ─────────────────────────────────────────
if only_it:
    print(f"\n  Activities ONLY in IATI Tables ({len(only_it)}):")
    mask   = it_acts["iatiidentifier"].str.strip().isin(only_it)
    subset = it_acts[mask][["iatiidentifier","reportingorg_narrative"]].head(20)
    # Get their D+E values from IATI Tables
    only_it_links = sole_df[sole_df["_link_activity"].isin(
        it(f"""
            SELECT _link FROM activity
            WHERE  iatiidentifier IN ({", ".join(f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in list(only_it)[:BATCH])})
        """)["_link"].tolist() if len(only_it) <= BATCH else []
    )]["_link_activity"].tolist() if len(only_it) <= BATCH else []

    for _, row in subset.iterrows():
        print(f"    {str(row['iatiidentifier']):<55} {str(row['reportingorg_narrative'])[:40]}")

if only_dp:
    print(f"\n  Activities ONLY in d-portal ({len(only_dp)}):")
    mask   = dp_acts["aid"].str.strip().isin(only_dp)
    subset = dp_acts[mask]["aid"].tolist()[:20]

    # Get their D+E USD total from d-portal
    in_list = ", ".join(f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in subset[:BATCH])
    dp_excl = dp(f"""
        SELECT t.aid, SUM(t.trans_usd) AS total_usd
        FROM   trans AS t
        WHERE  t.aid IN ({in_list})
          AND  t.trans_code IN ('D','E')
        GROUP  BY t.aid
        ORDER  BY total_usd DESC
    """)
    if not dp_excl.empty:
        dp_excl["total_usd"] = pd.to_numeric(dp_excl["total_usd"], errors="coerce").fillna(0)
        excl_total = dp_excl["total_usd"].sum()
        print(f"    Combined D+E for shown sample: ${excl_total:,.0f}")
        for _, row in dp_excl.head(10).iterrows():
            print(f"    {str(row['aid']):<55} ${row['total_usd']:>12,.0f}")


# ── 5. Per-activity amount comparison for shared IDs ─────────────────────────
print(f"\n{'═'*64}")
print(f"AMOUNT COMPARISON — shared IDs (sample of 20 largest by IATI Tables)")
print(f"{'═'*64}")

# Get IATI Tables amounts for activities in both
it_amounts_chunks = []
for i in range(0, len(link_ids), BATCH):
    chunk   = link_ids[i:i + BATCH]
    in_list = ", ".join(f"'{x}'" for x in chunk)
    it_amounts_chunks.append(it(f"""
        SELECT iatiidentifier, SUM(value_usd) AS it_usd
        FROM   transaction_breakdown
        WHERE  recipientcountry_code = 'KM'
          AND  transactiontype_code IN ('3','4')
          AND  _link_activity IN ({in_list})
        GROUP  BY iatiidentifier
    """))
it_amounts = pd.concat(it_amounts_chunks, ignore_index=True)
it_amounts["it_usd"] = pd.to_numeric(it_amounts["it_usd"], errors="coerce").fillna(0)
# Keep only shared IDs
it_amounts = it_amounts[it_amounts["iatiidentifier"].str.strip().isin(in_both)]
top20 = it_amounts.nlargest(20, "it_usd").reset_index(drop=True)

# d-portal amounts for same IDs
ids_list = ", ".join(f"'{x.replace(chr(39), chr(39)*2)}'" for x in top20["iatiidentifier"].tolist())
dp_amounts = dp(f"""
    SELECT t.aid, SUM(t.trans_usd) AS dp_usd
    FROM   trans AS t
    WHERE  t.aid IN ({ids_list})
      AND  t.trans_code IN ('D','E')
    GROUP  BY t.aid
""")
if not dp_amounts.empty:
    dp_amounts["dp_usd"] = pd.to_numeric(dp_amounts["dp_usd"], errors="coerce").fillna(0)
    dp_amounts = dp_amounts.rename(columns={"aid": "iatiidentifier"})

cmp = top20.merge(dp_amounts, on="iatiidentifier", how="left").fillna({"dp_usd": 0})
cmp["gap_usd"] = cmp["it_usd"] - cmp["dp_usd"]
cmp["gap_pct"] = (cmp["gap_usd"] / cmp["dp_usd"].replace(0, float("nan"))) * 100

print(f"\n  {'IATI Identifier':<45} {'IATI Tbl':>12} {'d-portal':>12} {'Gap%':>7}")
print(f"  {'-'*45} {'-'*12} {'-'*12} {'-'*7}")
for _, row in cmp.iterrows():
    gap_str = f"{row['gap_pct']:>+6.1f}%" if pd.notna(row["gap_pct"]) else "   N/A"
    flag = " ⚠" if pd.notna(row["gap_pct"]) and abs(row["gap_pct"]) > 20 else ""
    print(f"  {str(row['iatiidentifier'])[:44]:<45} "
          f"${row['it_usd']:>11,.0f} "
          f"${row['dp_usd']:>11,.0f} "
          f"{gap_str}{flag}")

avg_gap = cmp["gap_pct"].dropna()
print(f"\n  Median gap across top-20 shared activities: {avg_gap.median():>+.1f}%")
print(f"  Std-dev: {avg_gap.std():.1f}pp")
print()
if avg_gap.std() < 5:
    print("  ✅ Uniform gap across activities → consistent FX rate difference")
else:
    print("  ⚠  Variable gap across activities → data completeness differences per activity")

print("\nDone.")
