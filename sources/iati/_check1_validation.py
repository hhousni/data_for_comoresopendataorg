import requests
import pandas as pd

BASE = "https://datasette-tables.iatistandard.org/iati.json"
HDR  = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
DPORTAL_REFERENCE = 1_726_470_646  # figure confirmed by IATI team email

def query(sql):
    all_rows, cols, offset = [], None, 0
    while True:
        r = requests.get(BASE, params={"sql": f"{sql} LIMIT 2000 OFFSET {offset}"},
                         timeout=60, headers=HDR)
        d = r.json()
        if not d.get("ok"):
            print("ERROR:", d.get("error"))
            break
        rows = d.get("rows", [])
        cols = d.get("columns", [])
        all_rows.extend(rows)
        print(f"  {len(all_rows)} rows fetched …")
        if len(rows) < 2000:
            break
        offset += 2000
    return pd.DataFrame(all_rows, columns=cols) if all_rows else pd.DataFrame(columns=cols or [])

# ── Check 1: non-multinational only ──────────────────────────────────────────
# d-portal defines "non-multinational" as KM being the sole recipient country.
# In IATI Tables: recipientcountry has exactly 1 row for that activity with code=KM.
# transaction_breakdown already applies percentage_used → for 100% activities,
# value_usd is the full transaction value at IMF exchange rates.

print("Step A: fetching KM-only activity link IDs …")
# First get the list of _link_activity values where KM is the sole recipient.
# Do it in one aggregated call — returns a small set.
sole_sql = """
    SELECT _link_activity
    FROM recipientcountry
    WHERE code = 'KM'
    GROUP BY _link_activity
    HAVING COUNT(*) = 1 AND MAX(coalesce(percentage, 100)) >= 99
"""
sole_df = query(sole_sql)
link_ids = sole_df["_link_activity"].tolist()
print(f"  → {len(link_ids)} non-multinational activity links")

print("Step B: summing D+E transactions for those activities …")
# Build IN list in batches of 500 to stay under URL limits
BATCH = 500
total_usd = 0.0
total_acts = set()

for i in range(0, len(link_ids), BATCH):
    chunk = link_ids[i:i + BATCH]
    ids_sql = ", ".join(f"'{x}'" for x in chunk)
    agg_sql = f"""
        SELECT
            COUNT(DISTINCT iatiidentifier) AS n_act,
            SUM(value_usd) AS total_usd
        FROM transaction_breakdown
        WHERE recipientcountry_code = 'KM'
          AND transactiontype_code IN ('3', '4')
          AND _link_activity IN ({ids_sql})
    """
    r = requests.get(BASE, params={"sql": agg_sql}, timeout=60, headers=HDR)
    d = r.json()
    if not d.get("ok"):
        print("  ERROR:", d.get("error"))
        continue
    row = d["rows"][0]
    cols = d["columns"]
    batch_total = float(row[cols.index("total_usd")] or 0)
    batch_acts  = int(row[cols.index("n_act")] or 0)
    total_usd  += batch_total
    total_acts.add(batch_acts)
    print(f"  batch {i//BATCH + 1}: ${batch_total:,.0f}")

total      = total_usd
activities = sum(total_acts)
diff       = total - DPORTAL_REFERENCE
diff_pct   = diff / DPORTAL_REFERENCE * 100

print()
print("=" * 60)
print(f"  Activities (non-multinational) : {activities:>6,}")
print(f"  IATI Tables total (USD)        : ${total:>16,.0f}")
print(f"  d-portal reference (USD)       : ${DPORTAL_REFERENCE:>16,.0f}")
print(f"  Difference                     : ${diff:>+16,.0f}  ({diff_pct:+.2f}%)")
print("=" * 60)
print()
if abs(diff_pct) < 5:
    print("  ✅ Within 5% — sources are equivalent (gap explained by FX rates)")
elif abs(diff_pct) < 10:
    print("  ⚠  Gap 5-10% — likely FX + data freshness, worth noting")
else:
    print("  ❌ Gap > 10% — needs investigation")
