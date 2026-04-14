#!/usr/bin/env python3
"""
Check whether the 163 activities missing from IATI Tables
actually exist in the IATI Datastore.

Endpoint: POST https://api.iatistandard.org/datastore/iati-identifiers/exist
Requires a free API key from https://developer.iatistandard.org/signup

Usage:
    IATI_KEY=your_key_here python3 _datastore_check.py

Three possible outcomes per activity:
  A) Found in Datastore  → the XML exists; IATI Tables just hasn't indexed it yet
  B) Not found anywhere  → genuinely unpublished / withdrawn
"""
import os
import requests
import pandas as pd

IATI_API_KEY = os.environ.get("IATI_KEY", "")

DPORTAL_BASE  = "https://d-portal.org/q.json"
IATI_BASE     = "https://datasette-tables.iatistandard.org/iati.json"
DATASTORE_URL = "https://api.iatistandard.org/datastore/iati-identifiers/exist"

IATI_HDR      = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
DS_HDR        = {"Content-Type": "application/json", "Accept": "application/json"}
if IATI_API_KEY:
    DS_HDR["Ocp-Apim-Subscription-Key"] = IATI_API_KEY
else:
    print("⚠  No IATI_KEY env var set. Get a free key at https://developer.iatistandard.org/signup")
    print("   Then run:  IATI_KEY=your_key python3 _datastore_check.py\n")
BATCH_IT      = 500
BATCH_DP      = 50
BATCH_DS      = 50    # Datastore POST — stay conservative


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


# ── Step 1: rebuild the list of 163 missing IDs ──────────────────────────────
print("Step 1 — IATI Tables: non-multinational KM identifier set …")
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
    act_chunks.append(it(f"SELECT iatiidentifier FROM activity WHERE _link IN ({in_list})"))
it_ids = set(pd.concat(act_chunks, ignore_index=True)["iatiidentifier"].dropna().str.strip())
print(f"  → {len(it_ids)} identifiers in IATI Tables\n")

print("Step 2 — d-portal: non-multinational KM activities …")
dp_acts = dp(f"""
    SELECT act.aid, act.reporting
    FROM   act
    JOIN   country ON country.aid = act.aid
    WHERE  country.country_code = 'KM'
      AND  (country.country_percent = 100 OR country.country_percent IS NULL)
""")
dp_ids_all = set(dp_acts["aid"].dropna().str.strip())

missing_ids = sorted(dp_ids_all - it_ids)
print(f"  → {len(missing_ids)} activities missing from IATI Tables\n")

# Map id → reporter for display
id_to_reporter = dict(zip(dp_acts["aid"].str.strip(), dp_acts["reporting"]))


# ── Step 2: query IATI Datastore in batches ───────────────────────────────────
print(f"Step 3 — IATI Datastore: checking {len(missing_ids)} identifiers …")
found_in_ds     = {}
not_found_in_ds = {}
errors          = []

for i in range(0, len(missing_ids), BATCH_DS):
    chunk = missing_ids[i:i + BATCH_DS]
    try:
        r = requests.post(
            DATASTORE_URL,
            json={"iati_identifiers": chunk},
            headers=DS_HDR,
            timeout=60
        )
        if r.status_code == 401:
            print("  ⚠  401 Unauthorized — API key required. Set Ocp-Apim-Subscription-Key header.")
            print(f"     Response: {r.text[:300]}")
            break
        r.raise_for_status()
        d = r.json()
        found_in_ds.update(d.get("iati_identifiers_found", {}))
        not_found_in_ds.update(d.get("iati_identifiers_not_found", {}))
        print(f"  batch {i//BATCH_DS + 1}: "
              f"{len(d.get('iati_identifiers_found', {}))} found, "
              f"{len(d.get('iati_identifiers_not_found', {}))} not found")
    except Exception as e:
        errors.append(str(e))
        print(f"  ERROR batch {i//BATCH_DS + 1}: {e}")

if errors:
    print(f"\n  {len(errors)} batch errors — partial results only")

# ── Step 3: display results ───────────────────────────────────────────────────
print(f"\n{'═'*64}")
print(f"RESULTS — {len(missing_ids)} activities missing from IATI Tables")
print(f"{'═'*64}")
print(f"  Found in IATI Datastore     : {len(found_in_ds):>4}  → XML exists, Tables just hasn't indexed them")
print(f"  Not found in IATI Datastore : {len(not_found_in_ds):>4}  → Genuinely withdrawn or unpublished")
print()

if found_in_ds:
    print("  Present in Datastore but MISSING from IATI Tables (top 30):")
    print(f"  {'IATI Identifier':<55} {'Occurrences':>11}  Publisher")
    print(f"  {'-'*55} {'-'*11}  {'-'*30}")
    for iid, meta in list(found_in_ds.items())[:30]:
        occ = meta.get("occurrences", "?") if isinstance(meta, dict) else "?"
        reporter = str(id_to_reporter.get(iid, "")).strip()[:40]
        print(f"  {iid:<55} {str(occ):>11}  {reporter}")

if not_found_in_ds:
    print(f"\n  Not found anywhere (sample of 20):")
    print(f"  {'IATI Identifier':<55}  Publisher")
    print(f"  {'-'*55}  {'-'*30}")
    for iid in list(not_found_in_ds.keys())[:20]:
        reporter = str(id_to_reporter.get(iid, "")).strip()[:40]
        print(f"  {iid:<55}  {reporter}")

print("\nDone.")
