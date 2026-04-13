"""
Script to fetch the latest Comoros trade data releases from the public UN Comtrade API
and download the relevant data files (no API key required).
"""
import requests
import pandas as pd
import os

# Constants
RELEASES_URL = "https://comtradeapi.un.org/public/v1/getComtradeReleases"
COMOROS_CODE = 174  # UN Comtrade reporter code for Comoros
OUTPUT_DIR = "outputs/comtrade_comoros_latest"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_releases():
    resp = requests.get(RELEASES_URL)
    resp.raise_for_status()
    return resp.json()

def filter_comoros_releases(releases):
    # Filter for Comoros (reporterCode == 174)
    return [r for r in releases if r.get("reporterCode") == COMOROS_CODE]

def download_file(url, dest):
    print(f"Downloading: {url}")
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved to: {dest}")

def main():
    print("Fetching releases metadata...")
    data = fetch_releases()
    releases = data.get("data", [])
    # Debug: print the first few items of the raw API response
    print("First 3 items from API response:")
    import json
    print(json.dumps(releases[:3], indent=2))
    comoros_releases = filter_comoros_releases(releases)
    if not comoros_releases:
        print("No releases found for Comoros. Check the printed API response above for structure and available keys.")
        return
    # Sort by period (descending) to get the most recent first
    comoros_releases.sort(key=lambda x: x.get("period", ""), reverse=True)
    print(f"Found {len(comoros_releases)} releases for Comoros.")
    for rel in comoros_releases:
        period = rel.get('period')
        file_url = rel.get('fileUrl')
        if file_url:
            filename = os.path.join(OUTPUT_DIR, f"comoros_{period}.csv")
            if not os.path.exists(filename):
                try:
                    download_file(file_url, filename)
                except Exception as e:
                    print(f"Failed to download {file_url}: {e}")
            else:
                print(f"File already exists: {filename}")
        else:
            print(f"No file URL for period {period}")

if __name__ == "__main__":
    main()
