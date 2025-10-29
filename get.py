import requests
import csv
import json
from io import StringIO

# -------------------------------
# CONFIGURATION
# -------------------------------
API_KEY = "ZusbR2G0ugcJUeumHhtN4mNfu6KmlshMzJTdzLJ9"
STATE = "WA"
ACCESS = "public"
OUTPUT_FILE = "ev_charging_units.geojson"
FORMAT = "geojson"
RESPONSE_FORMAT = "compact"
RESOURCE = "ev-charging-units"
# Only keep stations with more than this many J3400 connectors
MIN_J3400 = 1


def _find_key_case_insensitive(row, candidates):
    """Return the first key from row that matches any candidate substring (case-insensitive)."""
    lower_keys = {k.lower(): k for k in row.keys()}
    for cand in candidates:
        for lk, orig in lower_keys.items():
            if cand.lower() in lk:
                return orig
    return None


def csv_to_geojson(csv_text):
    """Convert CSV text to a GeoJSON FeatureCollection.

    Detects latitude/longitude field names (case-insensitive) and includes all other
    fields as properties.
    """
    f = StringIO(csv_text)
    reader = csv.DictReader(f)

    features = []
    for row in reader:
        lat_key = _find_key_case_insensitive(row, ["latitude", "lat"])
        lon_key = _find_key_case_insensitive(row, ["longitude", "lon", "lng"])
        if not lat_key or not lon_key:
            continue
        try:
            lat = float(row[lat_key])
            lon = float(row[lon_key])
        except Exception:
            continue

        props = {k: v for k, v in row.items() if k not in [lat_key, lon_key]}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features}


def filter_geojson_j3400(feature_collection, min_count=1):
    """Return a new FeatureCollection containing only features with EV J3400 Connector Count >= min_count.

    This looks for a property name containing 'j3400' case-insensitively.
    """
    features = feature_collection.get("features", [])
    if not features:
        return {"type": "FeatureCollection", "features": []}

    # find property key in first feature (case-insensitive search)
    first_props = features[0].get("properties", {})
    j3400_key = None
    for k in first_props.keys():
        if "j3400" in k.lower():
            j3400_key = k
            break

    if j3400_key is None:
        print("⚠️ Could not find a 'J3400' connector count property in features; returning empty collection.")
        return {"type": "FeatureCollection", "features": []}

    out = []
    for feat in features:
        props = feat.get("properties", {})
        val = props.get(j3400_key, "0")
        try:
            num = int(float(val)) if val not in (None, "") else 0
        except Exception:
            num = 0
        if num >= min_count:
            out.append(feat)

    return {"type": "FeatureCollection", "features": out}

# -------------------------------
# API REQUEST (GeoJSON output)
# See: https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/all/#geojson-output-format
# -------------------------------
# The API supports several endpoints and output formats. Request the .geojson endpoint
# directly to receive a GeoJSON FeatureCollection from the API instead of CSV.
url = f"https://developer.nrel.gov/api/alt-fuel-stations/v1/{RESOURCE}.csv"
params = {
    "api_key": API_KEY,
    "state": STATE,
    "access": ACCESS,
}

print(f"Requesting EV charging data (GeoJSON) for state={STATE} ...")


response = requests.get(url, params=params)


# -------------------------------
# HANDLE RESPONSE
# -------------------------------
if response.status_code == 200:
    # Try to parse JSON (if API returned geojson). If that fails, assume CSV and convert.
    try:
        data = response.json()
        # Basic validation: expect a FeatureCollection
        if isinstance(data, dict) and data.get("type") == "FeatureCollection":
            features = data.get("features", [])
            # filter by J3400 connector count
            filtered = filter_geojson_j3400(data, min_count=MIN_J3400)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(filtered, f, ensure_ascii=False, indent=2)
            print(f"✅ GeoJSON saved successfully as '{OUTPUT_FILE}'. Features: {len(features)} -> {len(filtered.get('features', []))} (filtered)")
        else:
            # Save whatever JSON we got
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"⚠️ Received JSON but it is not a FeatureCollection. Saved raw JSON to '{OUTPUT_FILE}'.")
    except ValueError:
        # Not JSON — assume CSV and convert
        try:
            geo = csv_to_geojson(response.text)
            filtered = filter_geojson_j3400(geo, min_count=MIN_J3400)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(filtered, f, ensure_ascii=False, indent=2)
            print(f"✅ Converted CSV -> GeoJSON and saved to '{OUTPUT_FILE}'. Features: {len(geo.get('features', []))} -> {len(filtered.get('features', []))} (filtered)")
        except Exception as e:
            print("❌ Failed to convert CSV response to GeoJSON:", e)
            print(response.text[:1000])
else:
    print(f"❌ Error: HTTP {response.status_code}")
    print(response.text)


if __name__ == "__main__":
    print(f"Wrote output (if request succeeded) to: {OUTPUT_FILE}")
