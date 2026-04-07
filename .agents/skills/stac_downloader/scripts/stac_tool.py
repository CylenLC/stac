import os
import json
import argparse
import requests
from typing import List, Dict, Any
from shapely.wkt import loads as load_wkt
from pystac_client import Client
import planetary_computer as pc

STAC_CATALOGS = {
    "microsoft": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "earth-search": "https://earth-search.aws.element84.com/v1",
}
NASA_CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"

def resolve_asset_url(asset_info: dict, catalog: str) -> str:
    href = asset_info.get("href", "")
    if catalog == "microsoft" or "planetarycomputer" in href:
        if asset_info.get("msft:https-url"):
            href = asset_info.get("msft:https-url")
        if href.startswith("http"):
            try:
                return pc.sign(href)
            except:
                return href
    return href

def search(catalog_key: str, wkt: str, collections: List[str], start_date: str, end_date: str, max_items: int):
    aoi = load_wkt(wkt)
    bbox = aoi.bounds
    
    if catalog_key == "nasa":
        print(f"Searching NASA CMR catalog for {collections}...")
        temporal_str = f"{start_date}T00:00:00Z,{end_date}T23:59:59Z"
        all_items = []
        for col_id in collections:
            params = {
                "short_name": col_id,
                "bounding_box": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                "temporal": temporal_str,
                "page_size": max_items,
            }
            r = requests.get(NASA_CMR_URL, params=params)
            r.raise_for_status()
            entries = r.json().get("feed", {}).get("entry", [])
            for entry in entries:
                item = {
                    "id": entry.get("id"),
                    "collection": col_id,
                    "properties": {"datetime": entry.get("time_start")},
                    "assets": {}
                }
                for link in entry.get("links", []):
                    href = link.get("href", "")
                    if href.endswith((".nc", ".zip", ".nc.zip")) and "opendap" not in href.lower():
                        item["assets"]["data"] = {"href": href, "roles": ["data"]}
                        break
                all_items.append(item)
        print(f"Found {len(all_items)} NASA CMR items.")
        return all_items

    catalog_url = STAC_CATALOGS.get(catalog_key)
    if not catalog_url:
        print(f"Error: Invalid catalog '{catalog_key}'")
        return []
        
    dt_str = f"{start_date}/{end_date}"
    print(f"Searching {catalog_key} STAC catalog...")
    client = Client.open(catalog_url)
    search_obj = client.search(
        collections=collections,
        intersects=aoi,
        datetime=dt_str,
        max_items=max_items
    )
    items = [item.to_dict() for item in search_obj.items()]
    print(f"Found {len(items)} items.")
    return items

def download(catalog_key: str, items: List[dict], output_dir: str, only_main: bool):
    os.makedirs(output_dir, exist_ok=True)
    
    for item in items:
        cid = item.get("collection") or "unknown"
        iid = item.get("id", "unknown")
        item_dir = os.path.join(output_dir, cid, iid)
        os.makedirs(item_dir, exist_ok=True)
        
        assets = item.get("assets", {})
        if not assets: continue

        for a_key, a_info in assets.items():
            roles = a_info.get("roles", [])
            is_main = any(r in ["data", "visual", "overview"] for r in roles) or a_key in ["visual", "rendered_preview", "B04", "B08", "data"]
            
            if only_main and not is_main:
                continue
            
            url = resolve_asset_url(a_info, catalog_key)
            filename = url.split("/")[-1].split("?")[0] or f"{a_key}.data"
            save_path = os.path.join(item_dir, filename)
            
            if os.path.exists(save_path):
                print(f"  [Skip] {filename} exists.")
                continue
                
            print(f"  [Downloading] {filename} ...")
            try:
                r = requests.get(url, stream=True, timeout=60)
                r.raise_for_status()
                with open(save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=32768):
                        f.write(chunk)
            except Exception as e:
                print(f"  [Error] Failed to download {filename}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="STAC & CMR Tool")
    subparsers = parser.add_subparsers(dest="command")
    s_parser = subparsers.add_parser("search")
    s_parser.add_argument("--wkt", required=True)
    s_parser.add_argument("--collections", required=True)
    s_parser.add_argument("--start", default="2023-12-01")
    s_parser.add_argument("--end", default="2023-12-31")
    s_parser.add_argument("--catalog", default="microsoft", choices=["microsoft", "earth-search", "nasa"])
    s_parser.add_argument("--max", type=int, default=50)
    s_parser.add_argument("--output", help="Save search results to JSON")
    
    d_parser = subparsers.add_parser("download")
    d_parser.add_argument("--input", required=True)
    d_parser.add_argument("--catalog", default="microsoft", choices=["microsoft", "earth-search", "nasa"])
    d_parser.add_argument("--outdir", default="downloads")
    d_parser.add_argument("--all", action="store_true")
    
    args = parser.parse_args()
    if args.command == "search":
        col_list = [c.strip() for c in args.collections.split(",")]
        items = search(args.catalog, args.wkt, col_list, args.start, args.end, args.max)
        if args.output:
            with open(args.output, "w") as f:
                json.dump(items, f, indent=2)
            print(f"Results saved to {args.output}")
        else:
            print(json.dumps(items, indent=2))
    elif args.command == "download":
        with open(args.input, "r") as f:
            items = json.load(f)
        download(args.catalog, items, args.outdir, not args.all)
