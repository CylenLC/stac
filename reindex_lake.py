"""Backfill Earth Lake registry metadata for assets downloaded before profile support."""

import argparse
from pathlib import Path

from earth_lake import EarthLake
from stac_core import get_collection_metadata


def source_catalog(local_path: str) -> str:
    parts = Path(local_path).parts
    return parts[1] if len(parts) > 1 and parts[0] == "source" else "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description="Reindex Earth Lake source assets and raster metadata")
    parser.add_argument("root", nargs="?", default="downloads", help="Earth Lake root directory")
    args = parser.parse_args()

    lake = EarthLake(args.root)
    collection_metadata: dict[str, dict] = {}
    for asset in lake._read_rows("assets"):
        item = lake._read_item_metadata(lake.root / Path(asset["local_path"]).parent / "metadata.json") or {}
        collection = str(item.get("collection") or asset["product_id"])
        if collection in collection_metadata:
            continue
        try:
            collection_metadata[collection] = get_collection_metadata(source_catalog(asset["local_path"]), collection)
        except Exception as exc:
            print(f"Warning: could not fetch collection metadata for {collection}: {exc}")

    asset_ids = lake.reindex_source_assets(collection_metadata)
    print(f"Reindexed {len(asset_ids)} source assets in {lake.root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
