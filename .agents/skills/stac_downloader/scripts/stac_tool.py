import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from earth_lake import EarthLake
from stac_core import (
    asset_filename,
    download_asset,
    get_collection_metadata,
    get_asset_size,
    http_session,
    resolve_asset_url,
    search_items,
    selected_assets,
)


def search(catalog: str, wkt: str, collections: list[str], start: str, end: str, max_items: int) -> list[dict]:
    print(f"Searching {catalog} for {collections}...")
    items = search_items(catalog, wkt, collections, start, end, max_items)
    print(f"Found {len(items)} items.")
    return items


def download(catalog: str, items: list[dict], output_dir: str, only_main: bool) -> int:
    lake = EarthLake(output_dir)
    run_id = lake.start_run(
        {"interface": "cli", "catalog": catalog, "only_main": only_main, "item_count": len(items)}
    )
    session = http_session()
    failures: list[str] = []
    output_asset_ids: list[str] = []
    downloaded = 0
    skipped = 0
    collection_metadata_by_id: dict[str, dict] = {}
    for collection in {str(item.get("collection")) for item in items if item.get("collection")}:
        try:
            collection_metadata_by_id[collection] = get_collection_metadata(catalog, collection)
        except Exception as exc:
            print(f"  [Warning] Could not fetch metadata for {collection}: {exc}", file=sys.stderr)

    for item in items:
        assets = list(selected_assets(item, only_main))
        if not assets:
            failures.append(f"{item.get('id', 'unknown')}: no matching downloadable assets")
            continue
        for key, asset in assets:
            try:
                directory = lake.source_item_directory(catalog, item.get("collection"), item.get("id"))
                directory.mkdir(parents=True, exist_ok=True)
                metadata_path = directory / "metadata.json"
                if not metadata_path.exists():
                    metadata_path.write_text(json.dumps(item, indent=2), encoding="utf-8")
                destination = directory / asset_filename(key, asset)
                url = resolve_asset_url(asset, catalog)
                expected_size = get_asset_size(session, url)
                if destination.exists():
                    actual_size = destination.stat().st_size
                    if expected_size and actual_size != expected_size:
                        raise ValueError(f"existing file size {actual_size} does not match expected {expected_size}")
                    skipped += 1
                    print(f"  [Skip] {destination.name} exists.")
                    output_asset_ids.append(
                        lake.record_asset(
                            run_id=run_id,
                            catalog=catalog,
                            item=item,
                            asset_key=key,
                            source_url=asset.get("href", ""),
                            local_path=destination,
                            status="skipped",
                            collection_metadata=collection_metadata_by_id.get(item.get("collection")),
                        )
                    )
                    continue
                print(f"  [Downloading] {destination.name} ...")
                download_asset(session, url, destination)
                output_asset_ids.append(
                    lake.record_asset(
                        run_id=run_id,
                        catalog=catalog,
                        item=item,
                        asset_key=key,
                        source_url=asset.get("href", ""),
                        local_path=destination,
                        status="downloaded",
                        collection_metadata=collection_metadata_by_id.get(item.get("collection")),
                    )
                )
                downloaded += 1
            except Exception as exc:
                failures.append(f"{item.get('id', 'unknown')}/{key}: {exc}")
                print(f"  [Error] {failures[-1]}", file=sys.stderr)

    completed = downloaded + skipped
    status = "partial" if failures and completed else "failed" if failures else "completed"
    lake.finish_run(run_id, status, output_asset_ids)
    print(f"Downloaded: {downloaded}, skipped: {skipped}, failed: {len(failures)}")
    print(f"Protocol root: {lake.root} (run_id={run_id})")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="STAC & NASA CMR Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("--wkt", required=True)
    search_parser.add_argument("--collections", required=True)
    search_parser.add_argument("--start", required=True)
    search_parser.add_argument("--end", required=True)
    search_parser.add_argument("--catalog", default="microsoft", choices=["microsoft", "earth-search", "nasa"])
    search_parser.add_argument("--max", type=int, default=50)
    search_parser.add_argument("--output", help="Save search results to JSON")

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("--input", required=True)
    download_parser.add_argument("--catalog", default="microsoft", choices=["microsoft", "earth-search", "nasa"])
    download_parser.add_argument("--outdir", default="downloads")
    download_parser.add_argument("--all", action="store_true")

    args = parser.parse_args()
    if args.command == "search":
        if args.max < 1:
            parser.error("--max must be at least 1")
        collections = [value.strip() for value in args.collections.split(",") if value.strip()]
        if not collections:
            parser.error("--collections must include at least one collection")
        items = search(args.catalog, args.wkt, collections, args.start, args.end, args.max)
        if args.output:
            Path(args.output).write_text(json.dumps(items, indent=2), encoding="utf-8")
            print(f"Results saved to {args.output}")
        else:
            print(json.dumps(items, indent=2))
        return 0

    items = json.loads(Path(args.input).read_text(encoding="utf-8"))
    return download(args.catalog, items, args.outdir, not args.all)


if __name__ == "__main__":
    raise SystemExit(main())
