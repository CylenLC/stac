import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from earth_lake import EarthLake
from acquisition import AcquisitionManager, AcquisitionRequest
from stac_core import (
    AssetResolutionError,
    canonical_asset_path,
    classify_asset,
    download_asset_checked,
    get_collection_metadata,
    get_asset_size,
    http_session,
    resolve_asset_url,
    search_items,
    resolve_assets,
)
from stac_identity import AssetIdentity
from stac_integrity import IntegrityGate


DEFAULT_OUTPUT_DIR = os.environ.get("EARTH_LAKE_ROOT", "/Volumes/Untitled/stac")


def search(catalog: str, wkt: str, collections: list[str], start: str, end: str, max_items: int) -> list[dict]:
    print(f"Searching {catalog} for {collections}...")
    items = search_items(catalog, wkt, collections, start, end, max_items)
    print(f"Found {len(items)} items.")
    return items


def download(
    catalog: str,
    items: list[dict],
    output_dir: str,
    only_main: bool,
    asset_keys: list[str] | None = None,
) -> int:
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
        selector = "explicit" if asset_keys is not None else "main" if only_main else "all"
        try:
            resolution = resolve_assets(item, catalog, mode=selector, asset_keys=asset_keys)
            assets = list(resolution.selected)
        except AssetResolutionError as exc:
            failures.append(str(exc))
            print(f"  [Error] {failures[-1]}", file=sys.stderr)
            continue
        for key, asset in assets:
            try:
                directory = lake.source_item_directory(catalog, item.get("collection"), item.get("id"))
                directory.mkdir(parents=True, exist_ok=True)
                metadata_path = directory / "metadata.json"
                if not metadata_path.exists():
                    metadata_path.write_text(json.dumps(item, indent=2), encoding="utf-8")
                identity = AssetIdentity.from_item(catalog, item, key)
                destination = canonical_asset_path(lake.root, identity, asset)
                url = resolve_asset_url(asset, catalog)
                expected_size = get_asset_size(session, url)
                if destination.exists():
                    cache_result = IntegrityGate.validate(destination, asset, expected_size=expected_size or None)
                    if cache_result.ok:
                        skipped += 1
                        print(f"  [Skip] {destination.name} exists and passed integrity checks.")
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
                                integrity=cache_result,
                                asset_category=classify_asset(key, asset),
                            )
                        )
                        continue
                    print(f"  [Warning] Existing cache failed integrity checks; replacing it: {cache_result}", file=sys.stderr)
                print(f"  [Downloading] {destination.name} ...")
                integrity = download_asset_checked(
                    session,
                    url,
                    destination,
                    asset_metadata=asset,
                    expected_size=expected_size or None,
                    asset_identity=identity,
                )
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
                        integrity=integrity,
                        asset_category=classify_asset(key, asset),
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
    download_parser.add_argument("--outdir", default=DEFAULT_OUTPUT_DIR)
    download_parser.add_argument("--all", action="store_true")
    download_parser.add_argument("--asset", dest="asset_keys", action="append", help="Explicit Asset key; repeat for multiple keys")

    acquire_parser = subparsers.add_parser("acquire", help="Create and execute a durable Acquisition Run")
    acquire_parser.add_argument("--wkt", required=True)
    acquire_parser.add_argument("--collections", required=True)
    acquire_parser.add_argument("--start", required=True)
    acquire_parser.add_argument("--end", required=True)
    acquire_parser.add_argument("--catalog", default="nasa", choices=["microsoft", "earth-search", "nasa"])
    acquire_parser.add_argument("--max", type=int)
    acquire_parser.add_argument("--outdir", default=DEFAULT_OUTPUT_DIR)
    acquire_parser.add_argument("--all", action="store_true")
    acquire_parser.add_argument("--asset", dest="asset_keys", action="append", help="Explicit Asset key; repeat for multiple keys")
    acquire_parser.add_argument("--idempotency-key", required=True)

    args = parser.parse_args()
    if args.command == "acquire":
        collections = [value.strip() for value in args.collections.split(",") if value.strip()]
        if not collections:
            parser.error("--collections must include at least one collection")
        manager = AcquisitionManager(args.outdir)
        run_id = manager.create_run(
            AcquisitionRequest(
                catalog=args.catalog, collections=collections, wkt=args.wkt,
                start_date=args.start, end_date=args.end, max_items=args.max,
                only_main=not args.all,
                asset_keys=args.asset_keys,
            ),
            args.idempotency_key,
        )
        print(f"Acquisition Run: {run_id}")
        result = manager.run(run_id)
        print(json.dumps(result, indent=2))
        return 0 if result["status"] == "completed" else 1
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
    return download(args.catalog, items, args.outdir, not args.all, args.asset_keys)


if __name__ == "__main__":
    raise SystemExit(main())
