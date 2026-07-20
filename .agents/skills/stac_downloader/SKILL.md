# STAC Downloader Skill

Use this skill when the user asks to search for or download geospatial data
from Microsoft Planetary Computer, AWS Earth Search, or NASA CMR.

## Capabilities

- Search for items using a WKT area of interest, one or more collections, and
  an optional date range.
- Download search results into an Earth Zarr Protocol lake under `downloads/`.
- Maintain STAC, Parquet registries, checksums, and processing lineage automatically.
- Sign Microsoft Planetary Computer asset URLs automatically.
- Download NASA CMR data when local Earthdata authentication is configured.

## Tool Entry Point

All operations are provided by one script:

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py <command> [options]
```

Run commands from the repository root.

## Agent Workflow

When the user requests a download, execute the workflow rather than only
describing commands:

1. Collect or infer the required catalog, collection ID, WKT geometry, date
   range, result limit, and whether to download main assets only or all assets.
2. Choose a stable, explicit idempotency key for the user's submission.
3. Run `acquire`; it persists catalog pages, plans bounded asset batches, and downloads them.
4. Report the Acquisition Run ID, terminal status, downloaded paths, and failures.

Ask for missing information only when it cannot be reasonably inferred. A
download request needs at least a collection and a geographic area.

## Durable Acquire (Default)

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py acquire \
  --catalog nasa \
  --wkt "POLYGON ((-125 24,-66 24,-66 49,-125 49,-125 24))" \
  --collections "HLSL30_V2.0,HLSS30_V2.0" \
  --start "2024-01-01" \
  --end "2024-01-03" \
  --max 10 \
  --outdir downloads \
  --idempotency-key hls-us-sample-2024-01-01
```

Omit `--max` to follow catalog pages until exhausted. Add `--all` to download
all data assets. Interrupted transfers retain `.part` files and continue with
HTTP Range requests when the server supports them. Mutable checkpoints are in
`registry/acquisition_state.sqlite`; immutable request and Search Page snapshots
are in `manifests/acquisitions/<run_id>/`.

Use a new idempotency key for an intentional rerun. Reusing a key with the same
request returns the existing run; reusing it with different parameters is an error.

The separate `search` and `download` commands below remain available for manual,
non-paged JSON workflows, but Agent downloads should use `acquire`.

## Search

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py search \
  --catalog microsoft \
  --wkt "POLYGON ((124.4 42.1, 124.5 42.1, 124.5 42.2, 124.4 42.2, 124.4 42.1))" \
  --collections "sentinel-2-l2a" \
  --start "2023-12-01" \
  --end "2023-12-05" \
  --max 10 \
  --output results.json
```

Search arguments:

| Argument | Description |
| --- | --- |
| `--catalog` | `microsoft`, `earth-search`, or `nasa`; defaults to `microsoft`. |
| `--wkt` | Required WKT geometry for the area of interest. |
| `--collections` | Required comma-separated collection IDs or NASA short names. |
| `--start` | Required start date in `YYYY-MM-DD`. |
| `--end` | Required end date in `YYYY-MM-DD`. |
| `--max` | Maximum item count; defaults to `50`. |
| `--output` | Optional JSON path for results; use it when downloading afterward. |

## Download

Download main assets only:

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py download \
  --input results.json \
  --catalog microsoft \
  --outdir downloads
```

Download every asset included in the result items:

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py download \
  --input results.json \
  --catalog microsoft \
  --outdir downloads \
  --all
```

Download arguments:

| Argument | Description |
| --- | --- |
| `--input` | Required JSON file created by the `search` command. |
| `--catalog` | `microsoft`, `earth-search`, or `nasa`; must match the search source. |
| `--outdir` | Output directory; defaults to `downloads`. |
| `--all` | Download all assets. Without it, only main assets are downloaded. |

## NASA CMR Downloads

NASA searches use the CMR collection short name. For HLS v2.0, this skill also
accepts the product-style aliases `HLSL30_V2.0` and `HLSS30_V2.0`, and maps
them to the CMR short names `HLSL30` and `HLSS30` with version `2.0`.

```bash
uv run python .agents/skills/stac_downloader/scripts/stac_tool.py search \
  --catalog nasa \
  --wkt "POLYGON ((124.4 42.1, 124.5 42.1, 124.5 42.2, 124.4 42.2, 124.4 42.1))" \
  --collections "HLSL30_V2.0,HLSS30_V2.0" \
  --start "2023-12-01" \
  --end "2023-12-05" \
  --max 5 \
  --output nasa_results.json
```

Before downloading protected NASA files, ensure Earthdata credentials are
configured in `~/.netrc`:

```text
machine urs.earthdata.nasa.gov
    login YOUR_USERNAME
    password YOUR_PASSWORD
```

## Output Layout

Downloaded files are stored under:

```text
downloads/
  protocol/
  catalog/stac/
  registry/
  source/
    <catalog>/
      <collection>/
        <item_id>/
          <asset files>
  arrays/
  entities/
  virtual/
  manifests/
  cache/
```

The download command initializes the protocol tree when needed. Every successful
or skipped source asset is registered in `registry/assets.parquet`, linked to a
row in `registry/processing_runs.parquet`, and exposed as a STAC Asset. Source
files are registered only; downloading does not materialize Zarr arrays.
