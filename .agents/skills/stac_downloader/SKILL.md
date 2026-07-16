# STAC Downloader Skill

Use this skill when the user asks to search for or download geospatial data
from Microsoft Planetary Computer, AWS Earth Search, or NASA CMR.

## Capabilities

- Search for items using a WKT area of interest, one or more collections, and
  an optional date range.
- Download search results into `downloads/<collection>/<item_id>/`.
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
2. Run `search` and save the results to a JSON file.
3. Run `download` using the saved JSON file.
4. Report the downloaded paths and any failures shown during execution.

Ask for missing information only when it cannot be reasonably inferred. A
download request needs at least a collection and a geographic area.

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
  <collection>/
    <item_id>/
      <asset files>
```
