# STAC Downloader Skill

This skill allows searching and downloading geospatial data from STAC (SpatioTemporal Asset Catalog) APIs like Microsoft Planetary Computer, Earth Search (AWS), and NASA CMR.

## Capabilities

- **Search**: Find data items based on a WKT geometry, date range, and collection names.
- **Download**: Download specific items or all search results locally.

## Usage

### Searching for Data

To search for data, the agent can use the `scripts/search.py` script.
Parameters:
- `--wkt`: The geometry in WKT format.
- `--collections`: Comma-separated list of collections (e.g., `sentinel-2-l2a,cop-dem-glo-30`).
- `--start`: Start date (YYYY-MM-DD).
- `--end`: End date (YYYY-MM-DD).
- `--catalog`: `microsoft`, `earth-search`, or `nasa`.

### Downloading Data

To download data, the agent can use the `scripts/download.py` script.
It accepts a JSON file containing the items to download (output from search) or specific parameters.

## Directory Structure

- `scripts/search.py`: The search logic.
- `scripts/download.py`: The download logic.
- `downloads/`: Default directory for downloaded data.
