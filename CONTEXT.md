# Earth Lake Acquisition

This context covers discovering remote geospatial assets, downloading them reliably, and registering completed source assets in the Earth Lake protocol.

## Language

**Acquisition Run**:
A durable execution of one catalog query and its resulting downloads, including search checkpoints, batches, attempts, and terminal outcome.
_Avoid_: task, workflow job, background task

**Search Page**:
An immutable, committed page of catalog results with its incoming and outgoing cursors, query fingerprint, and discovered Items.
_Avoid_: result chunk, response page

**Download Batch**:
A durable, bounded group of asset transfers generated from committed Search Pages within one Acquisition Run.
_Avoid_: queue slice, download chunk

**Acquisition Manifest**:
The immutable request, Search Page snapshots, and deterministic download plan retained for one Acquisition Run under the protocol manifests layer.
_Avoid_: task dump, temporary results

**Protocol Commit**:
A journaled publication that stages related Registry, STAC, or manifest files and makes an interrupted publication recoverable as one logical protocol change.
_Avoid_: save operation, best-effort update

**Health Audit**:
A persistent quick or full inspection of protocol facts and physical files that emits categorized issues, severity, evidence, and a suggested repair action.
_Avoid_: dashboard warning, validation task

**Materialization Run**:
A durable, controllable execution that converts or adopts source hydrology data into protocol Entities and Array Stores.
_Avoid_: import task, conversion script

**Entity**:
A typed GeoParquet or Parquet table representing basins, stations, rivers, patches, or their attributes.
_Avoid_: arbitrary file, shapefile copy

**Array Store**:
A protocol-managed Zarr store for multidimensional hydrology observations, forcing, or static attributes.
_Avoid_: folder of chunks, converted file

**Preview Slice**:
A bounded, cacheable read of one Array Store variable for inspection; it is not an analytical export or training sample.
_Avoid_: full array, downloaded dataset

## Operating Constraints

- Acquisition Runs execute on one machine through one scheduler process.
- Mutable run state is persisted in local SQLite; Parquet registries remain the durable protocol facts.
- FastAPI, CLI, and Agent Skill entry points share the same Acquisition Run interface.
- On process startup, queued and interrupted Acquisition Runs resume automatically from their last committed checkpoint.
- A manually paused Acquisition Run remains paused across restarts and never resumes network transfer automatically.
- Pause and cancel are cooperative at transfer chunk checkpoints; both preserve resumable `.part` files, while cancel prevents all new paging and transfers.
- Authentication failures pause the Acquisition Run as `auth_required`; transient network, throttling, and server failures use bounded retry with backoff.
- Completed assets remain registered when later files or Search Pages fail; a mixed outcome is `partial`.
- Search and download execute as a bounded pipeline; a Search Page must be committed before any Download Batch may reference its Items.
- Download Batches are measured in asset files, not catalog Items; defaults are 20 files per batch, 3 concurrent transfers, and at most 2 buffered batches.
- Backpressure pauses catalog paging while the buffered Download Batch limit is reached.
- Mutable checkpoints live in `registry/acquisition_state.sqlite`; immutable request and Search Page snapshots live under `manifests/acquisitions/{run_id}/`.
- `cache/` remains fully rebuildable and never contains state required to resume an Acquisition Run.
- Every submission carries an explicit idempotency key: retries with the same key return the same Acquisition Run, while a new key permits an intentional rerun of identical query parameters.
- Items are unique within an Acquisition Run by catalog, Collection, and source Item ID; asset transfer attempts use a deterministic run-scoped identity.
- A Search Page file is atomically committed and checksummed before SQLite advances its outgoing cursor.
- Failed later pages do not invalidate completed assets; recovery resumes from the failed Search Page checkpoint.
- Distributed scheduling and multi-machine downloads are outside the current scope.
- Protocol Commit preparation writes to durable staging under `manifests/protocol_commits/.staging/`; a failed preparation is aborted without changing published protocol files.
- Publishing is journaled and recoverable by roll-forward after process restart; lock-free readers may briefly observe individual file replacement while recovery is pending.
- Health Audits never mutate data. Repair actions are recommendations until a dedicated repair command is explicitly run.
- Quick Health Audits compare structure, paths, sizes, schemas, STAC links, and materialization output stats; full audits additionally hash registered source assets and materialization outputs.
- Materialization Runs persist mutable execution state in `registry/materialization_state.sqlite` and publish their output manifest through a Protocol Commit.
- A materialization record separates `logical_asset_id` and source fingerprint from the physical output path; reuse requires the recorded content checksum and output stats to match, not merely path existence.
- Materialization pause and cancel are cooperative at source or copied-file checkpoints; source datasets remain read-only.
- Entity row previews are paged and omit binary geometry from tabular responses; map previews are capped at 500 features.
- Preview Slices cap cell counts and persist reusable results under `cache/zarr-slices/`; callers must use an export or training pipeline for complete arrays.
- `/health` and `/health/live` are read-only liveness probes; `/health/ready` checks only critical local protocol stores and does not contact external STAC services or perform business mutation.
