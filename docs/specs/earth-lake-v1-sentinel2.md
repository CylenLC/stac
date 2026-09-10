# Alibaba Cloud Earth Lake V1 Sentinel-2 Platform Specification

## Problem Statement

The current repository is a capable single-machine Earth Lake V0. It already protects canonical STAC identity, fail-closed asset resolution, transfer integrity, resumable Acquisition Runs, idempotency, Materialization Runs, Protocol Commit recovery, Health Audits, and explicit liveness/readiness boundaries.

Its production assumptions are still local filesystem storage, SQLite execution state, mutable Parquet registries, static STAC as the online catalog, and filesystem rename as the publication mechanism. Those assumptions do not provide a safe multi-user cloud platform:

- multiple ECS Workers need shared durable state and task leasing;
- OSS does not provide directory-level transactions;
- PostgreSQL must own metadata and publication state;
- large data must move directly between clients, TiTiler, Workers, and OSS rather than through FastAPI;
- online STAC search must be permission-aware and backed by a transactional index.

The repository therefore needs an architecture-level migration that preserves the existing correctness semantics while replacing the production storage and execution substrate. This is not a repository rewrite.

## Solution

Implement an incremental port-and-adapter migration from Earth Lake V0 to a private Alibaba Cloud V1. Preserve correct Domain and Application behavior, introduce explicit ports, and add cloud adapters without a Big Bang rewrite.

V1 is one vertical slice:

- Provider: Element 84 Earth Search v1.
- Collection: Sentinel-2 L2A.
- Dataset: sentinel2-l2a-dalian-51sud.
- AOI: WGS84 bbox [121.544, 38.867, 121.660, 38.957].
- Time window: 2025-01-01T00:00:00Z through 2025-12-31T23:59:59Z.
- Selection: intersects AOI, matching Collection and datetime, sorted by datetime ascending then Item ID ascending, taking exactly the first 64 Items.
- Required Assets: blue-jp2, green-jp2, and red-jp2 for every Item.
- Required Raw representations: 192.
- Required Canonical representations: 192 single-band COGs.

The frozen baseline consists of the upstream search response, 64 selected Item JSON documents, selected Item IDs, and a snapshot manifest hash. CI uses frozen metadata and immutable binary fixtures; only Nightly Canary accesses live Earth Search.

The cloud acceptance path is:

1. Frozen STAC and binary fixtures.
2. Immutable ACR image.
3. ECS Acquisition Worker and local scratch.
4. Private OSS create-only Multipart upload with SHA-256 and CRC64 evidence.
5. PostgreSQL/PostGIS Domain state.
6. Source-faithful COG processing.
7. PgSTAC search indexing.
8. One transactional Publication.
9. Auth-aware STAC Search.
10. Representation-ID-only TiTiler.
11. Stable content delivery.
12. Direct private OSS Range and Download.

V1 technical completion requires one real Alibaba Cloud acceptance run. Private Beta release additionally requires the complete correctness, security, recovery, migration, serving, and audit gates.

## User Stories

1. As an internal researcher, I want to discover the fixed Sentinel-2 Dataset through an Auth-aware STAC API, so that I can search geospatial data without accessing database internals.
2. As a researcher, I want STAC Search to return only Collections and Items from Projects I can access, so that search results, pagination, counts, and links cannot leak another Project.
3. As a researcher, I want every Published DatasetVersion to appear as an immutable STAC Collection, so that historical search results remain reproducible.
4. As a researcher, I want the stable Dataset resource to resolve to its latest Published Version, so that normal browsing is convenient without introducing a mutable latest Collection or ObjectKey.
5. As a researcher, I want to request an explicit historical Version, so that I can reproduce an analysis against a known snapshot.
6. As an administrator, I want to create a DatasetVersion from a frozen snapshot manifest, so that Version membership does not depend on a later upstream search.
7. As an administrator, I want the snapshot search response, selected Item IDs, Item JSON, and snapshot hash to be retained, so that the exact selection can be audited.
8. As the platform, I want a missing required Item or RGB Asset to fail the whole Version closed, so that a partial 63-Item result is never presented as the intended 64-Item DatasetVersion.
9. As the platform, I want every upstream Asset to have a stable AssetIdentity, so that URL changes, storage changes, and processing changes do not alter the logical identity of the scientific Asset.
10. As the platform, I want VersionAsset to capture the source href and metadata observed in a particular snapshot, so that historical upstream observations remain immutable.
11. As an administrator, I want to register External Assets before mirroring them, so that discovery is not coupled to immediately copying every upstream object.
12. As the Acquisition Worker, I want to mirror only the three required RGB JP2 Assets per Item in V1, so that the acceptance scope is bounded and measurable.
13. As the Acquisition Worker, I want downloads to pass through ECS local scratch, so that large objects are never held in memory and incomplete transfers never become available objects.
14. As the Acquisition Worker, I want streaming SHA-256, CRC64, byte counting, size limits, and scratch-space reservations, so that oversized or incomplete source transfers fail safely.
15. As the platform, I want Raw PhysicalObjects to use content-addressed immutable ObjectKeys, so that identical Raw bytes can be reused across Versions without conflating logical Assets.
16. As the platform, I want concurrent creation of the same ObjectKey to be safe, so that one creator succeeds, equivalent attempts reuse the object, and conflicting bytes are rejected without overwrite.
17. As the Acquisition Worker, I want Multipart upload sessions and completed parts to be durable, so that Worker crashes can resume or safely abort without relying on scratch persistence.
18. As the platform, I want Raw PhysicalObjects to become AVAILABLE after complete upload and transfer-integrity verification, while Raw Representation validation remains a separate step, so that shared bytes are not confused with Version-specific scientific evidence.
19. As the Processing Worker, I want Canonicalization to read a verified Raw object, materializing it to scratch when necessary, so that processing never depends on a previous Worker leaving a file behind.
20. As a researcher, I want the V1 COG to preserve the source JP2 dtype, DN values, NoData, dimensions, CRS, and transform, so that Canonicalization changes physical encoding without silently changing scientific content.
21. As a researcher, I want scale and offset retained as metadata rather than applied to pixel values, so that DN-to-reflectance conversion remains an explicit future Derived Representation.
22. As the platform, I want full blockwise base-resolution pixel equality between Raw JP2 and Canonical COG, so that the source-faithful invariant is proven rather than inferred from file structure.
23. As the platform, I want ProcessingSpec and ValidationSpec to be separately versioned and hashed, so that output-generation changes are distinguished from validation-rule changes.
24. As the platform, I want the exact processor image digest and deterministic runtime settings recorded, so that the same input and processing envelope produce the same Canonical SHA-256.
25. As a researcher, I want each spectral band to remain a separate STAC Asset, so that blue, green, and red are not confused with a derived three-band visualization.
26. As a researcher, I want COG overviews to optimize access without changing base-resolution scientific pixels, so that map rendering and scientific content have separate guarantees.
27. As an administrator, I want each Processing Run to expose durable Run and Task state, so that progress, retry, cancellation, lease loss, and permanent failure are inspectable.
28. As the platform, I want at-least-once Task execution with idempotent handlers, so that Worker crashes can be recovered without creating duplicate logical Representations or Publications.
29. As the platform, I want retryable transient errors separated from permanent input, integrity, policy, and cancellation errors, so that invalid data is not pointlessly retried.
30. As a researcher, I want to cancel my own Run cooperatively, so that new work stops at safe checkpoints without pretending that already written immutable Objects can be rolled back.
31. As an administrator, I want a Publication barrier to prove the complete 64-Item and 192-asset Version before commit, so that incomplete processing cannot become externally visible.
32. As the platform, I want Domain state, PgSTAC Collection/Items whose assets maps contain the canonical STAC Assets, Representation visibility, Publication, and the latest pointer committed in one PostgreSQL transaction, so that no Published Version is missing from search or pointing at incomplete data.
33. As the platform, I want latest to advance only by immutable snapshot_sequence, so that an older Version completing late cannot downgrade the Dataset's latest pointer.
34. As a researcher, I want stable STAC Asset hrefs that do not contain expired credentials or physical ObjectKeys, so that catalog documents remain immutable and usable over time.
35. As an authorized client, I want HEAD on content to return metadata directly and GET/Range GET to redirect with 307 to a short-lived Presigned URL, so that clients can inspect and download large objects without FastAPI proxying bytes.
36. As an authorized user, I want download authorization to be audited separately from actual OSS access, so that the platform does not claim a download completed merely because a URL was issued.
37. As a map client, I want Tile, Preview, TileJSON, and Statistics endpoints addressed only by representation_id, so that no user-controlled URL can trigger arbitrary remote reads.
38. As the platform, I want Tile access to resolve only Published Canonical PhysicalObjects in the configured private namespace, so that Raw, External, Quarantined, Deleted, and Draft data cannot be served.
39. As a viewer, I want unauthorized private data to return 404 through Data, STAC, Tile, Content, and Download entry points, so that resource existence cannot be enumerated.
40. As a researcher, I want to see my own Project's Run status but not internal Version state through Data APIs, so that control-plane information is exposed according to role.
41. As an administrator, I want to see all Run, Task, IntegrityEvidence, Publication, and Audit states through Admin APIs, so that operational recovery does not require database access.
42. As an administrator, I want orphan reconciliation to distinguish referenced, published, grace-period, GC-eligible, missing, and unknown Objects, so that cleanup is evidence-based and recoverable.
43. As an administrator, I want published Object deletion to be impossible through normal Worker credentials, so that a processing failure cannot destroy published data.
44. As a maintainer, I want LocalObjectStore, local execution, and compatibility backends retained for development and tests, so that cloud migration does not remove fast deterministic feedback.
45. As a maintainer, I want V0 data migrated through inventory, identity extraction, Object reconciliation, state reconciliation, backup/restore, and audit, so that the switch to PostgreSQL has a machine-checkable gate.
46. As a maintainer, I want forward-only expand/contract migrations and an N/N-1 compatibility window, so that application rollback does not require unsafe schema rollback.
47. As an operator, I want real Cloud Acceptance to use ECS, ACR, RDS, PostGIS, PgSTAC, private OSS, RAM Roles, and the internal endpoint, so that Local or mock success cannot be mistaken for production readiness.
48. As a platform owner, I want benchmark results to include the cloud profile, image digest, corpus hash, concurrency, and cold/warm definition, so that performance regressions are comparable.

## Implementation Decisions

### Architecture and seams

Use the highest useful application seam: a versioned Sentinel2V1Workflow that coordinates snapshot ingestion, AssetIdentity and VersionAsset creation, mirroring, integrity, canonicalization, readiness, and Publication.

Preserve existing identity, resolver, integrity, Acquisition Run, Protocol Commit, Health Audit, and Materialization Run boundaries where their semantics remain correct. The current local implementation is a V0 adapter and compatibility backend, not disposable prototype code.

Introduce ports for ObjectStore, MetadataRepository, Catalog, Task/Job Store, Execution Backend, and Credentials. Keep Domain and Application free of OSS SDK, database ORM, FastAPI, TiTiler, GDAL implementation, and cloud-specific types.

Use LocalObjectStore and in-memory/fake repositories for the M1 tracer bullet, then move quickly to real PostgreSQL in M2. Do not build S3, MinIO, Dask, Kafka, or a generic workflow engine merely to demonstrate abstraction.

### Domain model

Use AssetIdentity for global upstream identity: provider, collection, item_id, and asset_key. Use VersionAsset for a DatasetVersion's immutable snapshot binding, including source href and source metadata snapshot. Use Representation for Raw, Canonical, and future Derived forms. Use PhysicalObject as the first-class record of actual immutable bytes.

Multiple Representations may reference one PhysicalObject. Raw PhysicalObjects are content-addressed and reusable across Versions. Canonical ObjectKeys are Version and ProcessingSpec scoped. Physical object identity is never substituted for logical Asset identity.

PhysicalObject and Representation states are intentionally separate:

- PhysicalObject AVAILABLE means immutable bytes exist completely and the object-store transfer proof is complete: size, local SHA-256, object-store checksum evidence, and successful object inspection.
- Representation RAW_VALIDATED or CANONICAL_VALIDATED means the bytes also passed the relevant JP2/COG structure, geospatial semantics, lineage, and pixel-equality checks.
- A shared PhysicalObject may remain AVAILABLE even when one VersionAsset has a metadata or semantic mismatch. That VersionAsset or Representation is failed or quarantined; the shared PhysicalObject is quarantined only when an audit proves the bytes themselves are corrupt or unsafe for every reference.
- DatasetVersion PUBLISHED is a third, higher-level state. A validated Representation is not externally visible until the complete Version publication barrier and transaction succeed.

Use immutable StateTransition history for Version and other stateful entities. Application policy defines valid transitions; PostgreSQL serializes concurrent updates and enforces valid enum values, optimistic state_version checks, and append-only history.

### V1 source and representation

Use the frozen Earth Search v1 Sentinel-2 L2A snapshot. The required set is exactly 64 Items and 192 required RGB Assets. A missing or invalid required Asset fails the entire DatasetVersion; no automatic replacement or reselection is permitted.

Canonical output is three single-band source-faithful COGs per Item: B02 as blue, B03 as green, and B04 as red. Preserve source DN, dtype, NoData, dimensions, CRS, transform, scale, and offset metadata. Do not apply radiometric conversion, reprojection, or base-resolution resampling.

Use a versioned Sentinel-2 spectral ProcessingSpec with lossless DEFLATE, deterministic settings, pinned GDAL toolchain, 512 block size, and AVERAGE overviews. Overview semantics are access optimization and must respect NoData. Categorical or mask data requires a different future ProcessingSpec.

Use source-band metadata from the frozen upstream STAC snapshot. Do not hardcode wavelength or radiometric facts in application code. Pin the applicable STAC EO and Raster extension versions.

### Storage and upload

ObjectStorePort exposes ObjectKey, ObjectRef, ObjectHead, ObjectReceipt, ChecksumEvidence, put_file, put_stream, head, get, get_range, exists, and namespace listing. It does not expose Bucket, UploadId, ETag, PartNumber, or endpoint concepts.

Raw acquisition must flow through ECS scratch, streaming SHA-256 and CRC64, transfer completion checks, create-only OSS upload, server-side forbid-overwrite, receipt verification, and HEAD reconciliation. JP2 structural validation is recorded against the Raw Representation after the PhysicalObject is AVAILABLE; it is not a prerequisite for the shared object to become AVAILABLE. No transparent upstream-to-OSS pipe is allowed.

Persist Multipart session and part checkpoint records in PostgreSQL. Database records are the recovery source; OSS ListParts is reconciliation evidence. Resume when the scratch fingerprint and recorded/provider parts agree; otherwise abort and restart safely.

The authoritative V1 data bucket is private, uses application-level immutable ObjectKeys, has Versioning off for the create-only invariant, and grants delete only to controlled reconciliation/GC operations.

Raw Object metadata is limited to object schema, object ID, object kind, content hash, size, content type, creator build ID, and creation time. VersionAsset identity, lineage, publication state, and validation history remain PostgreSQL facts.

### Integrity and validation

IntegrityGate is divided into Metadata, Transfer, Raw Structural, Canonical Structural, Geospatial Consistency, Identity/Mapping, and Version Completeness checks.

Every Representation receives versioned IntegrityEvidence. Evidence records validator name and version, validator role, container image digest, ValidationSpec hash, source and output hashes, result, evidence payload, and validation time.

Primary COG validation uses a pinned GDAL full layout validator. A fixed secondary validator may be used as an independent check. COG structural validity does not replace scientific pixel equality or lineage validation.

Raw-to-COG validation compares every base-resolution pixel in bounded windows, exact dtype and NoData semantics, exact shape and band count, semantic CRS equality, and fixed-tolerance transform equality. Overviews are validated structurally and are not included in scientific base-pixel equality.

### Task and execution

Use PostgreSQL leasing with row locking and skip-locked task claiming. Tasks have durable idempotency keys, lease owner/until, heartbeat, attempt count, timeout, cancellation request, and structured error classification.

Use at-least-once delivery and idempotent handlers. The fixed V1 workflow includes snapshot metadata, per-Asset mirror, raw validation, per-Asset canonicalization, canonical validation, publication preparation, and one PublishVersion task.

Model dependencies explicitly but do not expose arbitrary DAG authoring. Block downstream tasks when an upstream task has permanent failure, with an auditable blocked reason.

Distinguish transient/retryable errors, lease loss, permanent input errors, permanent integrity errors, permanent policy errors, internal bugs, and cancellation. Lease duration, heartbeat interval, and execution timeout are separate controls. INTERNAL_BUG does not automatically retry like a transient network failure: the default is one failed attempt, FAILED_INTERNAL state, and alerting. A later retry is allowed only after explicit reclassification as transient or after a corrected build/spec is deployed.

### Publication and catalog

Use RDS PostgreSQL/PostGIS with an earthlake schema for Domain, state, lineage, evidence, permissions, and Publication. Use a separate pgstac schema as the STAC search index behind CatalogPort.

PgSTAC writes must use the same PostgreSQL connection and transaction context as Domain Publication. Do not publish through a separate STAC API HTTP request.

The final Publication transaction locks the Dataset and DatasetVersion, verifies current barrier evidence, updates Representation visibility, writes one PgSTAC Collection and its 64 Items with 192 canonical STAC Assets in the Item assets maps, marks the Version Published, conditionally advances latest by snapshot_sequence, and writes exactly one Publication record. The specification does not require or assume an independent PgSTAC Asset table.

Only Published Collections and Items enter Auth-aware STAC queries. Permission-filtered Collection IDs must be applied before PgSTAC query planning so counts, limits, pagination, and next links cannot leak unauthorized resources.

### API, authorization, and serving

Separate Data API, Research Control API, and Admin API. Data APIs expose Published Version and Representation data only; a Dataset shell may exist without a Published Version. Research Control exposes authorized Run state. Admin exposes full operational state without granting DDL.

Use OIDC for user authentication and OAuth2/OIDC flows to obtain access tokens. The API validates the access token/JWT, maps issuer plus subject to an Earth Lake User, and combines ProjectMembership with resource scope for authorization. Roles are admin, researcher, and viewer. Workers cannot change Published state, latest, or PgSTAC visibility; Publication uses a dedicated runtime capability.

Use stable Representation content hrefs. HEAD authorizes and returns Earth Lake metadata without redirect. GET and Range GET authorize, audit URL issuance, and return 307 to a short-lived Presigned GET URL. Use private, no-store responses in V1.

Expose TiTiler functionality only through Representation-ID routes. Reject user-controlled URL, bucket, key, endpoint, path, source href, external redirect, Raw, and non-Canonical inputs. Resolve the configured canonical bucket/prefix and use a read-only TiTiler RAM Role.

Use RFC 9457-style problem details with stable machine-readable code, retryable flag, request ID, and resource ID. Use opaque keyset cursors instead of deep offset pagination.

### Migration, deployment, and operations

Retain V0 Local and SQLite compatibility backends for development, rollback artifacts, and contract tests, but freeze V0 writes before production cutover. After the first V1 production write, SQLite and mutable Parquet are read-only archives and cannot be used as a production rollback path.

Use one Region, one ECS instance with separate API, Acquisition Worker, Processing Worker, and Tiler containers, private OSS, RDS HA PostgreSQL, ACR immutable images, RAM Roles, and OSS internal endpoint. Do not introduce ACK until benchmark evidence requires it.

Use forward-only expand/contract database evolution. Application rollback may use an older compatible image; PostgreSQL restore/PITR is disaster recovery, not normal deployment rollback.

The candidate staging profile is same-Region Alibaba infrastructure, currently proposed as cn-beijing, Linux amd64, approximately 16 vCPU/64 GiB ECS with rebuildable scratch, RDS HA PostgreSQL 16 with PostGIS and btree_gist candidates, private Versioning-off OSS, ACR immutable images, and internal OSS endpoint. Exact purchasable SKUs and versions remain POC/deployment parameters and must be recorded in benchmark manifests.

## Testing Decisions

Tests assert externally observable behavior and Domain invariants, not ORM details, SQL statement order, cloud SDK calls, or private helper structure. The primary seam is Sentinel2V1Workflow; adapter contract suites and infrastructure POCs cover lower seams only where behavior cannot be proven above.

Preserve and extend existing prior art for canonical identity, fail-closed asset resolution, transfer integrity, resumable Acquisition Runs, idempotency, Protocol Commit recovery, Health Audits, Materialization Runs, and FastAPI behavior.

Required test layers:

- Unit tests for identity normalization, Version state policy, ObjectKey derivation, ProcessingSpec and ValidationSpec hashing, error classification, publication barriers, authorization policy, and RFC 9457 error mapping.
- Property-based tests for identity serialization, path/key safety, idempotency normalization, state transitions, manifest round trips, cursor behavior, and namespace escape prevention.
- ObjectStore contract tests for LocalObjectStore and OssObjectStore behavior: immutable repeated writes, conflicting writes, full and Range reads, checksums, missing objects, list semantics, and error mapping.
- Fault semantics tests using an injected failure adapter for timeouts, resets, short reads, checksum mismatch, upload-session loss, duplicate delivery, and stale leases.
- M1 Local Tracer Bullet with 2 Items and 6 Assets using real GDAL, full IntegrityGate, Publication, query, stable local content abstraction, and failure recovery. M1 does not validate the real 307-to-OSS, Presigned URL, or Range-to-private-OSS path.
- M2 PostgreSQL Tracer Bullet covering schema constraints, leasing, dependencies, idempotency, state transitions, and transaction rollback.
- POC-1 PgSTAC/RDS tests covering clean install, migration, roles, 1M deterministic Items, bbox, datetime, combined filters, CQL2, backup/restore, reconnect, and same-transaction Publication.
- POC-2 OSS tests covering concurrent same-key creation, forbid-overwrite, Multipart crash/resume, ListParts reconciliation, CRC64, stale abort, and orphan safety.
- POC-3 COG tests covering repeated output SHA reproducibility under the pinned image, full blockwise Raw-to-COG pixel equality, dtype/NoData/CRS/transform semantics, and primary/secondary COG validation.
- POC-4 serving tests covering OIDC authorization, Project filtering before PgSTAC search, stable href behavior, HEAD, GET 307, Range 206, private OSS, TiTiler routing, SSRF denial, and IDOR denial.
- POC-5 migration tests covering inventory, identity collisions, object references, state mapping, checksum/size reconciliation, backup/restore, and point-of-no-return behavior.
- Full 64-Item Release E2E verifying 64 Items, 192 VersionAssets, all required Representations, evidence completeness, one Publication, one PgSTAC Collection with 64 Items and 192 canonical STAC Assets in Item assets maps, correct latest pointer, download hash, tile samples, and audit reconciliation.
- Failure E2E for missing required Asset, permanent integrity failure, Worker crash, lease expiry, OSS timeout, Multipart loss, PgSTAC write failure during Publication, and latest-pointer race.
- Authorization matrix E2E across admin, researcher in Project A, viewer in Project B, and anonymous clients for Data, Run, STAC, Content, Tile, Preview, TileJSON, and Download entry points.
- Benchmark suites separated into Functional E2E, 1M-Item Catalog Benchmark, Worker/Object Benchmark, and Serving Benchmark. Every result records corpus hash, benchmark spec hash, commit, image digest, ECS/RDS profile, concurrency, and cold/warm conditions.

Functional E2E uses frozen metadata and immutable binary fixtures. PR tests use a tiny deterministic subset; Main and Release use the complete 64-Item fixture. CI never falls back to live Earth Search when fixtures are missing. Live Earth Search is Nightly Canary only and never creates a formal DatasetVersion.

Release correctness, security, Publication atomicity, migration integrity, and capacity safety are hard gates. Latency and throughput SLO misses may be waived only with an owner, risk record, supported-capacity limit, mitigation, and expiry date.

## Out of Scope

- CAMELS, ERA5, DEM, Zarr, GeoParquet, hydrology-specific canonicalization, and other non-Sentinel vertical slices.
- Dask distributed execution, ACK/Kubernetes, Redis, Kafka, RocketMQ, Celery, and generic workflow/DAG platforms.
- GPU, PyTorch, model training, training cache, and high-performance training infrastructure.
- CDN, shared Tile Cache, public data access, and cross-user HTTP caching.
- S3 or MinIO production adapters and multi-cloud runtime support.
- Full multi-tenancy, tenant isolation, quota, billing, complex organization hierarchy, and marketplace workflows.
- Cross-Region active-active HA, multi-Region replication, automatic horizontal autoscaling, and real-time streaming ingestion.
- Full Sentinel-2 archive mirroring, arbitrary user-defined processing, and automatic Asset substitution.
- Destructive database rollback, SQLite/PostgreSQL production dual-write, mutable latest ObjectKeys, and FastAPI byte proxying.
- Destructive or automatic renaming of Earth Zarr Protocol. The new Representation model is introduced first; any protocol renaming is a later compatibility-managed change.

## Further Notes

### Implementation milestones

1. M0: freeze scope, fixture and manifest schemas, ProcessingSpec and ValidationSpec schemas, candidate Sentinel-2 policy, API errors, RBAC, and benchmark manifest format. Do not freeze the POC-3-dependent GDAL image digest, exact toolchain parameters, or Golden SHA outputs here.
2. M1: Local 2-Item Tracer Bullet with 6 Assets, LocalObjectStore, fake/in-memory repositories, real GDAL, full IntegrityGate, Publication, query, stable local content abstraction, and failure recovery. Do not perform V0 directory migration or implement OSS, PgSTAC, OIDC, TiTiler, or Cloud Acceptance in M1.
3. M2: PostgreSQL Tracer Bullet with the real schema, leasing, dependencies, idempotency, state transitions, and rollback behavior.
4. M3: OSS Adapter with private storage, create-only writes, Multipart recovery, SHA-256, CRC64, RAM Role, and internal endpoint.
5. M4: Canonical Processing with pinned GDAL image, ProcessingSpec, ValidationSpec, pixel equality, COG validation, and Golden Output hashes.
6. M5: PgSTAC Catalog Adapter and same-transaction Publication. Write one Collection and Items whose assets maps contain the canonical STAC Assets; do not assume a standalone PgSTAC Asset table.
7. M6: Data API, Research Control API, Admin API, OIDC/RBAC, Auth-aware STAC, Representation Tiler, content redirect, and download authorization.
8. M7: V0 migration rehearsal, inventory, reconciliation, backup/restore, and point-of-no-return validation.
9. M8: real Alibaba Cloud 64-Item Cloud Acceptance.
10. M9: Private Beta Release after the complete Release Gate.

### POC gates

- POC-1: exact RDS/PostGIS/PgSTAC compatibility, installation, migration, roles, 1M ingest, spatial/temporal/CQL2 search, backup/restore, reconnect, and same-transaction Publication.
- POC-2: OSS prevent-overwrite, concurrent same-key writes, Multipart resume, ListParts reconciliation, CRC64, stale session abort, and orphan safety.
- POC-3: deterministic GDAL output, repeated Canonical SHA equality, full pixel equality, and source metadata preservation.
- POC-4: private OSS, RAM Role, Representation-ID resolver, Tile/Preview, Content HEAD/GET/Range, SSRF denial, and cross-Project authorization.
- POC-5: real V0 inventory, identity extraction, object/state reconciliation, migration report, backup/restore, audit, and dry-run cutover.

A POC may change an Adapter, dependency version, cloud SKU, or deployment parameter. It may not silently weaken Identity, Integrity, Immutability, Publication Atomicity, or Authorization invariants. If a Domain invariant is disproven, the affected ADR must be reopened before implementation continues.

### Frozen, POC Required, and Deferred decisions

Frozen:

- Sentinel-2 L2A Earth Search source and 64-Item snapshot.
- 192 RGB JP2 Raw assets and 192 source-faithful single-band COGs.
- AssetIdentity, VersionAsset, Representation, and PhysicalObject model.
- OSS as byte truth and PostgreSQL as Domain/state truth.
- PgSTAC behind CatalogPort.
- Private OSS and create-only immutable storage.
- At-least-once Tasks with leasing and idempotency.
- Single PostgreSQL Publication transaction.
- Monotonic latest by snapshot_sequence.
- Stable content href and Representation-ID-only Tile API.
- OIDC/RBAC with admin, researcher, and viewer roles.
- No production dual-write and no user-controlled URL.

POC Required:

- Exact RDS, PostGIS, btree_gist, and PgSTAC version tuple.
- OSS Multipart and CRC64 behavior.
- GDAL byte reproducibility.
- Private TiTiler integration.
- Real client HEAD, GET, and Range behavior.
- V0 migration and reconciliation.
- Real Cloud Acceptance.

Deferred:

- CAMELS, Zarr, GeoParquet, DEM, ERA5, Dask, ACK, Redis, Kafka, RocketMQ, Celery, CDN, shared Tile cache, multi-cloud adapters, full multi-tenancy, quota, billing, GPU, training, cross-Region HA, generic DAG authoring, and real-time ingestion.

### Definition of Done

The V1 technical DoD requires:

- exactly 64 frozen Items;
- exactly 192 required VersionAssets;
- 192 valid Raw Representations;
- 192 valid Canonical Representations;
- complete SHA-256, CRC64, structural, semantic, lineage, and Version evidence;
- full base-resolution pixel equality;
- Golden Canonical SHA reproducibility;
- DatasetVersion Published;
- exactly one Publication;
- one PgSTAC Collection with 64 Items;
- correct monotonic latest pointer;
- zero Published references to missing objects;
- successful HEAD, GET redirect, Range 206, Presigned Download, Tile, Preview, and TileJSON behavior;
- Worker crash, lease recovery, Multipart recovery, orphan audit, and backup/restore passing;
- permission matrix, IDOR, and Tile SSRF tests passing;
- one real Alibaba Cloud acceptance run using ECS, ACR, RDS/PostGIS, PgSTAC, private OSS, RAM Roles, and the internal endpoint.

Correctness, security, Publication atomicity, migration integrity, and capacity safety are non-waivable. Only latency and throughput may receive a time-bounded, explicitly approved waiver.

### ADR backlog

The implementation should record the following ADRs:

ADR-001 V0 architecture refactor rather than repository rewrite.

ADR-002 Domain, Application, Ports, and Adapters boundaries.

ADR-003 AssetIdentity, VersionAsset, Representation, and PhysicalObject.

ADR-004 Immutable Objects and create-only storage.

ADR-005 ObjectStorePort and Multipart recovery.

ADR-006 IntegrityGate, IntegrityEvidence, ProcessingSpec, and ValidationSpec.

ADR-007 DatasetVersion as the atomic Publication boundary.

ADR-008 PostgreSQL Domain truth and PgSTAC search index.

ADR-009 At-least-once Tasks, leasing, and idempotent handlers.

ADR-010 Auth-aware STAC and Representation-only Tiler.

ADR-011 Stable content href and private OSS delivery.

ADR-012 V0 to V1 migration and point-of-no-return.

ADR-013 Sentinel-2 source-faithful COG semantics.

ADR-014 Test pyramid, benchmark baselines, and Release Gates.

ADR-015 OIDC, RBAC, and Project authorization.

ADR-016 Forward-only expand/contract database evolution.

ADR-017 Alibaba Cloud staging and benchmark profile.

Each ADR must include status, context, decision, invariants, consequences, rejected alternatives, validation/POC, and rollback implications.
