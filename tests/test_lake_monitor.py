import tempfile
import unittest
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from earth_lake import EarthLake
from lake_footprint import valid_data_footprint
from lake_preview import render_preview
from lake_monitor import LakeMonitor


class LakeMonitorTests(unittest.TestCase):
    def test_summary_and_catalog_views_use_registered_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"catalog": "nasa", "interface": "test"})
            asset_path = lake.source_item_directory("nasa", "HLSL30_V2.0", "granule-1") / "scene.B04.tif"
            asset_path.parent.mkdir(parents=True)
            asset_path.write_bytes(b"sample-raster")
            item = {
                "id": "granule-1",
                "collection": "HLSL30_V2.0",
                "bbox": [-101.0, 39.0, -99.0, 41.0],
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-101.0, 39.0],
                        [-99.0, 39.0],
                        [-99.0, 41.0],
                        [-101.0, 41.0],
                        [-101.0, 39.0],
                    ]],
                },
                "properties": {"datetime": "2024-01-01T12:00:00Z"},
            }
            asset_id = lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=asset_path,
                status="downloaded",
            )
            lake.finish_run(run_id, "completed", [asset_id])

            monitor = LakeMonitor(directory)
            summary = monitor.summary()
            self.assertEqual(summary["registry_counts"]["assets"], 1)
            self.assertEqual(summary["available_assets"], 1)
            self.assertEqual(summary["missing_assets"], 0)
            self.assertEqual(summary["unregistered_source_files"], 0)

            products = monitor.products()
            self.assertEqual(products[0]["asset_count"], 1)
            self.assertEqual(products[0]["variable_count"], 1)
            self.assertEqual(products[0]["bboxes"], [[-101.0, 39.0, -99.0, 41.0]])
            self.assertEqual(monitor.product("hlsl30_v2.0")["variables"][0]["source_name"], "B04")
            self.assertIsNone(monitor.product("missing"))

            asset = monitor.asset(asset_id)
            self.assertEqual(asset["bbox"], [-101.0, 39.0, -99.0, 41.0])
            self.assertEqual(asset["local_path"], "source/nasa/HLSL30_V2.0/granule-1/scene.B04.tif")
            spatial = monitor.spatial_assets()
            self.assertEqual(spatial["type"], "FeatureCollection")
            self.assertEqual(spatial["features"][0]["properties"]["asset_id"], asset_id)
            self.assertEqual(spatial["features"][0]["geometry"]["type"], "Polygon")
            self.assertIsNone(spatial["features"][0]["properties"]["preview_coordinates"])
            self.assertIsNone(spatial["features"][0]["properties"]["preview_cache_key"])

    def test_registry_pagination_and_query(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            monitor = LakeMonitor(directory)
            rows = monitor.registry_rows("sources", query="nasa", offset=0, limit=10)
            self.assertEqual(rows["total"], 0)
            self.assertEqual(rows["columns"][0], "source_id")
            with self.assertRaises(KeyError):
                monitor.registry_rows("not-a-table")

    def test_resources_do_not_escape_lake_root(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            (Path(directory) / "entities" / "stations" / "stations.parquet").write_bytes(b"table")
            resources = LakeMonitor(directory).resources("entities")
            paths = {item["path"] for item in resources["items"]}
            self.assertIn("entities/stations/stations.parquet", paths)
            self.assertTrue(all(not path.startswith("/") for path in paths))

    def test_geotiff_preview_is_cached_by_source_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLSL30_V2.0", "granule-preview") / "scene.B04.tif"
            path.parent.mkdir(parents=True)
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=4,
                height=3,
                count=1,
                dtype="uint16",
                crs="EPSG:4326",
                transform=from_origin(-101, 41, 0.01, 0.01),
                nodata=0,
            ) as dataset:
                dataset.write(np.array([[0, 100, 200, 300], [400, 500, 600, 700], [800, 900, 1000, 1100]], dtype="uint16"), 1)
            asset_id = lake.record_asset(
                run_id=lake.start_run({"interface": "test"}),
                catalog="nasa",
                item={"id": "granule-preview", "collection": "HLSL30_V2.0", "bbox": [-101, 40.97, -100.96, 41], "geometry": None, "properties": {}},
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=path,
                status="downloaded",
            )
            asset = LakeMonitor(directory).asset(asset_id)
            first = render_preview(directory, asset, max_size=256)
            second = render_preview(directory, asset, max_size=256)
            first_footprint = valid_data_footprint(directory, asset)
            second_footprint = valid_data_footprint(directory, asset)
            self.assertFalse(first.cached)
            self.assertTrue(second.cached)
            self.assertFalse(first_footprint.cached)
            self.assertTrue(second_footprint.cached)
            self.assertEqual(first.path, second.path)
            self.assertTrue(first.path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
            feature = LakeMonitor(directory).spatial_assets()["features"][0]
            self.assertEqual(len(feature["properties"]["preview_coordinates"]), 4)
            self.assertTrue(feature["properties"]["preview_cache_key"].endswith("preview-v1"))
            self.assertEqual(feature["properties"]["geometry_source"], "valid_data")
            self.assertEqual(feature["geometry"]["type"], "Polygon")
            self.assertNotEqual(
                feature["geometry"]["coordinates"][0],
                feature["properties"]["preview_coordinates"] + [feature["properties"]["preview_coordinates"][0]],
            )
            self.assertEqual(len(list((Path(directory) / "cache" / "footprints").glob("*.geojson"))), 1)

    def test_valid_data_footprint_drops_internal_nodata_holes(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            path = lake.source_item_directory("nasa", "HLSL30_V2.0", "granule-hole") / "scene.B04.tif"
            path.parent.mkdir(parents=True)
            values = np.ones((5, 5), dtype="uint16")
            values[2, 2] = 0
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=5,
                height=5,
                count=1,
                dtype="uint16",
                crs="EPSG:4326",
                transform=from_origin(-101, 41, 0.01, 0.01),
                nodata=0,
            ) as dataset:
                dataset.write(values, 1)
            asset_id = lake.record_asset(
                run_id=lake.start_run({"interface": "test"}),
                catalog="nasa",
                item={"id": "granule-hole", "collection": "HLSL30_V2.0", "properties": {}},
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=path,
                status="downloaded",
            )

            geometry = valid_data_footprint(directory, LakeMonitor(directory).asset(asset_id)).geometry

            self.assertEqual(geometry["type"], "Polygon")
            self.assertEqual(len(geometry["coordinates"]), 1)


if __name__ == "__main__":
    unittest.main()
