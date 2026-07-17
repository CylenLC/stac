import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pystac
import rasterio
from rasterio.transform import from_origin

from earth_lake import EarthLake, sha256_file


class EarthLakeTests(unittest.TestCase):
    def test_initialize_and_record_hls_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"catalog": "nasa", "test": True})
            item = {
                "id": "granule-1",
                "collection": "HLSL30_V2.0",
                "bbox": [-101.0, 39.0, -99.0, 41.0],
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-101.0, 39.0], [-99.0, 39.0], [-99.0, 41.0], [-101.0, 41.0], [-101.0, 39.0]]],
                },
                "properties": {"datetime": "2024-01-01T12:00:00Z"},
            }
            asset_path = lake.source_item_directory("nasa", item["collection"], item["id"]) / "scene.B04.tif"
            asset_path.parent.mkdir(parents=True)
            with rasterio.open(
                asset_path,
                "w",
                driver="GTiff",
                width=2,
                height=3,
                count=1,
                dtype="int16",
                crs="EPSG:32619",
                transform=from_origin(500000, 5500000, 30, 30),
                nodata=-9999,
            ) as dataset:
                dataset.write(np.array([[1, 2], [3, 4], [5, 6]], dtype="int16"), 1)

            asset_id = lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=asset_path,
                status="downloaded",
            )
            lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="B04",
                source_url="https://example/scene.B04.tif",
                local_path=asset_path,
                status="skipped",
            )
            lake.finish_run(run_id, "completed", [asset_id])

            self.assertTrue((lake.protocol_dir / "earth_zarr_protocol.json").exists())
            self.assertEqual(len(pq.read_table(lake.registry_dir / "assets.parquet")), 1)
            asset = pq.read_table(lake.registry_dir / "assets.parquet").to_pylist()[0]
            self.assertEqual(asset["checksum_sha256"], sha256_file(asset_path))
            self.assertEqual(asset["local_path"], "source/nasa/HLSL30_V2.0/granule-1/scene.B04.tif")
            self.assertEqual(asset["run_id"], run_id)
            self.assertEqual(asset["status"], "downloaded")

            variable = pq.read_table(lake.registry_dir / "variables.parquet").to_pylist()[0]
            self.assertEqual(variable["canonical_name"], "surface_reflectance_red")
            self.assertEqual(variable["unit"], "1")
            self.assertEqual(variable["central_wavelength_nm"], 655.0)
            self.assertEqual(variable["nodata"], "-9999.0")
            grid = pq.read_table(lake.registry_dir / "grids.parquet").to_pylist()[0]
            self.assertEqual(grid["epsg"], 32619)
            self.assertEqual(grid["x_origin"], 500000.0)
            self.assertEqual(grid["y_origin"], 5500000.0)
            self.assertEqual(grid["pixel_size_x"], 30.0)
            self.assertEqual(grid["pixel_size_y"], -30.0)

            product = pq.read_table(lake.registry_dir / "products.parquet").to_pylist()[0]
            self.assertEqual(product["title"], "Harmonized Landsat Sentinel-2 L30 Version 2.0")
            self.assertIn("hlsl30v002", product["documentation_urls_json"])

            run = pq.read_table(lake.registry_dir / "processing_runs.parquet").to_pylist()[0]
            self.assertEqual(run["status"], "completed")
            self.assertEqual(json.loads(run["output_asset_ids"]), [asset_id])

            catalog = pystac.Catalog.from_file(str(lake.stac_dir / "catalog.json"))
            collection = next(value for value in catalog.get_collections() if value.id == "hlsl30_v2.0")
            stac_item = next(collection.get_items("granule-1"))
            self.assertIn("B04", stac_item.assets)
            self.assertEqual(stac_item.assets["B04"].extra_fields["earthzarr:asset_id"], asset_id)

    def test_hls_fmask_uses_native_quality_definition(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = EarthLake(directory)
            run_id = lake.start_run({"catalog": "nasa", "test": True})
            item = {"id": "granule-1", "collection": "HLSL30_V2.0", "properties": {}}
            asset_path = lake.source_item_directory("nasa", item["collection"], item["id"]) / "scene.Fmask.tif"
            asset_path.parent.mkdir(parents=True)
            with rasterio.open(
                asset_path,
                "w",
                driver="GTiff",
                width=1,
                height=1,
                count=1,
                dtype="uint8",
                crs="EPSG:32619",
                transform=from_origin(500000, 5500000, 30, 30),
                nodata=255,
            ) as dataset:
                dataset.write(np.array([[0]], dtype="uint8"), 1)
                dataset.update_tags(**{"Fmask bit description": "cloud and water bits"})
            lake.record_asset(
                run_id=run_id,
                catalog="nasa",
                item=item,
                asset_key="Fmask",
                source_url="https://example/scene.Fmask.tif",
                local_path=asset_path,
                status="downloaded",
            )

            variable = pq.read_table(lake.registry_dir / "variables.parquet").to_pylist()[0]
            self.assertIsNone(variable["unit"])
            self.assertEqual(variable["nodata"], "255.0")
            self.assertEqual(variable["quality_flag_definition"], "cloud and water bits")


if __name__ == "__main__":
    unittest.main()
