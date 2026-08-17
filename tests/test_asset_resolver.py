import unittest

from stac_core import AssetResolutionError, resolve_assets
from stac_identity import AssetIdentity


def asset(href, *, roles=None, media_type=None):
    value = {"href": href}
    if roles is not None:
        value["roles"] = roles
    if media_type is not None:
        value["type"] = media_type
    return value


class AssetResolverTests(unittest.TestCase):
    def test_preview_roles_are_not_selected_as_main(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "thumbnail": asset("https://example/preview.jpg", roles=["thumbnail"], media_type="image/jpeg"),
                "visual": asset("https://example/visual.tif", roles=["visual"], media_type="image/tiff"),
                "overview": asset("https://example/overview.tif", roles=["overview"], media_type="image/tiff"),
                "metadata": asset("https://example/item.json", roles=["metadata"], media_type="application/json"),
                "B04": asset("https://example/B04.tif", roles=["data"], media_type="image/tiff"),
            },
        }

        result = resolve_assets(item, "test", mode="main")

        self.assertEqual([key for key, _ in result.selected], ["B04"])
        self.assertEqual(
            {decision.asset_key for decision in result.decisions if not decision.accepted},
            {"thumbnail", "visual", "overview", "metadata"},
        )

    def test_explicit_metadata_remains_downloadable(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "metadata": asset("https://example/item.json", roles=["metadata"], media_type="application/json"),
            },
        }

        result = resolve_assets(item, "test", asset_keys=["metadata"])

        self.assertEqual([key for key, _ in result.selected], ["metadata"])
        self.assertEqual(result.policy, "explicit")

    def test_explicit_missing_asset_fails_without_fallback(self):
        item = {"id": "item", "collection": "generic", "assets": {"visual": asset("https://example/visual.jpg")}}

        with self.assertRaisesRegex(AssetResolutionError, "requested Asset key is missing"):
            resolve_assets(item, "test", asset_keys=["B04"])

    def test_roles_missing_scientific_tiff_is_accepted(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"data": asset("https://example/data.tif", media_type="image/tiff")},
        }

        result = resolve_assets(item, "test", mode="main")

        self.assertEqual([key for key, _ in result.selected], ["data"])

    def test_roles_missing_jpeg_preview_is_rejected(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"preview": asset("https://example/preview.jpg", media_type="image/jpeg")},
        }

        with self.assertRaisesRegex(AssetResolutionError, "no policy-approved"):
            resolve_assets(item, "test", mode="main")

    def test_roleless_preview_tiff_is_rejected_as_automatic_data(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"preview": asset("https://example/preview.tif", media_type="image/tiff")},
        }

        with self.assertRaisesRegex(AssetResolutionError, "no policy-approved"):
            resolve_assets(item, "test", mode="main")

    def test_roleless_browse_tiff_is_rejected_as_automatic_data(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"browse": asset("https://example/browse.tif", media_type="image/tiff")},
        }

        with self.assertRaisesRegex(AssetResolutionError, "no policy-approved"):
            resolve_assets(item, "test", mode="main")

    def test_roleless_quicklook_tiff_is_rejected_as_automatic_data(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"quicklook": asset("https://example/quicklook.tif", media_type="image/tiff")},
        }

        with self.assertRaisesRegex(AssetResolutionError, "no policy-approved"):
            resolve_assets(item, "test", mode="main")

    def test_explicit_preview_tiff_remains_selectable(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"preview": asset("https://example/preview.tif", media_type="image/tiff")},
        }

        result = resolve_assets(item, "test", asset_keys=["preview"])

        self.assertEqual([key for key, _ in result.selected], ["preview"])

    def test_preview_signal_precedes_scientific_role_and_extension_heuristics(self):
        for key, value in (
            ("B04", {**asset("https://example/B04.tif"), "title": "Preview image"}),
            ("quicklook", asset("https://example/quicklook.tif", roles=["data"])),
        ):
            item = {"id": "item", "collection": "generic", "assets": {key: value}}
            with self.subTest(key=key), self.assertRaisesRegex(AssetResolutionError, "no policy-approved"):
                resolve_assets(item, "test", mode="main")

    def test_ambiguous_assets_fail_closed(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "first": asset("https://example/first.bin", media_type="application/octet-stream"),
                "second": asset("https://example/second.bin", media_type="application/octet-stream"),
            },
        }

        with self.assertRaisesRegex(AssetResolutionError, "ambiguous"):
            resolve_assets(item, "test", mode="main")

    def test_generic_main_rejects_multiple_roleless_tiff_candidates(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "a": asset("https://example/a.tif", media_type="image/tiff"),
                "b": asset("https://example/b.tif", media_type="image/tiff"),
            },
        }

        with self.assertRaisesRegex(AssetResolutionError, "ambiguous"):
            resolve_assets(item, "test", mode="main")

        self.assertEqual(
            [key for key, _ in resolve_assets(item, "test", mode="all").selected],
            ["a", "b"],
        )

    def test_generic_explicit_selection_does_not_infer_other_candidates(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "a": asset("https://example/a.tif", media_type="image/tiff"),
                "b": asset("https://example/b.tif", media_type="image/tiff"),
            },
        }

        self.assertEqual(
            [key for key, _ in resolve_assets(item, "test", asset_keys=["a"]).selected],
            ["a"],
        )

    def test_role_data_beats_weaker_extension_only_candidate(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "role-data": asset("https://example/role-data.tif", roles=["data"]),
                "extension-data": asset("https://example/extension-data.tif"),
            },
        }

        result = resolve_assets(item, "test", mode="main")

        self.assertEqual([key for key, _ in result.selected], ["role-data"])

    def test_missing_href_fails_closed(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {"data": {"type": "image/tiff"}},
        }

        with self.assertRaisesRegex(AssetResolutionError, "missing or invalid href"):
            resolve_assets(item, "test", mode="main")

    def test_all_is_scientific_assets_only_and_order_is_stable(self):
        item = {
            "id": "item",
            "collection": "generic",
            "assets": {
                "thumbnail": asset("https://example/preview.jpg", roles=["thumbnail"]),
                "B08": asset("https://example/B08.tif"),
                "B04": asset("https://example/B04.tif"),
            },
        }

        result = resolve_assets(item, "test", mode="all")

        self.assertEqual([key for key, _ in result.selected], ["B04", "B08"])

    def test_signed_url_does_not_change_selection_or_identity(self):
        first = {
            "id": "item",
            "collection": "generic",
            "assets": {"data": asset("https://example/data.tif?token=AAA")},
        }
        second = {
            **first,
            "assets": {"data": asset("https://example/data.tif?token=BBB")},
        }

        self.assertEqual(resolve_assets(first, "test").selected[0][0], resolve_assets(second, "test").selected[0][0])
        self.assertEqual(
            AssetIdentity("test", "generic", "item", "data"),
            AssetIdentity("test", "generic", "item", "data"),
        )

    def test_hls_profiles_keep_sensor_specific_representative_assets(self):
        l30 = {
            "id": "l30",
            "collection": "HLSL30_V2.0",
            "assets": {key: asset(f"https://example/{key}.tif") for key in ("B04", "B05", "B08", "Fmask")},
        }
        s30 = {
            "id": "s30",
            "collection": "HLSS30_V2.0",
            "assets": {key: asset(f"https://example/{key}.tif") for key in ("B04", "B05", "B8A", "Fmask")},
        }

        self.assertEqual([key for key, _ in resolve_assets(l30, "nasa").selected], ["B04", "B05", "Fmask"])
        self.assertEqual([key for key, _ in resolve_assets(s30, "nasa").selected], ["B04", "B8A", "Fmask"])


if __name__ == "__main__":
    unittest.main()
