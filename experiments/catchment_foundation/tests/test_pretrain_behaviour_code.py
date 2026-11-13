"""Tests for the behaviour-pretraining adapter: year selection, items, readers, held-out basins."""
import importlib.util
import os
import tempfile
import unittest

import numpy as np
import pandas as pd
import torch

from experiments.catchment_foundation import pretrain_behaviour_code as pretrain
from experiments.catchment_foundation import tabular_forecast_baseline as tabular

try:
    HAVE_FF = importlib.util.find_spec("flood_forecast.meta_models.behaviour_pretrain") is not None
except ModuleNotFoundError:
    HAVE_FF = False


def synthetic_daily(gain: float, seed: int, start: str = "2012-01-01",
                    end: str = "2021-12-31") -> pd.DataFrame:
    """A basin whose flow is a share of the last days' rain."""
    days = pd.date_range(start, end, freq="D")
    rng = np.random.default_rng(seed)
    rain = rng.gamma(0.3, 8.0, len(days))
    flow = gain * pd.Series(rain).rolling(5, min_periods=1).mean().to_numpy()
    return pd.DataFrame({"q_mm": flow, "p": rain, "t": 10.0 + 8.0 * np.sin(days.dayofyear / 58.0),
                         "pet": 2.0}, index=days).astype(np.float32)


class TestWaterYears(unittest.TestCase):
    """A water year needs its spin-up, its 365 days and enough flow."""

    def test_years_need_spin_up_full_length_and_flow(self):
        daily = synthetic_daily(0.5, 0)
        daily.loc["2015-10-01":"2016-06-30", "q_mm"] = np.nan       # 91 days of flow left
        years = pretrain.water_years(daily)
        self.assertEqual(years[0], pd.Timestamp("2012-10-01"))     # spin-up from July 2012
        self.assertNotIn(pd.Timestamp("2015-10-01"), years)
        self.assertEqual(years[-1], pd.Timestamp("2020-10-01"))    # 2021 has no full year
        self.assertEqual(pretrain.water_years(daily.iloc[:0]), [])


class TestWaterYearDataset(unittest.TestCase):
    """Items keep the decoded year away from the reader and stay in physical units."""

    def setUp(self):
        self.daily = {"a": synthetic_daily(0.2, 1), "b": synthetic_daily(0.8, 2),
                      "short": synthetic_daily(0.5, 3, start="2020-01-01")}

    def test_item_shapes_units_and_spin_up(self):
        dataset = pretrain.WaterYearDataset(self.daily, ["a", "b", "short"], n_read=3, seed=0)
        self.assertEqual(dataset.site_ids, ["a", "b"])              # one usable year is too few
        item = dataset[0]
        self.assertEqual(item["history"].shape, (3, 365, 7))
        self.assertEqual(item["drivers"].shape, (455, 5))
        self.assertTrue(torch.isnan(item["target"][:90]).all())
        self.assertTrue(torch.isfinite(item["target"][90:]).all())
        # Flow is not rescaled per basin: the wetter basin's log flow is higher by log(4).
        flows = [np.mean([dataset.member(site, year)[:, 0].mean() for year in dataset.years[site]])
                 for site in ("a", "b")]
        self.assertAlmostEqual(flows[1] - flows[0], np.log(4.0), delta=0.15)

    def test_the_decoded_year_is_never_read(self):
        dataset = pretrain.WaterYearDataset(self.daily, ["a", "b"], n_read=3, draws=20, seed=0)
        for index in range(len(dataset)):
            _, decoded, read = dataset.choose(index)
            self.assertNotIn(decoded, read)
            self.assertEqual(len(read), 3)

    def test_fixed_items_repeat_and_missing_flow_is_flagged(self):
        self.daily["a"].loc["2014-11-01":"2014-11-10", "q_mm"] = np.nan
        dataset = pretrain.WaterYearDataset(self.daily, ["a", "b"], n_read=2, draws=3, seed=5,
                                            fixed=True)
        self.assertEqual(dataset.choose(4), dataset.choose(4))
        member = dataset.member("a", pd.Timestamp("2014-10-01"))
        self.assertEqual(member[31:41, 1].sum(), 0.0)               # valid flag off for the gap
        self.assertEqual(member[31:41, 0].sum(), 0.0)
        self.assertEqual(member[:, 1].sum(), 355.0)

    def test_reader_groups_cover_the_record(self):
        reader = pretrain.ReadYearsDataset(self.daily, ["a", "short"], n_read=3)
        self.assertEqual(reader.site_ids, ["a", "short"])
        seen = set()
        for group in range(3):
            reader.group = group
            seen.update(reader.read_years("a"))
        self.assertEqual(seen, set(reader.years["a"]))
        self.assertEqual(reader[1]["history"].shape, (3, 365, 7))   # one year, repeated

    def test_aux_is_standardized_on_the_given_basins_only(self):
        early = pd.DataFrame({name: [1.0, 3.0, 100.0] for name in pretrain.AUX},
                             index=["a", "b", "held"])
        early.loc["b", "bfi"] = np.nan
        aux = pretrain.standardized_aux(early, ["a", "b"])
        self.assertAlmostEqual(aux.loc["a", "low_flow"], -1.0 / np.sqrt(2.0), places=5)
        self.assertEqual(aux.loc["held", "low_flow"], 5.0)          # clipped, not used to fit
        self.assertTrue(np.isnan(aux.loc["b", "bfi"]))


def write_bank(folder: str, name: str, sites: list, values: np.ndarray) -> None:
    """Writes a context bank the way the baselines store one."""
    torch.save({"site_ids": sites, "embeddings": torch.tensor(values, dtype=torch.float32)},
               os.path.join(folder, "context_%s.pt" % name))


class TestLastingInputs(unittest.TestCase):
    """The static description and the reach image reach the readers as the baselines see them."""

    def setUp(self):
        self.daily = {"a": synthetic_daily(0.2, 1), "b": synthetic_daily(0.8, 2),
                      "short": synthetic_daily(0.5, 3, start="2021-06-01")}
        self.statics = pd.DataFrame({0: [1.0, 2.0, 3.0], 1: [0.5, -0.5, 0.0]},
                                    index=["a", "b", "short"], dtype=np.float32)
        rng = np.random.default_rng(0)
        self.images = {site: rng.normal(size=(2, 8, 8)).astype(np.float32)
                       for site in ("a", "b", "short")}

    def test_static_banks_are_joined_and_constant_columns_dropped(self):
        sites = ["a", "b", "held"]
        with tempfile.TemporaryDirectory() as tmp:
            write_bank(tmp, "statics", sites, np.array([[1.0, 0.0, 7.0], [2.0, 0.0, 7.0],
                                                        [3.0, 0.0, 9.0]]))
            write_bank(tmp, "location", sites, np.array([[0.1], [0.2], [0.3]]))
            frame = pretrain.load_static_banks(tmp, ["statics", "location"], ["a", "b"])
        # Padding is dropped by the bank loader; 7.0 does not vary over the training basins.
        self.assertEqual(frame.shape, (3, 2))
        np.testing.assert_allclose(frame.loc["held"].to_numpy(), [3.0, 0.3], rtol=1e-6)

    def test_images_are_standardized_per_band_on_the_fitting_basins(self):
        rng = np.random.default_rng(1)
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "CO"))
            for site, level in (("a", 1500.0), ("b", 2500.0), ("held", 9000.0)):
                image = level + rng.normal(0.0, 100.0, (2, 8, 8)) * np.array([1.0, 3.0])[:, None, None]
                np.savez(os.path.join(tmp, "CO", site + ".npz"), image=image.astype(np.float32))
            images = pretrain.load_images(tmp, {"a": "CO", "b": "CO", "held": "CO", "gone": "CO"},
                                          ["a", "b"])
        self.assertEqual(sorted(images), ["a", "b", "held"])
        fitted = np.stack([images["a"], images["b"]])
        np.testing.assert_allclose(fitted.mean(axis=(0, 2, 3)), 0.0, atol=1e-4)
        np.testing.assert_allclose(fitted.std(axis=(0, 2, 3)), 1.0, atol=1e-4)
        # Reflectance is clipped at twice the scale before standardizing: each band is flat.
        np.testing.assert_allclose(images["held"].std(axis=(1, 2)), 0.0, atol=1e-5)

    def test_items_carry_both_inputs_and_only_training_images_are_turned(self):
        fixed = pretrain.WaterYearDataset(self.daily, ["a", "b"], n_read=2, fixed=True,
                                          statics=self.statics, images=self.images, augment=True)
        item = fixed[1]
        np.testing.assert_allclose(item["statics"].numpy(), [2.0, -0.5])
        np.testing.assert_array_equal(item["image"].numpy(), self.images["b"])
        turned = pretrain.WaterYearDataset(self.daily, ["a"], n_read=2, draws=12, seed=3,
                                           statics=self.statics, images=self.images,
                                           augment=True)
        images = [turned[index]["image"].numpy() for index in range(12)]
        self.assertTrue(any(not np.array_equal(image, self.images["a"]) for image in images))
        for image in images:                                       # same pixels, moved
            np.testing.assert_allclose(np.sort(image, axis=None),
                                       np.sort(self.images["a"], axis=None))
            np.testing.assert_allclose(image.mean(axis=(1, 2)), self.images["a"].mean(axis=(1, 2)),
                                       atol=1e-6)

    def test_codes_without_the_gauged_record_never_see_it(self):
        reader = pretrain.ReadYearsDataset(self.daily, ["a", "short"], n_read=2,
                                           statics=self.statics, images=self.images,
                                           read_history=False)
        self.assertEqual(reader.site_ids, ["a", "short"])           # no usable year needed
        for index in range(2):
            self.assertEqual(float(reader[index]["history"].abs().sum()), 0.0)
        self.assertEqual(reader[1]["image"].shape, (2, 8, 8))
        gauged = pretrain.ReadYearsDataset(self.daily, ["a", "short"], n_read=2,
                                           statics=self.statics, images=self.images)
        self.assertEqual(gauged.site_ids, ["a"])
        self.assertGreater(float(gauged[0]["history"].abs().sum()), 0.0)

    def test_banks_written_for_each_set_of_readers(self):
        self.assertEqual(pretrain.reader_subsets(["history"]), {"": ("history",)})
        self.assertEqual(pretrain.reader_subsets(["history", "statics"]),
                         {"": ("history", "statics"), "gauge": ("history",),
                          "ungauged": ("statics",)})
        self.assertEqual(pretrain.reader_subsets(["history", "statics", "image"]),
                         {"": ("history", "statics", "image"), "gauge": ("history",),
                          "ungauged": ("statics", "image"), "statics": ("statics",),
                          "image": ("image",)})

    def test_shuffled_bank_hands_each_basin_another_code_of_its_split(self):
        sites = ["t0", "t1", "t2", "h0", "h1"]
        split = {site: "train" if site.startswith("t") else "holdout" for site in sites}
        with tempfile.TemporaryDirectory() as tmp:
            write_bank(tmp, "code", sites, np.arange(5.0)[:, None] + 1.0)
            name = pretrain.shuffle_bank(tmp, "code", seed=0, split=split)
            shuffled = tabular.load_bank(tmp, name)
        self.assertEqual(name, "code_shuffled")
        values = shuffled[0].to_dict()
        self.assertEqual(sorted(values[site] for site in sites[:3]), [1.0, 2.0, 3.0])
        self.assertEqual([values["h0"], values["h1"]], [5.0, 4.0])
        self.assertTrue(all(values[site] != index + 1.0 for index, site in enumerate(sites)))


@unittest.skipUnless(HAVE_FF, "needs Flow Forecast's behaviour_pretrain on the path")
class TestTrain(unittest.TestCase):
    """The run trains on training basins only and writes a code for every basin."""

    def test_bank_covers_all_basins_and_separates_wet_from_dry(self):
        rng = np.random.default_rng(0)
        gains = np.exp(rng.uniform(np.log(0.05), np.log(2.0), 40))
        daily = {"s%02d" % index: synthetic_daily(gain, index, start="2015-01-01")
                 for index, gain in enumerate(gains)}
        split = {site: "holdout" if index >= 30 else "train" for index, site in enumerate(daily)}
        early = pd.DataFrame({name: np.log(gains) for name in pretrain.AUX}, index=list(daily))
        with tempfile.TemporaryDirectory() as tmp:
            summary = pretrain.train(tmp, "tiny", seed=0, epochs=12, code_dim=4, dim=16, depth=1,
                                     hidden=16, dropout=0.0, n_read=2, draws=4, batch_size=16,
                                     lr=3e-3, aux_weight=0.5, validation_fraction=0.2,
                                     read_passes=2, cache={"daily": daily, "split": split},
                                     early=early, verbose=False)
            bank = tabular.load_bank(tmp, "tiny")
            self.assertTrue(os.path.exists(os.path.join(tmp, "reports", "pretrain_tiny.json")))
        self.assertEqual(summary["fit_basins"], 24)
        self.assertEqual(summary["validation_basins"], 6)
        self.assertEqual(sorted(bank.index), sorted(daily))
        self.assertLess(summary["series_loss"][-1], summary["series_loss"][0])
        # Held-out basins were never trained on, yet their codes order them by wetness.
        held = [site for site in daily if split[site] == "holdout"]
        fitted = [site for site in daily if split[site] == "train"]
        design = np.column_stack([bank.loc[fitted].to_numpy(), np.ones(len(fitted))])
        weights = np.linalg.lstsq(design, np.log(gains[:30]), rcond=None)[0]
        predicted = np.column_stack([bank.loc[held].to_numpy(), np.ones(len(held))]) @ weights
        self.assertGreater(np.corrcoef(predicted, np.log(gains[30:]))[0, 1], 0.8)

    def test_several_readers_write_a_code_without_the_gauged_record(self):
        rng = np.random.default_rng(1)
        gains = np.exp(rng.uniform(np.log(0.05), np.log(2.0), 40))
        daily = {"s%02d" % index: synthetic_daily(gain, index, start="2015-01-01")
                 for index, gain in enumerate(gains)}
        sites = list(daily)
        split = {site: "holdout" if index >= 30 else "train" for index, site in enumerate(sites)}
        early = pd.DataFrame({name: np.log(gains) for name in pretrain.AUX}, index=sites)
        # The static description knows the wetness roughly; the image carries nothing.
        wetness = (np.log(gains) - np.log(gains[:30]).mean()) / np.log(gains[:30]).std()
        statics = pd.DataFrame({0: wetness + rng.normal(0.0, 0.1, 40), 1: rng.normal(size=40)},
                               index=sites)
        images = {site: rng.normal(size=(2, 16, 16)).astype(np.float32) for site in sites}
        with tempfile.TemporaryDirectory() as tmp:
            summary = pretrain.train(tmp, "tiny", seed=0, epochs=14, code_dim=4, dim=16, depth=1,
                                     hidden=16, dropout=0.0, n_read=2, draws=4, batch_size=16,
                                     lr=3e-3, aux_weight=0.5, validation_fraction=0.2,
                                     read_passes=2, modality_dropout=0.5, patch_size=8,
                                     cache={"daily": daily, "split": split}, early=early,
                                     statics=statics, images=images, verbose=False)
            banks = {name: tabular.load_bank(tmp, name) for name in summary["banks"]}
        self.assertEqual(summary["readers"], ["history", "statics", "image"])
        self.assertEqual(sorted(banks), ["tiny", "tiny_gauge", "tiny_image", "tiny_statics",
                                         "tiny_ungauged"])
        self.assertEqual(sorted(summary["validation_by_readers"]),
                         ["all", "gauge", "image", "mean code", "statics", "ungauged"])
        for bank in banks.values():
            self.assertEqual(sorted(bank.index), sorted(sites))
        held, fitted = sites[30:], sites[:30]

        def held_out_correlation(bank: pd.DataFrame) -> float:
            design = np.column_stack([bank.loc[fitted].to_numpy(), np.ones(len(fitted))])
            weights = np.linalg.lstsq(design, np.log(gains[:30]), rcond=None)[0]
            predicted = np.column_stack([bank.loc[held].to_numpy(), np.ones(len(held))]) @ weights
            return float(np.corrcoef(predicted, np.log(gains[30:]))[0, 1])

        # Without their gauged record, held-out basins are still ordered by wetness.
        self.assertGreater(held_out_correlation(banks["tiny_ungauged"]), 0.8)
        self.assertGreater(held_out_correlation(banks["tiny"]), 0.8)
        self.assertFalse(np.allclose(banks["tiny"].to_numpy(), banks["tiny_ungauged"].to_numpy()))
        self.assertLess(summary["validation_by_readers"]["all"],
                        summary["validation_by_readers"]["mean code"])

    def test_aligned_readers_write_into_the_gauge_code_without_changing_it(self):
        rng = np.random.default_rng(2)
        gains = np.exp(rng.uniform(np.log(0.05), np.log(2.0), 40))
        daily = {"s%02d" % index: synthetic_daily(gain, index, start="2015-01-01")
                 for index, gain in enumerate(gains)}
        sites = list(daily)
        split = {site: "holdout" if index >= 30 else "train" for index, site in enumerate(sites)}
        cache = {"daily": daily, "split": split}
        wetness = (np.log(gains) - np.log(gains[:30]).mean()) / np.log(gains[:30]).std()
        statics = pd.DataFrame({0: wetness + rng.normal(0.0, 0.1, 40), 1: rng.normal(size=40)},
                               index=sites)
        with tempfile.TemporaryDirectory() as tmp:
            pretrain.train(tmp, "gauge", seed=3, epochs=12, code_dim=4, dim=16, depth=1,
                           hidden=16, dropout=0.0, n_read=2, draws=4, batch_size=16, lr=3e-3,
                           validation_fraction=0.2, read_passes=2, cache=cache, verbose=False)
            before = torch.load(os.path.join(tmp, "behaviour_gauge.pt"), weights_only=True)
            summary = pretrain.align(tmp, "student", "gauge", epochs=30, dim=16, depth=1,
                                     dropout=0.0, draws=4, batch_size=16, lr=3e-3,
                                     validation_fraction=0.2, read_passes=2, cache=cache,
                                     statics=statics, static_banks=(), verbose=False)
            after = torch.load(os.path.join(tmp, "behaviour_student.pt"), weights_only=True)
            gauge, student, transfer = (tabular.load_bank(tmp, name)
                                        for name in ("gauge", "student", "student_transfer"))
            with self.assertRaises(ValueError):                  # the teacher must read the gauge
                pretrain.align(tmp, "again", "student", cache=cache, statics=statics,
                               verbose=False)
        self.assertTrue(summary["decoder_unchanged"])
        for key, value in before["decoder"].items():
            torch.testing.assert_close(after["decoder"][key], value)
        self.assertEqual(summary["readers"], ["statics"])
        self.assertEqual((summary["fit_basins"], summary["validation_basins"]), (24, 6))
        held, fitted = sites[30:], sites[:30]
        # Training basins keep their gauge code; held-out basins get the code from statics.
        np.testing.assert_allclose(transfer.loc[fitted].to_numpy(), gauge.loc[fitted].to_numpy(),
                                   atol=1e-5)
        np.testing.assert_allclose(transfer.loc[held].to_numpy(), student.loc[held].to_numpy(),
                                   atol=1e-5)
        # The two codes share a space: a readout fitted on gauge codes works on the new codes.
        design = np.column_stack([gauge.loc[fitted].to_numpy(), np.ones(len(fitted))])
        weights = np.linalg.lstsq(design, np.log(gains[:30]), rcond=None)[0]
        predicted = np.column_stack([transfer.loc[held].to_numpy(), np.ones(len(held))]) @ weights
        self.assertGreater(np.corrcoef(predicted, np.log(gains[30:]))[0, 1], 0.8)
        checks = summary["validation"]
        self.assertLess(checks["gauge code"], checks["mean code"])
        self.assertLess(checks["new code"], checks["mean code"])


class TestPool(unittest.TestCase):
    """Extra basins are pretrained on and nothing else: no code, no validation, no scale."""

    SPLIT = {"a": {"group": "140100", "fold": 0, "state": "CO"},
             "b": {"group": "140200", "fold": 2, "state": "CO"},
             "c": {"group": "140100", "fold": 3, "state": "UT"},   # shares a group with a
             "d": {"group": "140300", "fold": 1, "state": "UT"},
             "e": {"group": "140200", "fold": 2, "state": "CO"},
             "f": {"group": "140400", "fold": 4, "state": "UT"}}

    def test_selection_leaves_out_the_cohort_and_held_out_groups(self):
        basins = [{"site_id": site} for site in ("a", "b", "c", "d", "e", "e", "x", "f")]
        chosen, dropped = pretrain.select_pool(basins, self.SPLIT, cohort=["b"], exclude_folds=[0])
        self.assertEqual([basin["site_id"] for basin in chosen], ["d", "e", "f"])
        self.assertEqual(dropped, {"in a held-out group": 2, "not in the split": 1,
                                   "already in the cohort or listed twice": 2})
        chosen, _ = pretrain.select_pool(basins, self.SPLIT, cohort=["b"], exclude_folds=[0, 1])
        self.assertEqual([basin["site_id"] for basin in chosen], ["e", "f"])
        tidal = [{"site_id": "d", "negative_flow_fraction": 0.2}, {"site_id": "f"},
                 {"site_id": "e", "negative_flow_fraction": 0.005}]
        chosen, dropped = pretrain.select_pool(tidal, self.SPLIT, cohort=[], exclude_folds=[0],
                                               max_negative_flow_fraction=0.01)
        self.assertEqual([basin["site_id"] for basin in chosen], ["f", "e"])
        self.assertEqual(dropped, {"tidal or backwater": 1})

    def test_members_need_a_static_description_when_statics_are_read(self):
        pool = {"daily": {"p1": None, "p2": None, "s1": None}}
        statics = pd.DataFrame({0: [0.0, 1.0]}, index=["s1", "p2"])
        split = {"s1": "train"}
        self.assertEqual(pretrain.pool_members(None, split, None), [])
        self.assertEqual(pretrain.pool_members(pool, split, None), ["p1", "p2"])
        self.assertEqual(pretrain.pool_members(pool, split, statics), ["p2"])

    def test_static_banks_take_extra_rows_on_the_same_columns(self):
        sites = ["s0", "s1", "s2", "s3"]
        with tempfile.TemporaryDirectory() as tmp:
            write_bank(tmp, "statics", sites, np.array([[1.0, 5.0, 0.0], [2.0, 5.0, 0.0],
                                                        [3.0, 5.0, 0.0], [4.0, 5.0, 0.0]]))
            write_bank(tmp, "location", sites, np.array([[0.1], [0.2], [0.3], [0.4]]))
            extra = {"statics": pd.DataFrame([[9.0, 5.0], [8.0, 5.0]], index=["p1", "p2"]),
                     "location": pd.DataFrame([[0.9]], index=["p1"])}
            frame = pretrain.load_static_banks(tmp, ["statics", "location"], sites[:3],
                                               extra=extra)
            with self.assertRaises(ValueError):
                pretrain.load_static_banks(tmp, ["statics", "location"], sites[:3],
                                           extra={"statics": extra["statics"]})
        # p2 has no location and is left out; the column that is constant in training is dropped.
        self.assertEqual(list(frame.index), sites + ["p1"])
        np.testing.assert_allclose(frame.loc["p1"].to_numpy(), [9.0, 0.9], rtol=1e-6)

    @unittest.skipUnless(HAVE_FF, "Flow Forecast behaviour_pretrain is not importable")
    def test_pool_basins_are_fitted_on_but_get_no_code(self):
        rng = np.random.default_rng(4)
        gains = np.exp(rng.uniform(np.log(0.05), np.log(2.0), 32))
        daily = {"s%02d" % index: synthetic_daily(gain, index, start="2015-01-01")
                 for index, gain in enumerate(gains)}
        sites = list(daily)
        cohort, extra = sites[:20], sites[20:]
        split = {site: "holdout" if index >= 15 else "train" for index, site in enumerate(cohort)}
        cache = {"daily": {site: daily[site] for site in cohort}, "split": split}
        early = pd.DataFrame({name: np.log(gains) for name in pretrain.AUX}, index=sites)
        wetness = pd.DataFrame({0: np.log(gains), 1: rng.normal(size=32)}, index=sites)
        pool = {"daily": {site: daily[site] for site in extra + cohort[:2]},   # two overlap
                "early": early.loc[extra], "banks": None}
        described = wetness.drop(index=extra[:4])            # four pool basins have no statics
        with tempfile.TemporaryDirectory() as tmp:
            options = dict(seed=5, epochs=3, code_dim=4, dim=16, depth=1, hidden=16, dropout=0.0,
                           n_read=2, draws=2, batch_size=16, lr=3e-3, validation_fraction=0.2,
                           read_passes=2, cache=cache, verbose=False)
            alone = pretrain.train(tmp, "alone", aux_weight=0.5, early=early.loc[cohort],
                                   **options)
            pooled = pretrain.train(tmp, "pooled", aux_weight=0.5, early=early.loc[cohort],
                                    pool=pool, **options)
            student = pretrain.align(tmp, "student", "pooled", epochs=3, dim=16, depth=1,
                                     dropout=0.0, draws=2, batch_size=16, lr=3e-3,
                                     validation_fraction=0.2, read_passes=2, cache=cache,
                                     statics=described, static_banks=(), pool=pool, verbose=False)
            banks = {name: tabular.load_bank(tmp, name)
                     for name in ("pooled", "student", "student_transfer")}
        self.assertEqual((alone["fit_basins"], alone["pool_basins"]), (12, 0))
        self.assertEqual((pooled["fit_basins"], pooled["pool_basins"]), (24, 12))
        self.assertEqual(pooled["validation_basins"], alone["validation_basins"])
        self.assertEqual((student["fit_basins"], student["pool_basins"]), (20, 8))
        for bank in banks.values():                          # codes for the cohort only
            self.assertEqual(sorted(bank.index), sorted(cohort))


if __name__ == "__main__":
    unittest.main()
