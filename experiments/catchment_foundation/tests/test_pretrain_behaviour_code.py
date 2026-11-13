"""Tests for the behaviour-pretraining adapter: year selection, items, and held-out basins."""
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


if __name__ == "__main__":
    unittest.main()
