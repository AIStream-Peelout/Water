"""Tests of the decoder simulation features (decoder_simulation.py)."""
import os
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

from experiments.catchment_foundation import decoder_simulation as simulation  # noqa: E402
from experiments.catchment_foundation import pretrain_behaviour_code as behaviour  # noqa: E402


def record(start: str, end: str, seed: int = 0) -> pd.DataFrame:
    index = pd.date_range(start, end, freq="1D")
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"q_mm": rng.gamma(2.0, 1.0, len(index)), "p": rng.gamma(0.5, 4.0, len(index)),
                         "t": rng.normal(8.0, 6.0, len(index)), "pet": rng.gamma(2.0, 1.0, len(index))},
                        index=index).astype(np.float32)


class TestYearBlocks(unittest.TestCase):
    def test_blocks_write_every_day_once_after_the_first_spin_up(self):
        index = pd.date_range("2015-03-10", "2018-01-20", freq="1D")
        blocks = simulation.year_blocks(index)
        written = np.zeros(len(index), dtype=int)
        for begin, write_from, stop in blocks:
            self.assertGreaterEqual(write_from - begin, 0)
            written[write_from:stop] += 1
        self.assertTrue((written[:behaviour.SPIN_UP_DAYS] == 0).all())
        self.assertTrue((written[behaviour.SPIN_UP_DAYS:] == 1).all())

    def test_a_full_water_year_is_read_as_in_pretraining(self):
        index = pd.date_range("2015-03-10", "2018-01-20", freq="1D")
        blocks = simulation.year_blocks(index)
        begin, write_from, stop = blocks[1]
        self.assertEqual(index[write_from], pd.Timestamp("2015-10-01"))
        self.assertEqual(write_from - begin, behaviour.SPIN_UP_DAYS)
        self.assertEqual(index[stop - 1], pd.Timestamp("2016-09-30"))

    def test_an_empty_record_has_no_blocks(self):
        self.assertEqual(simulation.year_blocks(pd.DatetimeIndex([])), [])


class TestSimulateBasin(unittest.TestCase):
    def setUp(self):
        from flood_forecast.meta_models.behaviour_pretrain import DriverConditionedDecoder
        torch.manual_seed(0)
        self.decoder = DriverConditionedDecoder(5, 4, hidden_dim=8).eval()
        self.code = torch.randn(4)
        self.daily = record("2015-03-10", "2018-01-20")

    def test_matches_a_direct_pass_over_the_pretraining_window(self):
        simulated, _ = simulation.simulate_basin(self.decoder, self.daily, self.code)
        block = self.daily["2016-10-01":"2017-09-30"]
        window = self.daily[block.index[0] - pd.Timedelta(days=behaviour.SPIN_UP_DAYS):block.index[-1]]
        with torch.no_grad():
            direct = self.decoder(torch.from_numpy(behaviour.weather_channels(window))[None],
                                  self.code[None])[0, behaviour.SPIN_UP_DAYS:].numpy()
        position = self.daily.index.get_loc(block.index[0])
        np.testing.assert_allclose(simulated[position:position + len(block)], direct, atol=1e-5)

    def test_only_the_first_spin_up_is_missing(self):
        simulated, _ = simulation.simulate_basin(self.decoder, self.daily, self.code)
        self.assertTrue(np.isnan(simulated[:behaviour.SPIN_UP_DAYS]).all())
        self.assertTrue(np.isfinite(simulated[behaviour.SPIN_UP_DAYS:]).all())

    def test_the_overrun_continues_the_previous_run_past_a_restart(self):
        simulated, overrun = simulation.simulate_basin(self.decoder, self.daily, self.code)
        restart = self.daily.index.get_loc(pd.Timestamp("2016-10-01"))
        days = simulation.HORIZON_DAYS
        self.assertTrue(np.isfinite(overrun[restart:restart + days]).all())
        self.assertTrue(np.isnan(overrun[restart + days:restart + 300]).all())
        window = self.daily[pd.Timestamp("2015-10-01") - pd.Timedelta(days=behaviour.SPIN_UP_DAYS):
                            pd.Timestamp("2016-10-14")]
        with torch.no_grad():
            direct = self.decoder(torch.from_numpy(behaviour.weather_channels(window))[None],
                                  self.code[None])[0].numpy()
        np.testing.assert_allclose(overrun[restart:restart + days], direct[-days:], atol=1e-5)
        np.testing.assert_allclose(simulated[restart - 5:restart], direct[-days - 5:-days], atol=1e-5)

    def test_a_day_does_not_depend_on_later_weather(self):
        simulated, _ = simulation.simulate_basin(self.decoder, self.daily, self.code)
        changed = self.daily.copy()
        cut = self.daily.index.get_loc(pd.Timestamp("2017-02-01"))
        changed.iloc[cut:, changed.columns.get_loc("p")] += 50.0
        again, _ = simulation.simulate_basin(self.decoder, changed, self.code)
        np.testing.assert_allclose(simulated[behaviour.SPIN_UP_DAYS:cut],
                                   again[behaviour.SPIN_UP_DAYS:cut], atol=1e-6)
        self.assertGreater(np.abs(simulated[cut:] - again[cut:]).max(), 1e-4)

    def test_a_water_year_can_be_run_with_its_own_code(self):
        plain, _ = simulation.simulate_basin(self.decoder, self.daily, self.code)
        year = pd.Timestamp("2016-10-01")
        mixed, overrun = simulation.simulate_basin(self.decoder, self.daily, self.code,
                                                   year_codes={year: -self.code})
        other, _ = simulation.simulate_basin(self.decoder, self.daily, -self.code)
        inside = (self.daily.index >= year) & (self.daily.index <= pd.Timestamp("2017-09-30"))
        np.testing.assert_allclose(mixed[inside], other[inside], atol=1e-6)
        outside = ~inside & np.isfinite(plain)
        np.testing.assert_allclose(mixed[outside], plain[outside], atol=1e-6)
        after = self.daily.index.get_loc(pd.Timestamp("2017-10-01"))
        self.assertGreater(np.abs(overrun[after:after + 14] - plain[after:after + 14]).max(), 1e-4)

    def test_water_year_start(self):
        self.assertEqual(simulation.water_year_start(pd.Timestamp("2017-03-05")), pd.Timestamp("2016-10-01"))
        self.assertEqual(simulation.water_year_start(pd.Timestamp("2017-10-01")), pd.Timestamp("2017-10-01"))

    def test_another_code_gives_another_series(self):
        first, _ = simulation.simulate_basin(self.decoder, self.daily, self.code)
        second, _ = simulation.simulate_basin(self.decoder, self.daily, -self.code)
        self.assertGreater(np.nanmax(np.abs(first - second)), 1e-3)


class TestFeatures(unittest.TestCase):
    def test_lead_bands_line_up_with_the_issue_day(self):
        index = pd.date_range("2020-01-01", periods=40, freq="1D")
        level = pd.Series(np.arange(1.0, 41.0), index=index)
        flow = pd.Series(2.0 * level.to_numpy(), index=index)
        frame = simulation.basin_features(np.log(level), flow, scale=0.5)
        day = 9                                   # level 10 on the issue day
        self.assertAlmostEqual(frame["sim_dlog_day1-3"].iloc[day], np.log(12.0 / 10.0), places=5)
        self.assertAlmostEqual(frame["sim_add_day1-3"].iloc[day], 2.0 / 12.0, places=5)
        self.assertAlmostEqual(frame["sim_dlog_day8-14"].iloc[day], np.log(21.0 / 10.0), places=5)
        self.assertAlmostEqual(frame["sim_err0"].iloc[day],
                               np.log(20.0 + behaviour.FLOW_FLOOR_MM) - np.log(10.0), places=5)
        self.assertTrue(np.isnan(frame["sim_dlog_day8-14"].iloc[-14:]).all())

    def test_a_horizon_across_a_restart_is_read_from_the_issue_days_run(self):
        index = pd.date_range("2020-09-01", periods=60, freq="1D")
        restart = index.get_loc(pd.Timestamp("2020-10-01"))
        simulated = pd.Series(np.log(2.0), index=index)
        simulated.iloc[restart:] = np.log(8.0)             # the new run starts at another level
        overrun = pd.Series(np.nan, index=index)
        overrun.iloc[restart:restart + 14] = np.log(2.0)   # the old run stays where it was
        flow = pd.Series(1.0, index=index)
        jumpy = simulation.basin_features(simulated, flow, 1.0)
        steady = simulation.basin_features(simulated, flow, 1.0, overrun)
        before = restart - 2                               # issued two days before the restart
        self.assertGreater(float(jumpy["sim_dlog_day4-7"].iloc[before]), 1.0)
        for band in ("day1-3", "day4-7", "day8-14"):
            self.assertAlmostEqual(float(steady[f"sim_dlog_{band}"].iloc[before]), 0.0, places=6)
        after = restart + 1                                # issued in the new run: no overrun
        self.assertAlmostEqual(float(steady["sim_dlog_day1-3"].iloc[after]), 0.0, places=6)
        early = restart - 20                               # horizon ends before the restart
        self.assertAlmostEqual(float(steady["sim_dlog_day8-14"].iloc[early]), 0.0, places=6)

    def test_depth_is_the_log_of_the_issue_day_flow(self):
        index = pd.date_range("2020-01-01", periods=10, freq="1D")
        daily = pd.DataFrame({"q_mm": np.arange(1.0, 11.0)}, index=index)
        with tempfile.TemporaryDirectory() as arms_dir:
            pd.to_pickle({"daily": {"a": daily}, "scale": {"a": 1.0}},
                         os.path.join(arms_dir, simulation.DAILY))
            simulation._daily_cache.cache_clear()
            data = pd.DataFrame({"site_id": ["a", "zz"], "date": [index[3], index[3]]})
            block = simulation.depth_block(data, arms_dir)
        self.assertAlmostEqual(float(block[0, 0]), np.log(4.0 + behaviour.FLOW_FLOOR_MM), places=5)
        self.assertTrue(np.isnan(block[1, 0]))

    def test_a_flat_simulation_predicts_no_change(self):
        index = pd.date_range("2020-01-01", periods=30, freq="1D")
        frame = simulation.basin_features(pd.Series(0.3, index=index), pd.Series(1.0, index=index), 1.0)
        self.assertAlmostEqual(float(frame["sim_dlog_day4-7"].iloc[3]), 0.0, places=6)
        self.assertAlmostEqual(float(frame["sim_add_day4-7"].iloc[3]), 0.0, places=6)

    def test_feature_block_joins_rows_and_leaves_unsimulated_basins_missing(self):
        index = pd.date_range("2020-01-01", periods=40, freq="1D")
        level = pd.Series(np.arange(1.0, 41.0), index=index)
        with tempfile.TemporaryDirectory() as arms_dir:
            pd.to_pickle({"sim": {"a": np.log(level)}}, os.path.join(arms_dir, "sim_x.pkl"))
            daily = pd.DataFrame({"q_mm": level.to_numpy()}, index=index)
            pd.to_pickle({"daily": {"a": daily, "b": daily}, "scale": {"a": 0.5, "b": 0.5}},
                         os.path.join(arms_dir, simulation.DAILY))
            data = pd.DataFrame({"site_id": ["a", "b", "a"], "q0": [3.0, 3.0, 5.0],
                                 "date": [index[9], index[9], index[19]]})
            simulation._daily_cache.cache_clear()
            block = simulation.feature_block(data, arms_dir, "x")
        self.assertEqual(block.shape, (3, len(simulation.FEATURES)))
        self.assertTrue(np.isnan(block[1]).all())
        dlog = block[0, simulation.FEATURES.index("sim_dlog_day1-3")]
        self.assertAlmostEqual(float(dlog), np.log(1.2), places=5)
        self.assertAlmostEqual(float(block[0, simulation.FEATURES.index("sim_dq_day1-3")]),
                               3.0 * 0.2, places=4)
        self.assertAlmostEqual(float(block[2, simulation.FEATURES.index("sim_dlog_day1-3")]),
                               np.log(22.0 / 20.0), places=5)


class TestOtherYearCodes(unittest.TestCase):
    def test_a_years_code_does_not_depend_on_that_years_flow(self):
        torch.manual_seed(0)
        reader, _ = behaviour.build_models(code_dim=4, dim=16, depth=1, hidden=8, dropout=0.0)
        daily = {"a": record("2010-01-01", "2016-12-31", seed=1)}
        years = behaviour.water_years(daily["a"])
        self.assertGreaterEqual(len(years), 4)
        codes = simulation.other_year_codes(reader, daily, ["a", "missing"], n_read=3, passes=2)
        self.assertEqual(sorted(codes["a"]), years)
        changed = {"a": daily["a"].copy()}
        year = years[2]
        inside = (changed["a"].index >= year) & (changed["a"].index < year + pd.Timedelta(days=365))
        changed["a"].loc[inside, "q_mm"] *= 7.0
        again = simulation.other_year_codes(reader, changed, ["a"], n_read=3, passes=2)
        np.testing.assert_allclose(codes["a"][year].numpy(), again["a"][year].numpy(), atol=1e-6)
        self.assertGreater(float((codes["a"][years[0]] - again["a"][years[0]]).abs().max()), 1e-5)


class TestOtherCodes(unittest.TestCase):
    def test_other_state_codes_come_from_another_state_of_the_same_split(self):
        sites = ["a%d" % i for i in range(6)] + ["b%d" % i for i in range(6)]
        split = {site: "train" if int(site[1]) < 4 else "holdout" for site in sites}
        state = {site: site[0] for site in sites}
        codes = pd.DataFrame({"value": np.arange(len(sites), dtype=float)}, index=sites)
        moved = simulation.shuffle_within_split(codes, split, 0, apart=state)
        for site in sites:
            donor = codes.index[int(moved.loc[site, "value"])]
            self.assertNotEqual(state[donor], state[site])
            self.assertEqual(split[donor], split[site])

    def test_other_state_refuses_a_split_with_a_single_state(self):
        codes = pd.DataFrame({"value": [0.0, 1.0]}, index=["a0", "a1"])
        with self.assertRaises(ValueError):
            simulation.shuffle_within_split(codes, {"a0": "train", "a1": "train"}, 0,
                                            apart={"a0": "CO", "a1": "CO"})

    def test_donor_refuses_a_basin_without_a_location(self):
        location = pd.DataFrame({"x": [0.0], "y": [0.0]}, index=["t0"])
        codes = pd.DataFrame({"value": [1.0, 2.0]}, index=["t0", "h0"])
        with self.assertRaises(ValueError):
            simulation.nearest_training_codes(codes, location, ["t0"])

    def test_donor_is_the_nearest_training_basin_other_than_itself(self):
        location = pd.DataFrame({"x": [0.0, 1.0, 5.0, 5.2], "y": [0.0, 0.0, 0.0, 0.0]},
                                index=["t0", "t1", "t2", "h0"])
        codes = pd.DataFrame({"value": [10.0, 11.0, 12.0, 13.0]}, index=location.index)
        borrowed = simulation.nearest_training_codes(codes, location, ["t0", "t1", "t2"])
        self.assertEqual(list(borrowed["value"]), [11.0, 10.0, 11.0, 12.0])


class TestShuffle(unittest.TestCase):
    def test_every_basin_gets_another_basin_of_its_split(self):
        sites = ["t%d" % i for i in range(7)] + ["h%d" % i for i in range(4)]
        split = {site: "train" if site.startswith("t") else "holdout" for site in sites}
        codes = pd.DataFrame({"value": np.arange(len(sites), dtype=float)}, index=sites)
        for seed in range(5):
            shuffled = simulation.shuffle_within_split(codes, split, seed)
            for site in sites:
                donor = codes.index[int(shuffled.loc[site, "value"])]
                self.assertNotEqual(donor, site)
                self.assertEqual(split[donor], split[site])
            self.assertEqual(sorted(shuffled["value"]), sorted(codes["value"]))


if __name__ == "__main__":
    unittest.main()
