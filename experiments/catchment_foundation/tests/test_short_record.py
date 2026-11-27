"""Tests of the short-record descriptions (short_record.py)."""
import os
import sys
import unittest

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

from experiments.catchment_foundation import pretrain_behaviour_code as behaviour  # noqa: E402
from experiments.catchment_foundation import short_record as short  # noqa: E402

WIDTH = 2 * len(short.NUMBERS) + short.EXTRAS


def basin(start: str = "2012-01-01", end: str = "2019-12-31", seed: int = 0) -> pd.DataFrame:
    index = pd.date_range(start, end, freq="1D")
    rng = np.random.default_rng(seed)
    season = 1.0 + np.sin(2 * np.pi * index.dayofyear / 365.25)
    return pd.DataFrame({"q_mm": season * rng.gamma(2.0, 1.0, len(index)), "p": rng.gamma(0.5, 4.0, len(index)),
                         "t": 8.0 + 10.0 * np.cos(2 * np.pi * index.dayofyear / 365.25),
                         "pet": rng.gamma(2.0, 1.0, len(index))}, index=index).astype(np.float32)


def seasonal(seed: int = 0) -> pd.DataFrame:
    """A gauge that reports from April to October only."""
    frame = basin(seed=seed)
    frame.loc[~frame.index.month.isin(range(4, 11)), "q_mm"] = np.nan
    return frame


class TestReadBlocks(unittest.TestCase):
    def setUp(self):
        self.daily = basin()
        self.last = self.daily.index.get_loc(pd.Timestamp("2018-06-30"))

    def test_only_the_record_is_shown(self):
        blocks = short.read_blocks(self.daily, self.last, 3)
        self.assertEqual(blocks.shape, (3, 365, behaviour.HISTORY_CHANNELS))
        rows = blocks.reshape(-1, behaviour.HISTORY_CHANNELS)
        days = short.visible_days(3)
        np.testing.assert_array_equal(rows[:-days], 0.0)             # no flow, no weather before it
        np.testing.assert_array_equal(rows[-days:, 1], 1.0)
        window = self.daily.iloc[self.last - days + 1:self.last + 1]
        np.testing.assert_allclose(rows[-days:, 0], behaviour.flow_channel(window), atol=1e-6)
        np.testing.assert_allclose(rows[-days:, 2:], behaviour.weather_channels(window), atol=1e-6)

    def test_all_weather_shows_three_years_of_weather_but_not_of_flow(self):
        rows = short.read_blocks(self.daily, self.last, 3, all_weather=True).reshape(-1, 7)
        whole = self.daily.iloc[self.last - 3 * 365 + 1:self.last + 1]
        np.testing.assert_allclose(rows[:, 2:], behaviour.weather_channels(whole), atol=1e-6)
        np.testing.assert_array_equal(rows[:-short.visible_days(3), :2], 0.0)

    def test_a_record_of_zero_months_shows_no_flow(self):
        rows = short.read_blocks(self.daily, self.last, 0, all_weather=True).reshape(-1, 7)
        np.testing.assert_array_equal(rows[:, :2], 0.0)
        self.assertGreater(float(np.abs(rows[:, 2:]).sum()), 0.0)

    def test_nothing_outside_the_record_changes_either_description(self):
        days = short.visible_days(6)
        changed = self.daily.copy()
        outside = np.ones(len(changed), dtype=bool)
        outside[self.last - days + 1:self.last + 1] = False
        changed.loc[outside, ["q_mm", "p", "t", "pet"]] *= 11.0
        np.testing.assert_array_equal(short.read_blocks(self.daily, self.last, 6),
                                      short.read_blocks(changed, self.last, 6))
        scale = short.number_scale({"a": self.daily}, ["a"])
        record = slice(self.last - days + 1, self.last + 1)
        np.testing.assert_array_equal(short.number_vector(self.daily.iloc[record], scale, 6),
                                      short.number_vector(changed.iloc[record], scale, 6))

    def test_a_frame_shorter_than_three_years_is_padded(self):
        blocks = short.read_blocks(self.daily, 399, 1).reshape(-1, behaviour.HISTORY_CHANNELS)
        np.testing.assert_array_equal(blocks[:3 * 365 - 400], 0.0)
        self.assertEqual(float(blocks[-1, 1]), 1.0)


class TestShortNumbers(unittest.TestCase):
    def test_a_month_supports_some_numbers_and_flags_the_rest(self):
        daily = basin()
        month = daily["2018-06-01":"2018-06-30"]
        numbers = short.short_numbers(month)
        self.assertEqual(sorted(numbers), sorted(short.NUMBERS))
        self.assertAlmostEqual(numbers["log_runoff_mm"], float(np.log(month.q_mm.mean() + 1e-3)), places=5)
        for name in ("half_flow_day", "melt_share", "low_flow"):
            self.assertTrue(np.isnan(numbers[name]), name)
        scale = short.number_scale({"a": daily}, ["a"])
        vector = short.number_vector(month, scale, 1)
        self.assertEqual(vector.shape, (WIDTH,))
        flags = vector[len(short.NUMBERS):2 * len(short.NUMBERS)]
        self.assertEqual(float(flags[short.NUMBERS.index("half_flow_day")]), 0.0)
        self.assertEqual(float(flags[short.NUMBERS.index("log_runoff_mm")]), 1.0)
        self.assertEqual(float(vector[short.NUMBERS.index("half_flow_day")]), 0.0)

    def test_the_vector_carries_length_coverage_and_season(self):
        daily = basin()
        scale = short.number_scale({"a": daily}, ["a"])
        june = short.number_vector(daily["2018-06-01":"2018-06-30"], scale, 1)[-short.EXTRAS:]
        year = short.number_vector(daily["2017-12-31":"2018-12-30"], scale, 12)[-short.EXTRAS:]
        self.assertLess(float(june[0]), float(year[0]))                 # record length
        self.assertAlmostEqual(float(june[1]), 1.0, places=6)           # every day has flow
        angle = 2 * np.pi * pd.Timestamp("2018-06-30").dayofyear / 365.25
        self.assertAlmostEqual(float(june[2]), np.sin(angle), places=5)
        self.assertAlmostEqual(float(june[3]), np.cos(angle), places=5)

    def test_three_months_can_show_a_storm_response(self):
        daily = basin()                    # this synthetic basin is warm around the new year
        record = daily["2017-12-01":"2018-02-28"]
        self.assertTrue(np.isfinite(short.short_numbers(record)["storm_response"]))

    def test_half_flow_day_needs_no_calendar_alignment(self):
        index = pd.date_range("2017-04-01", "2018-03-31", freq="1D")      # not a water year
        flow = np.where((index.month == 6), 10.0, 0.01)                    # all the flow in June
        record = pd.DataFrame({"q_mm": flow, "p": 1.0, "t": 10.0, "pet": 1.0}, index=index)
        day = short.short_numbers(record)["half_flow_day"]
        first, last = (pd.Timestamp("2017-06-01") - pd.Timestamp("2016-10-01")).days + 1, \
                      (pd.Timestamp("2017-06-30") - pd.Timestamp("2016-10-01")).days + 1
        self.assertTrue(first <= day <= last, day)

    def test_a_whole_record_supports_every_number(self):
        numbers = short.short_numbers(basin())
        self.assertTrue(all(np.isfinite(value) for value in numbers.values()), numbers)

    def test_an_empty_record_gives_nothing(self):
        daily = basin()["2018-06-01":"2018-06-30"].assign(q_mm=np.nan)
        self.assertTrue(all(np.isnan(value) for value in short.short_numbers(daily).values()))


class TestDataset(unittest.TestCase):
    def test_the_decoded_year_lies_outside_the_record(self):
        daily = {"a": basin(seed=1), "b": basin(seed=2), "two": basin("2016-06-01", "2019-12-31", seed=3)}
        scale = short.number_scale(daily, ["a", "b"])
        dataset = short.ShortRecordDataset(daily, list(daily), scale, draws=40, seed=3, fixed=True)
        for index in range(len(dataset)):
            site, months, last, decoded = dataset.choose(index)
            start = daily[site].index[max(last - short.visible_days(months) + 1, 0)]
            end = daily[site].index[last]
            decoded_end = decoded + pd.Timedelta(days=behaviour.YEAR_DAYS - 1)
            self.assertTrue(decoded > end or decoded_end < start, (site, months, start, end, decoded))
        item = dataset[0]
        self.assertEqual(item["history"].shape, (3, 365, behaviour.HISTORY_CHANNELS))
        self.assertEqual(item["statics"].shape, (WIDTH,))
        self.assertEqual(item["drivers"].shape, (behaviour.SPIN_UP_DAYS + behaviour.YEAR_DAYS, 5))
        self.assertTrue(torch.isnan(item["target"][:behaviour.SPIN_UP_DAYS]).all())

    def test_records_are_mostly_observed_even_for_a_seasonal_gauge(self):
        daily = {"a": seasonal(seed=1)}
        scale = short.number_scale(daily, ["a"])
        dataset = short.ShortRecordDataset(daily, ["a"], scale, months=(1, 3), draws=60, seed=3, fixed=True)
        shares = []
        for index in range(len(dataset)):
            site, months, last, _ = dataset.choose(index)
            days = short.visible_days(months)
            shares.append(daily[site].q_mm.iloc[last - days + 1:last + 1].notna().mean())
        self.assertGreaterEqual(min(shares), short.MIN_VISIBLE_SHARE - 1e-9)

    def test_fixed_draws_repeat_and_a_run_builds_only_its_own_input(self):
        daily = {"a": basin(seed=1)}
        scale = short.number_scale(daily, ["a"])
        dataset = short.ShortRecordDataset(daily, ["a"], scale, draws=5, seed=3, fixed=True,
                                           inputs=("statics",))
        self.assertEqual(dataset.choose(2), dataset.choose(2))
        self.assertNotIn("history", dataset[0])

    def test_a_record_of_zero_months_can_be_served(self):
        daily = {"a": basin(seed=1)}
        scale = short.number_scale(daily, ["a"])
        dataset = short.ShortRecordDataset(daily, ["a"], scale, months=(0,), draws=3, seed=3, fixed=True,
                                           inputs=("history",), all_weather=True)
        self.assertEqual(float(dataset[0]["history"][..., :2].abs().sum()), 0.0)


class TestAnchorAndCodes(unittest.TestCase):
    def test_a_winter_end_day_is_moved_back_to_a_record_with_flow(self):
        frame = seasonal(seed=1)
        last = short.record_anchor(frame, "2018-12-31", 1)
        days = short.visible_days(1)
        self.assertLessEqual(frame.index[last], pd.Timestamp("2018-10-31"))
        self.assertGreaterEqual(frame.q_mm.iloc[last - days + 1:last + 1].notna().mean(),
                                short.MIN_VISIBLE_SHARE)
        whole = basin(seed=1)
        self.assertEqual(whole.index[short.record_anchor(whole, "2018-12-31", 1)], pd.Timestamp("2018-12-31"))
        self.assertIsNone(short.record_anchor(whole.assign(q_mm=np.nan), "2018-12-31", 1))
        self.assertIsNone(short.record_anchor(whole, "2011-01-01", 1))

    def test_nothing_after_the_end_day_is_read(self):
        torch.manual_seed(0)
        daily = {"a": basin(seed=1), "b": seasonal(seed=2), "late": basin(start="2019-06-01", seed=3)}
        scale = short.number_scale(daily, ["a", "b"])
        changed = {site: frame.copy() for site, frame in daily.items()}
        for frame in changed.values():
            frame.loc[frame.index > pd.Timestamp("2018-12-31"), ["q_mm", "p"]] *= 13.0
        for kind in short.KINDS:
            reader, _ = short.build_models(kind, code_dim=4, dim=16, depth=1, hidden=8, dropout=0.0)
            first = short.write_codes(reader, kind, daily, list(daily), "2018-12-31", 6, scale)
            second = short.write_codes(reader, kind, changed, list(daily), "2018-12-31", 6, scale)
            self.assertEqual(list(first.index), ["a", "b"])        # "late" starts after the end
            np.testing.assert_allclose(first.to_numpy(), second.to_numpy(), atol=1e-6)
            other = short.write_codes(reader, kind, daily, ["a"], "2018-12-31", 1, scale)
            self.assertGreater(float(np.abs(other.to_numpy() - first.loc[["a"]].to_numpy()).max()), 1e-6)
        self.assertTrue(short.write_codes(reader, "record", daily, ["late"], "2018-12-31", 6, scale).empty)


if __name__ == "__main__":
    unittest.main()
