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


def basin(start: str = "2012-01-01", end: str = "2019-12-31", seed: int = 0) -> pd.DataFrame:
    index = pd.date_range(start, end, freq="1D")
    rng = np.random.default_rng(seed)
    season = 1.0 + np.sin(2 * np.pi * index.dayofyear / 365.25)
    return pd.DataFrame({"q_mm": season * rng.gamma(2.0, 1.0, len(index)), "p": rng.gamma(0.5, 4.0, len(index)),
                         "t": 8.0 + 10.0 * np.cos(2 * np.pi * index.dayofyear / 365.25),
                         "pet": rng.gamma(2.0, 1.0, len(index))}, index=index).astype(np.float32)


class TestReadBlocks(unittest.TestCase):
    def setUp(self):
        self.daily = basin()
        self.last = self.daily.index.get_loc(pd.Timestamp("2018-06-30"))

    def test_only_the_records_flow_is_shown(self):
        blocks = short.read_blocks(self.daily, self.last, 3)
        self.assertEqual(blocks.shape, (3, 365, behaviour.HISTORY_CHANNELS))
        rows = blocks.reshape(-1, behaviour.HISTORY_CHANNELS)
        days = short.visible_days(3)
        np.testing.assert_array_equal(rows[:-days, 1], 0.0)
        np.testing.assert_array_equal(rows[:-days, 0], 0.0)
        np.testing.assert_array_equal(rows[-days:, 1], 1.0)
        window = self.daily.iloc[self.last - days + 1:self.last + 1]
        np.testing.assert_allclose(rows[-days:, 0], behaviour.flow_channel(window), atol=1e-6)
        whole = self.daily.iloc[self.last - 3 * 365 + 1:self.last + 1]
        np.testing.assert_allclose(rows[:, 2:], behaviour.weather_channels(whole), atol=1e-6)

    def test_flow_outside_the_record_changes_nothing(self):
        days = short.visible_days(6)
        changed = self.daily.copy()
        outside = np.ones(len(changed), dtype=bool)
        outside[self.last - days + 1:self.last + 1] = False
        changed.loc[outside, "q_mm"] *= 11.0
        np.testing.assert_array_equal(short.read_blocks(self.daily, self.last, 6),
                                      short.read_blocks(changed, self.last, 6))
        scale = short.number_scale({"a": self.daily}, ["a"])
        record = slice(self.last - days + 1, self.last + 1)
        np.testing.assert_array_equal(short.number_vector(self.daily.iloc[record], scale),
                                      short.number_vector(changed.iloc[record], scale))

    def test_a_frame_shorter_than_three_years_is_padded(self):
        blocks = short.read_blocks(self.daily, 399, 1).reshape(-1, behaviour.HISTORY_CHANNELS)
        np.testing.assert_array_equal(blocks[:3 * 365 - 400], 0.0)
        self.assertEqual(float(blocks[-1, 1]), 1.0)

    def test_a_full_length_record_shows_every_flow(self):
        blocks = short.read_blocks(self.daily, self.last, 36).reshape(-1, behaviour.HISTORY_CHANNELS)
        self.assertEqual(float(blocks[:, 1].mean()), 1.0)


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
        vector = short.number_vector(month, scale)
        self.assertEqual(vector.shape, (2 * len(short.NUMBERS),))
        flags = vector[len(short.NUMBERS):]
        self.assertEqual(float(flags[short.NUMBERS.index("half_flow_day")]), 0.0)
        self.assertEqual(float(flags[short.NUMBERS.index("log_runoff_mm")]), 1.0)
        self.assertEqual(float(vector[short.NUMBERS.index("half_flow_day")]), 0.0)

    def test_a_whole_record_supports_every_number(self):
        numbers = short.short_numbers(basin())
        self.assertTrue(all(np.isfinite(value) for value in numbers.values()), numbers)

    def test_an_empty_record_gives_nothing(self):
        daily = basin()["2018-06-01":"2018-06-30"].assign(q_mm=np.nan)
        self.assertTrue(all(np.isnan(value) for value in short.short_numbers(daily).values()))


class TestDataset(unittest.TestCase):
    def test_the_decoded_year_lies_outside_the_record(self):
        daily = {"a": basin(seed=1), "b": basin(seed=2)}
        scale = short.number_scale(daily, ["a", "b"])
        dataset = short.ShortRecordDataset(daily, ["a", "b"], scale, draws=40, seed=3, fixed=True)
        for index in range(len(dataset)):
            site, months, last, decoded = dataset.choose(index)
            start = daily[site].index[max(last - short.visible_days(months) + 1, 0)]
            end = daily[site].index[last]
            decoded_end = decoded + pd.Timedelta(days=behaviour.YEAR_DAYS - 1)
            self.assertTrue(decoded > end or decoded_end < start, (months, start, end, decoded))
        item = dataset[0]
        self.assertEqual(item["history"].shape, (3, 365, behaviour.HISTORY_CHANNELS))
        self.assertEqual(item["statics"].shape, (2 * len(short.NUMBERS),))
        self.assertEqual(item["drivers"].shape, (behaviour.SPIN_UP_DAYS + behaviour.YEAR_DAYS, 5))
        self.assertTrue(torch.isnan(item["target"][:behaviour.SPIN_UP_DAYS]).all())

    def test_fixed_draws_repeat(self):
        daily = {"a": basin(seed=1)}
        scale = short.number_scale(daily, ["a"])
        dataset = short.ShortRecordDataset(daily, ["a"], scale, draws=5, seed=3, fixed=True)
        self.assertEqual(dataset.choose(2), dataset.choose(2))


class TestWriteCodes(unittest.TestCase):
    def test_nothing_after_the_end_day_is_read(self):
        torch.manual_seed(0)
        daily = {"a": basin(seed=1), "b": basin(seed=2), "late": basin(start="2019-06-01", seed=3)}
        scale = short.number_scale(daily, ["a", "b"])
        changed = {site: frame.copy() for site, frame in daily.items()}
        for frame in changed.values():
            frame.loc[frame.index > pd.Timestamp("2018-12-31"), ["q_mm", "p"]] *= 13.0
        for kind in ("record", "numbers"):
            reader, _ = short.build_models(kind, code_dim=4, dim=16, depth=1, hidden=8, dropout=0.0)
            first = short.write_codes(reader, kind, daily, list(daily), "2018-12-31", 6, scale)
            second = short.write_codes(reader, kind, changed, list(daily), "2018-12-31", 6, scale)
            self.assertEqual(list(first.index), ["a", "b"])        # "late" starts after the end
            np.testing.assert_allclose(first.to_numpy(), second.to_numpy(), atol=1e-6)
            other = short.write_codes(reader, kind, daily, ["a"], "2018-12-31", 1, scale)
            self.assertGreater(float(np.abs(other.to_numpy() - first.loc[["a"]].to_numpy()).max()), 1e-6)


if __name__ == "__main__":
    unittest.main()
