import unittest

import numpy as np
import pandas as pd

from pet_functions import extraterrestrial_radiation, hargreaves_pet, daylight_weights, add_hargreaves_pet


class TestPetFunctions(unittest.TestCase):
    """Offline tests for the Hargreaves-Samani PET utilities."""

    def test_extraterrestrial_radiation_reference_value(self):
        # FAO-56 example 8: latitude -20 deg, 3 September (doy 246) gives Ra ~= 32.2 MJ/m2/day
        # which is ~13.1 mm/day water equivalent.
        ra = extraterrestrial_radiation(-20.0, np.array([246]))
        self.assertAlmostEqual(float(ra[0]), 13.1, delta=0.3)

    def test_radiation_seasonality_northern_hemisphere(self):
        summer = extraterrestrial_radiation(40.0, np.array([172]))
        winter = extraterrestrial_radiation(40.0, np.array([355]))
        self.assertGreater(float(summer[0]), float(winter[0]))

    def test_hargreaves_positive_and_scales_with_temperature(self):
        cool = hargreaves_pet(5.0, 15.0, 40.0, np.array([180]))
        warm = hargreaves_pet(15.0, 30.0, 40.0, np.array([180]))
        self.assertGreater(float(cool[0]), 0.0)
        self.assertGreater(float(warm[0]), float(cool[0]))

    def test_hargreaves_handles_inverted_range(self):
        pet = hargreaves_pet(20.0, 20.0, 40.0, np.array([180]))
        self.assertEqual(float(pet[0]), 0.0)

    def test_daylight_weights_sum_to_one_and_zero_at_night(self):
        weights = daylight_weights(40.0, 172)
        self.assertAlmostEqual(float(weights.sum()), 1.0, places=6)
        self.assertEqual(float(weights[0]), 0.0)  # midnight
        self.assertGreater(float(weights[12]), 0.0)  # noon

    def test_add_hargreaves_pet_hourly(self):
        times = pd.date_range("2023-06-01", periods=48, freq="h")
        temps_c = 20.0 + 8.0 * np.sin(np.pi * (times.hour - 6) / 12.0)
        df = pd.DataFrame({"hour_updated": times, "tmpf": temps_c * 9.0 / 5.0 + 32.0})
        result = add_hargreaves_pet(df, latitude=40.0)
        self.assertIn("pet_mm_hr", result.columns)
        daily_total = result["pet_mm_hr"].iloc[:24].sum()
        self.assertGreater(daily_total, 1.0)
        self.assertLess(daily_total, 15.0)
        # Nighttime hours should get (near) zero PET under the daylight distribution.
        self.assertAlmostEqual(result["pet_mm_hr"].iloc[0], 0.0, places=6)

    def test_add_pet_uniform_distribution(self):
        times = pd.date_range("2023-06-01", periods=24, freq="h")
        df = pd.DataFrame({"hour_updated": times, "tmpf": np.full(24, 70.0)})
        result = add_hargreaves_pet(df, latitude=40.0, distribution="uniform")
        self.assertAlmostEqual(result["pet_mm_hr"].std(), 0.0, places=8)


if __name__ == "__main__":
    unittest.main()
