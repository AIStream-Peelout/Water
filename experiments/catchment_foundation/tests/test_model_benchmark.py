"""Tests of the adapter that puts Flow Forecast's models on the forecast windows."""
import os
import sys
import unittest

import torch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

from experiments.catchment_foundation import model_benchmark as benchmark  # noqa: E402


class TestWindowForecaster(unittest.TestCase):
    def test_every_model_forecasts_the_horizon_and_trains(self):
        torch.manual_seed(0)
        length, horizon = 42, 14
        drivers, code = torch.randn(6, length, 9), torch.randn(6, 4)
        for name in benchmark.MODELS:
            for code_dim in (0, 4):
                model = benchmark.build(name, length, horizon, code_dim=code_dim, width=16)
                output = model(drivers, code)
                self.assertEqual(output.shape, (6, length), name)
                self.assertEqual(float(output[:, :-horizon].abs().sum()), 0.0, name)
                output[:, -horizon:].pow(2).mean().backward()
                moved = sum(float(parameter.grad.abs().sum()) for parameter in model.parameters()
                            if parameter.grad is not None)
                self.assertGreater(moved, 0.0, name)

    def test_the_observed_flow_channel_is_placed_first(self):
        captured = {}

        class Probe(torch.nn.Module):
            def forward(self, window):
                captured["window"] = window
                return window[:, -14:, 0]

        drivers = torch.randn(2, 30, 9)
        model = benchmark.WindowForecaster(Probe(), 14, code_dim=3)
        model(drivers, torch.ones(2, 3))
        self.assertEqual(captured["window"].shape, (2, 30, 12))
        torch.testing.assert_close(captured["window"][:, :, 0], drivers[:, :, 5])
        torch.testing.assert_close(captured["window"][:, :, 1], drivers[:, :, 0])
        self.assertEqual(float(captured["window"][:, :, 9:].min()), 1.0)

    def test_unknown_model_is_refused(self):
        with self.assertRaises(ValueError):
            benchmark.build("NoSuchModel", 42, 14)


if __name__ == "__main__":
    unittest.main()
