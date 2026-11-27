"""
Flow Forecast's sequence models on one forecast task, built so they can be compared.

``decoder_forecaster.py train --model <name>`` trains any of the models below on exactly the
windows, target, loss and training loop of the GRU forecaster, and scores it on the same rows.
A window is the days up to the issue day followed by the 14 days after it; every model gets the
whole window (the realized weather of the horizon included, the flow after the issue day
hidden) and writes the 14 daily changes in flow directly.

What this module adds is only the wiring: channel order, output shape and, for a model whose
head writes one value, a head that writes 14. The models themselves are Flow Forecast's.
"""
from typing import Callable, Dict, Optional

import torch
from torch import nn

FLOW_FIRST = [5, 0, 1, 2, 3, 4, 6, 7, 8]      # the observed-flow channel leads, as the models expect


class WindowForecaster(nn.Module):
    """
    Adapts a Flow Forecast sequence model to the forecast windows.

    The wrapped model reads (batch, window length, channels) with the observed flow as the
    first channel and returns the 14 days after the issue day. A basin code, when the run has
    a description, is repeated along the window as extra channels.
    """

    def __init__(self, model: nn.Module, horizon: int, code_dim: int = 0,
                 call: Optional[Callable] = None):
        """
        Initializes the adapter.

        :param model: A Flow Forecast model.
        :type model: nn.Module
        :param horizon: Days forecast.
        :type horizon: int
        :param code_dim: Width of the basin code appended as channels; zero for none.
        :type code_dim: int, optional
        :param call: How to call the model on the window when it needs more than the window
            itself; defaults to ``model(window)``.
        :type call: Callable, optional
        """
        super().__init__()
        self.model, self.horizon, self.code_dim, self.call = model, horizon, code_dim, call

    def forward(self, drivers: torch.Tensor, code: torch.Tensor) -> torch.Tensor:
        """
        Forecasts the horizon.

        :param drivers: Windows of shape (batch, window length, 9) as ``ForecastWindows`` serves.
        :type drivers: torch.Tensor
        :param code: Basin codes of shape (batch, code width); ignored when ``code_dim`` is zero.
        :type code: torch.Tensor
        :return: Shape (batch, window length): zeros up to the issue day, then the forecast.
        :rtype: torch.Tensor
        """
        window = drivers[:, :, FLOW_FIRST]
        if self.code_dim:
            window = torch.cat([window, code.unsqueeze(1).expand(-1, window.shape[1], -1)], dim=-1)
        output = self.call(self.model, window) if self.call else self.model(window)
        if output.dim() == 3:
            output = output[..., 0]
        history = torch.zeros(output.shape[0], drivers.shape[1] - self.horizon,
                              dtype=output.dtype, device=output.device)
        return torch.cat([history, output[:, -self.horizon:]], dim=1)


def _darnn(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.da_rnn.model import DARNN
    # Its window is one step shorter than ``forecast_history``; its head writes one value per
    # target column, so the head is replaced by one writing the horizon.
    model = DARNN(channels, width, length + 1, width, out_feats=1, dropout=0.1)
    model.decoder.fc_final = nn.Linear(model.decoder.fc_final.in_features, horizon)
    return model


def _lstm(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.basic.lstm_vanilla import LSTMForecast
    return LSTMForecast(length, channels, output_seq_len=horizon, hidden_states=width, num_layers=2)


def _gru(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.basic.gru_vanilla import VanillaGRU
    return VanillaGRU(channels, width, 2, 1, 0.1, forecast_length=horizon)


def _transformer(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.transformer_xl.transformer_basic import CustomTransformerDecoder
    return CustomTransformerDecoder(length, horizon, channels, d_model=width, n_layers_encoder=2,
                                    forward_dim=2 * width, n_heads=4)


def _multi_head(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.transformer_xl.multi_head_base import MultiAttnHeadSimple
    return MultiAttnHeadSimple(channels, seq_len=length, output_seq_len=horizon, d_model=width,
                               num_heads=4)


def _decoder_transformer(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.transformer_xl.transformer_bottleneck import DecoderTransformer
    return DecoderTransformer(channels, 4, 2, width, length, 0.1, 1, {}, forecast_length=horizon)


def _crossformer(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.transformer_xl.cross_former import Crossformer
    return Crossformer(channels, length, horizon, seg_len=7, win_size=2, factor=5, d_model=width,
                       d_ff=2 * width, n_heads=4, e_layers=2, dropout=0.1, n_targs=1,
                       device=torch.device("cpu"))


def _itransformer(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.transformer_xl.itransformer import ITransformer
    return ITransformer(length, horizon, width, "fixed", 0.1, n_heads=4, e_layers=2,
                        d_ff=2 * width, targs=1)


def _dlinear(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.basic.d_n_linear import DLinear
    return DLinear(length, horizon, False, channels, n_targs=1)


def _nlinear(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.basic.d_n_linear import NLinear
    return NLinear(length, horizon, enc_in=channels, n_targs=1)


def _tsmixer(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from torchtsmixer import TSMixer
    return TSMixer(length, horizon, channels, output_channels=1, ff_dim=width)


def _linear(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.basic.linear_regression import SimpleLinearModel
    return SimpleLinearModel(length, channels, output_seq_len=horizon)


def _narx(channels: int, length: int, horizon: int, width: int) -> nn.Module:
    from flood_forecast.basic.narx import NARX
    return NARX(channels, length, output_seq_len=horizon, hidden_size=width)


# name -> (builder, learning rate, how to call it on a window when not ``model(window)``)
MODELS: Dict[str, tuple] = {
    "DARNN": (_darnn, 1e-3, None),
    "LSTM": (_lstm, 1e-3, None),
    "VanillaGRU": (_gru, 1e-3, None),
    "CustomTransformerDecoder": (_transformer, 3e-4, None),
    "MultiAttnHeadSimple": (_multi_head, 3e-4, None),
    "DecoderTransformer": (_decoder_transformer, 3e-4, None),
    "Crossformer": (_crossformer, 3e-4, None),
    "ITransformer": (_itransformer, 3e-4, lambda model, window: model(window, None, None, None)),
    "DLinear": (_dlinear, 1e-3, None),
    "NLinear": (_nlinear, 1e-3, None),
    "TSMixer": (_tsmixer, 1e-3, None),
    "SimpleLinearModel": (_linear, 1e-3, None),
    "NARX": (_narx, 1e-3, None),
}


def build(name: str, length: int, horizon: int, channels: int = 9, code_dim: int = 0,
          width: int = 64) -> WindowForecaster:
    """
    Builds one Flow Forecast model behind the window adapter.

    :param name: A key of ``MODELS``.
    :type name: str
    :param length: Window length in days (history plus horizon).
    :type length: int
    :param horizon: Days forecast.
    :type horizon: int
    :param channels: Channels of the window, defaults to 9.
    :type channels: int, optional
    :param code_dim: Width of the basin code appended as channels; zero for none.
    :type code_dim: int, optional
    :param width: Hidden width given to the model, defaults to 64.
    :type width: int, optional
    :return: The adapted model.
    :rtype: WindowForecaster
    """
    if name not in MODELS:
        raise ValueError("unknown model %s; choose from %s" % (name, sorted(MODELS)))
    builder, _, call = MODELS[name]
    return WindowForecaster(builder(channels + code_dim, length, horizon, width), horizon,
                            code_dim, call)


def learning_rate(name: str) -> float:
    """The learning rate a model is benchmarked with."""
    return MODELS[name][1]
