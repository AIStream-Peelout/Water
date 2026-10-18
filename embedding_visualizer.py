"""
Interactive catchment embedding explorer, ported from the Cairo Genizah document visualizer
(AIStream-Peelout/historical-document-analysis src/embeddings/visualizations/document_visualizer.py).

Follows the same recipe as the Genizah tool: a Plotly scatter of t-SNE-reduced embeddings whose points
carry base64 thumbnails and full details in ``customdata``, a click popup that shows the full content,
and a self-contained HTML file that opens anywhere. Catchment adaptations: the document image becomes
the Sentinel-2 patch, the transcription becomes a rendered daily hydrograph, and each popup lists the
site's nearest neighbors in embedding space (click a neighbor to jump to it). A dropdown recolors the
points by cluster, mean basin elevation, snow fraction or log drainage area.

Example::

    python embedding_visualizer.py --data-dir pilot_data/embedding_dataset/CO
"""
import argparse
import base64
import io
import json
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE


class CatchmentVisualization(object):
    """Builds the interactive HTML explorer for trained catchment embeddings."""

    def __init__(self, output_dir: str = ".", thumbnail_size: int = 96, popup_image_size: int = 320):
        """
        Initializes the visualizer.

        :param output_dir: Directory the HTML file is written to, defaults to ".".
        :type output_dir: str, optional
        :param thumbnail_size: Pixel size of the popup Sentinel thumbnail source, defaults to 96.
        :type thumbnail_size: int, optional
        :param popup_image_size: Display size of the popup image in CSS pixels, defaults to 320.
        :type popup_image_size: int, optional
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.thumbnail_size = thumbnail_size
        self.popup_image_size = popup_image_size

    def reduce_dimensions(self, embeddings: np.ndarray, perplexity: int = 30) -> np.ndarray:
        """
        Reduces embeddings to 2D with t-SNE (perplexity adapted to small datasets, as in the
        Genizah visualizer).

        :param embeddings: The embedding matrix of shape (n_sites, dim).
        :type embeddings: np.ndarray
        :param perplexity: The t-SNE perplexity, defaults to 30.
        :type perplexity: int, optional
        :return: 2D coordinates of shape (n_sites, 2).
        :rtype: np.ndarray
        """
        if embeddings.shape[0] <= perplexity:
            perplexity = max(1, embeddings.shape[0] // 3)
        return TSNE(n_components=2, perplexity=perplexity,
                    random_state=42).fit_transform(embeddings)

    def sentinel_to_base64(self, image: np.ndarray) -> str:
        """
        Converts a (bands, H, W) Sentinel patch to a base64 RGB JPEG data URI.

        Assumes the band order of Water's embedding records (B02, B03, B04, ...) and uses a simple
        reflectance stretch (DN / 3000).

        :param image: The patch array of shape (bands, H, W).
        :type image: np.ndarray
        :return: A "data:image/jpeg;base64,..." string.
        :rtype: str
        """
        from PIL import Image
        rgb = np.clip(image[[2, 1, 0]].transpose(1, 2, 0) / 3000.0, 0.0, 1.0)
        pil = Image.fromarray((rgb * 255).astype(np.uint8))
        resample = Image.NEAREST if pil.size[0] <= self.popup_image_size else Image.BILINEAR
        pil = pil.resize((self.popup_image_size, self.popup_image_size), resample)
        buffer = io.BytesIO()
        pil.save(buffer, format="JPEG", quality=78)
        return "data:image/jpeg;base64," + base64.b64encode(buffer.getvalue()).decode()

    def hydrograph_to_base64(self, history: np.ndarray, history_start: str) -> str:
        """
        Renders a site's daily flow history to a small base64 PNG hydrograph.

        :param history: The daily flow array (NaN where missing).
        :type history: np.ndarray
        :param history_start: ISO date of the first value.
        :type history_start: str
        :return: A "data:image/png;base64,..." string.
        :rtype: str
        """
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        dates = pd.date_range(history_start, periods=len(history), freq="D")
        fig, ax = plt.subplots(figsize=(4.6, 1.6), dpi=90)
        ax.plot(dates, history, lw=0.4, color="#1f77b4")
        ax.set_yscale("symlog")
        ax.set_ylabel("cfs", fontsize=7)
        ax.tick_params(labelsize=6)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        fig.tight_layout(pad=0.3)
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png")
        plt.close(fig)
        return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode()

    def panel_to_base64(self, panel: np.ndarray, panel_types: np.ndarray,
                        panel_starts: np.ndarray) -> str:
        """
        Renders a panel record's canonical seasonal slices (plus flood and drought) as small
        multiples: one hourly 92-day hydrograph per slice type, log scale.

        :param panel: Slices of shape (n_slices, slice_hours) in cfs (NaN where missing).
        :type panel: np.ndarray
        :param panel_types: Slice type per row ("winter", ..., "flood", "drought").
        :type panel_types: np.ndarray
        :param panel_starts: Slice start timestamps per row.
        :type panel_starts: np.ndarray
        :return: A "data:image/png;base64,..." string.
        :rtype: str
        """
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        types = [str(t) for t in panel_types]
        order = ["winter", "spring", "summer", "fall", "flood", "drought"]
        rows = [(t, types.index(t)) for t in order if t in types]  # canonical = first of type
        fig, axes = plt.subplots(1, len(rows), figsize=(1.55 * len(rows), 1.7), dpi=90,
                                 sharey=True)
        for ax, (slice_type, row) in zip(np.atleast_1d(axes), rows):
            ax.plot(np.arange(panel.shape[1]) / 24.0, panel[row], lw=0.5,
                    color="#d62728" if slice_type == "flood" else
                    "#8c564b" if slice_type == "drought" else "#1f77b4")
            ax.set_yscale("log")
            ax.set_title("%s %s" % (slice_type, str(panel_starts[row])[:7]), fontsize=7)
            ax.tick_params(labelsize=6)
            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)
        np.atleast_1d(axes)[0].set_ylabel("cfs", fontsize=7)
        fig.tight_layout(pad=0.3)
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png")
        plt.close(fig)
        return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode()

    def create_visualization_dataframe(self, data_dir: str, site_ids: List[str],
                                       embeddings: np.ndarray, gauges_csv: Optional[str] = None,
                                       n_neighbors: int = 5, n_clusters: int = 5,
                                       signatures: Optional[pd.DataFrame] = None,
                                       split: Optional[Dict] = None,
                                       holdout_sites: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Assembles the per-site dataframe: coordinates, attributes, thumbnails and neighbors.

        :param data_dir: Directory with the <site>.npz embedding records (daily or panel).
        :type data_dir: str
        :param site_ids: The site ids aligned with the embedding rows.
        :type site_ids: List[str]
        :param embeddings: The embedding matrix of shape (n_sites, dim).
        :type embeddings: np.ndarray
        :param gauges_csv: Optional CSV with site_no/station_nm/dec_lat_va/dec_long_va for names,
            defaults to None.
        :type gauges_csv: str, optional
        :param n_neighbors: Nearest neighbors listed per site, defaults to 5.
        :type n_neighbors: int, optional
        :param n_clusters: k for the k-means cluster coloring, defaults to 5.
        :type n_clusters: int, optional
        :param signatures: Optional hydrologic-signature table indexed by site_id (the
            ``*_signatures.csv`` written by ``embedding_probes.py``), defaults to None.
        :type signatures: pd.DataFrame, optional
        :param split: Optional frozen basin split (``sites`` -> state/fold), defaults to None.
        :type split: Dict, optional
        :param holdout_sites: Sites the encoder did not train on, defaults to None.
        :type holdout_sites: List[str], optional
        :return: The visualization dataframe.
        :rtype: pd.DataFrame
        """
        normalized = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        xy = self.reduce_dimensions(normalized)
        clusters = KMeans(n_clusters=n_clusters, n_init=10, random_state=0).fit_predict(normalized)
        similarity = normalized @ normalized.T
        held = set(holdout_sites or [])
        split_sites = (split or {}).get("sites", {})

        names = {}
        if gauges_csv is not None and os.path.exists(gauges_csv):
            gauges = pd.read_csv(gauges_csv, dtype={"site_no": str})
            names = dict(zip(gauges["site_no"], gauges["station_nm"]))

        def signature(site: str, column: str) -> float:
            if signatures is None or site not in signatures.index or column not in signatures:
                return float("nan")
            return float(signatures.loc[site, column])

        rows = []
        for i, site in enumerate(site_ids):
            record = np.load(os.path.join(data_dir, site + ".npz"), allow_pickle=True)
            attr_names = [str(n) for n in record["static_names"]]
            static = record["static"]

            def attr(name: str) -> float:
                return float(static[attr_names.index(name)]) if name in attr_names else float("nan")

            order = np.argsort(-similarity[i])
            neighbors = [{"site": site_ids[j], "name": names.get(site_ids[j], ""),
                          "sim": round(float(similarity[i, j]), 3),
                          "state": split_sites.get(site_ids[j], {}).get("state", ""),
                          "melt": round(signature(site_ids[j], "melt_fraction"), 2)}
                         for j in order[1:n_neighbors + 1]]
            if "panel" in record.files:
                hydrograph = self.panel_to_base64(record["panel"], record["panel_types"],
                                                  record["panel_starts"])
            else:
                hydrograph = self.hydrograph_to_base64(record["history"],
                                                       str(record["history_start"]))
            regional = (self.sentinel_to_base64(record["image_regional"].astype(np.float32))
                        if "image_regional" in record.files else "")
            info = split_sites.get(site, {})
            rows.append({
                "site_no": site, "station_nm": names.get(site, site),
                "x": float(xy[i, 0]), "y": float(xy[i, 1]), "cluster": int(clusters[i]),
                "elev_m": attr("ELEV_MEAN_M_BASIN"), "snow_pct": attr("SNOW_PCT_PRECIP"),
                "drain_sqkm": attr("DRAIN_SQKM"), "slope_pct": attr("SLOPE_PCT"),
                "image_base64": self.sentinel_to_base64(record["image"]),
                "hydrograph_base64": hydrograph, "regional_base64": regional,
                "neighbors": neighbors,
                "state": info.get("state", ""), "fold": info.get("fold", -1),
                "holdout": site in held,
                "melt_fraction": signature(site, "melt_fraction"),
                "rb_flashiness": signature(site, "rb_flashiness"),
                "bfi": signature(site, "bfi"),
                "log_mean_flow": signature(site, "log_mean_flow"),
            })
        return pd.DataFrame(rows)

    def _popup_js(self) -> str:
        """
        Returns the click-popup JavaScript (the Genizah popup pattern with catchment content).

        :return: A <script> block string.
        :rtype: str
        """
        return """
<script>
(function() {
  var plots = document.getElementsByClassName('plotly-graph-div');
  if (!plots.length) return;
  var plotDiv = plots[0];
  window._siteIndex = window._siteIndex || {};
  function showPopup(cd) {
    var popup = document.getElementById('catchment-popup');
    if (!popup) {
      popup = document.createElement('div');
      popup.id = 'catchment-popup';
      popup.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);' +
        'background:white;border:2px solid #333;border-radius:10px;padding:16px;z-index:9999;' +
        'box-shadow:0 6px 18px rgba(0,0,0,0.35);max-width:760px;max-height:92vh;overflow:auto;' +
        'font-family:sans-serif;font-size:13px;color:#222';
      document.body.appendChild(popup);
      document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') popup.style.display = 'none';
      });
    }
    var neighbors = JSON.parse(cd[9]).map(function(n) {
      var tag = (n.state ? n.state : '') + (isFinite(n.melt) ? ' melt ' + n.melt : '');
      return '<li><a href="#" onclick="return window._jumpToSite(\\'' + n.site + '\\')">' +
             n.site + '</a> ' + n.name + ' <span style="color:#888">(' + tag + ', cos ' + n.sim +
             ')</span></li>';
    }).join('');
    var fmt = function(v, d) { return isFinite(Number(v)) ? Number(v).toFixed(d) : 'n/a'; };
    var status = cd[10] + (cd[11] >= 0 ? ', fold ' + cd[11] : '') +
                 (cd[12] ? ' &mdash; <b style="color:#b00">held out of training</b>' : '');
    var regional = cd[17] ? '<div><img src="' + cd[17] + '" style="border-radius:6px">' +
      '<div style="color:#888;font-size:11px;text-align:center">regional context, 25.6 km (summer)' +
      '</div></div>' : '';
    popup.innerHTML =
      '<button onclick="this.parentElement.style.display=\\'none\\'" style="position:absolute;' +
      'top:6px;right:12px;border:none;background:none;font-size:22px;cursor:pointer">&times;</button>' +
      '<h3 style="margin:0 0 2px 0">' + cd[0] + ' &mdash; ' + cd[1] + '</h3>' +
      '<div style="color:#666;margin-bottom:10px">' + status + ' &middot; cluster ' + cd[2] + '</div>' +
      '<div style="display:flex;gap:14px;flex-wrap:wrap">' +
      '<div><img src="' + cd[7] + '" style="border-radius:6px"><div style="color:#888;' +
      'font-size:11px;text-align:center">Sentinel-2, 1.28 km around gauge</div></div>' + regional +
      '<div style="min-width:260px"><table style="border-collapse:collapse">' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">basin elevation</td><td>' +
      fmt(cd[3], 0) + ' m</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">snow % of precip</td><td>' +
      fmt(cd[4], 0) + '%</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">drainage</td><td>' +
      fmt(cd[5], 0) + ' km&sup2;</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">basin slope</td><td>' +
      fmt(cd[6], 1) + '%</td></tr>' +
      '<tr><td style="padding:6px 8px 2px 0;color:#666">melt fraction (Apr&ndash;Jul)</td><td style="padding-top:6px">' +
      fmt(cd[13], 2) + '</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">flashiness (R-B)</td><td>' +
      fmt(cd[14], 2) + '</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">baseflow index</td><td>' +
      fmt(cd[15], 2) + '</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">log mean flow</td><td>' +
      fmt(cd[16], 2) + '</td></tr></table>' +
      '<div style="margin-top:8px;font-weight:bold">Nearest in embedding space</div>' +
      '<ul style="margin:4px 0 0 16px;padding:0">' + neighbors + '</ul></div></div>' +
      '<div style="margin-top:10px"><img src="' + cd[8] + '" style="max-width:100%"><div style="color:#888;' +
      'font-size:11px">hourly flow (log), canonical 92-day slices: seasons, flood, drought</div></div>';
    popup.style.display = 'block';
  }
  window._jumpToSite = function(site) {
    var cd = window._siteIndex[site];
    if (cd) showPopup(cd);
    return false;
  };
  plotDiv.on('plotly_click', function(data) {
    showPopup(data.points[0].customdata);
  });
  // Build the site index from all traces for neighbor jumps.
  (plotDiv.data || []).forEach(function(trace) {
    (trace.customdata || []).forEach(function(cd) { window._siteIndex[cd[0]] = cd; });
  });
})();
</script>
"""

    def create_main_visualization(self, df: pd.DataFrame,
                                  output_filename: str = "catchment_embeddings.html") -> str:
        """
        Builds the explorer figure and writes the self-contained HTML file.

        :param df: The dataframe from :func:`create_visualization_dataframe`.
        :type df: pd.DataFrame
        :param output_filename: The HTML file name, defaults to "catchment_embeddings.html".
        :type output_filename: str, optional
        :return: The path of the written HTML file.
        :rtype: str
        """
        customdata = df.apply(lambda row: [
            row["site_no"], row["station_nm"], row["cluster"], row["elev_m"], row["snow_pct"],
            row["drain_sqkm"], row["slope_pct"], row["image_base64"], row["hydrograph_base64"],
            json.dumps(row["neighbors"]), row.get("state", ""), int(row.get("fold", -1)),
            bool(row.get("holdout", False)), row.get("melt_fraction", float("nan")),
            row.get("rb_flashiness", float("nan")), row.get("bfi", float("nan")),
            row.get("log_mean_flow", float("nan")), row.get("regional_base64", ""),
        ], axis=1).tolist()

        hover = ("<b>%{customdata[0]}</b> %{customdata[10]}<br>%{customdata[1]}<br>" +
                 "elev %{customdata[3]:.0f} m | snow %{customdata[4]:.0f}% | " +
                 "drainage %{customdata[5]:.0f} km2<br>melt %{customdata[13]:.2f} | " +
                 "flash %{customdata[14]:.2f} | BFI %{customdata[15]:.2f}" +
                 "<br><i>click for details</i><extra></extra>")

        states = sorted(s for s in df.get("state", pd.Series([""] * len(df))).unique() if s)
        state_codes = df["state"].map({s: i for i, s in enumerate(states)}).fillna(-1).to_numpy() \
            if states else np.zeros(len(df))
        colorings: Dict[str, Tuple[np.ndarray, str]] = {
            "cluster": (df["cluster"].to_numpy(), "Turbo"),
            "melt fraction (Apr-Jul)": (df["melt_fraction"].to_numpy(), "Viridis"),
            "flashiness (R-B)": (df["rb_flashiness"].to_numpy(), "Plasma"),
            "baseflow index": (df["bfi"].to_numpy(), "Cividis"),
            "log mean flow (size)": (df["log_mean_flow"].to_numpy(), "Magma"),
            "elevation (m)": (df["elev_m"].to_numpy(), "Earth"),
            "snow % of precip": (df["snow_pct"].to_numpy(), "Blues"),
            "log10 drainage (km2)": (np.log10(df["drain_sqkm"].clip(lower=1.0)).to_numpy(),
                                     "Viridis"),
        }
        if states:
            colorings["state (%s)" % ", ".join("%d=%s" % (i, s) for i, s in enumerate(states))] = (
                state_codes, "Turbo")
        held = df["holdout"].to_numpy() if "holdout" in df else np.zeros(len(df), dtype=bool)
        fig = go.Figure(go.Scatter(
            x=df["x"], y=df["y"], mode="markers",
            marker=dict(size=11, color=colorings["cluster"][0], colorscale="Turbo",
                        showscale=True,
                        line=dict(width=np.where(held, 2.2, 0.5).tolist(),
                                  color=np.where(held, "#000", "#333").tolist())),
            customdata=customdata, hovertemplate=hover))
        title = ("Catchment embeddings — click a point for imagery, hydrograph and neighbors"
                 + (" — thick outline: %d basins held out of training" % int(held.sum())
                    if held.any() else ""))
        fig.update_layout(
            title=title,
            height=850, autosize=True, xaxis_title="t-SNE 1", yaxis_title="t-SNE 2",
            xaxis=dict(scaleanchor="y", scaleratio=1, gridcolor="#eee"),
            yaxis=dict(gridcolor="#eee"), plot_bgcolor="white",
            updatemenus=[dict(
                buttons=[dict(label=name, method="restyle",
                              args=[{"marker.color": [values.tolist()],
                                     "marker.colorscale": scale}])
                         for name, (values, scale) in colorings.items()],
                direction="down", x=1.0, xanchor="right", y=1.12, yanchor="top")])

        output_path = os.path.join(self.output_dir, output_filename)
        html_string = fig.to_html(include_plotlyjs=True, config={
            "displaylogo": False, "responsive": True, "modeBarButtonsToRemove": ["lasso2d"]})
        html_string = html_string.replace("</body>", self._popup_js() + "</body>")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_string)
        return output_path


def main() -> None:
    """
    CLI entry point: builds the explorer from a trained embedding directory.

    :return: None
    :rtype: None
    """
    import torch
    parser = argparse.ArgumentParser(description="Build the interactive catchment embedding explorer.")
    parser.add_argument("--data-dir", default=os.path.join("pilot_data", "embedding_dataset", "CO"))
    parser.add_argument("--embeddings", default=None,
                        help="Path to embeddings_*.pt (default <data-dir>/embeddings_concat.pt)")
    parser.add_argument("--gauges-csv", default=None,
                        help="site_no/station_nm CSV (default <data-dir>/gauges.csv, which the "
                             "training script writes; else the CO scrape list)")
    parser.add_argument("--records-dir", default=None,
                        help="Directory of <site>.npz records (default <data-dir>/combined_records "
                             "when present, else <data-dir>)")
    parser.add_argument("--signatures-csv", default=None,
                        help="*_signatures.csv written by embedding_probes.py, for regime colorings")
    parser.add_argument("--split", default=os.path.join("experiments", "catchment_foundation",
                                                        "splits", "basin_split_v1.json"),
                        help="Frozen basin split JSON for state/fold labels")
    parser.add_argument("--output", default="catchment_embeddings.html")
    args = parser.parse_args()
    payload = torch.load(args.embeddings or os.path.join(args.data_dir, "embeddings_concat.pt"),
                         weights_only=False)
    records_dir = args.records_dir or (os.path.join(args.data_dir, "combined_records")
                                       if os.path.isdir(os.path.join(args.data_dir, "combined_records"))
                                       else args.data_dir)
    gauges_csv = args.gauges_csv or (os.path.join(args.data_dir, "gauges.csv")
                                     if os.path.exists(os.path.join(args.data_dir, "gauges.csv"))
                                     else os.path.join("pilot_data", "scrapes", "CO", "gauges.csv"))
    signatures = pd.read_csv(args.signatures_csv, dtype={"site_id": str}).set_index("site_id") \
        if args.signatures_csv else None
    split = json.load(open(args.split)) if args.split and os.path.exists(args.split) else None
    embeddings = payload["embeddings"]
    embeddings = embeddings.float().numpy() if hasattr(embeddings, "numpy") else np.asarray(embeddings)
    visualizer = CatchmentVisualization(output_dir=args.data_dir, popup_image_size=256)
    df = visualizer.create_visualization_dataframe(
        records_dir, list(payload["site_ids"]), embeddings, gauges_csv=gauges_csv,
        signatures=signatures, split=split, holdout_sites=payload.get("holdout_sites"))
    path = visualizer.create_main_visualization(df, args.output)
    print("wrote", path, "(%.1f MB)" % (os.path.getsize(path) / 1e6))


if __name__ == "__main__":
    main()
