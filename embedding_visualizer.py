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
        pil = pil.resize((self.popup_image_size, self.popup_image_size), Image.NEAREST)
        buffer = io.BytesIO()
        pil.save(buffer, format="JPEG", quality=85)
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

    def create_visualization_dataframe(self, data_dir: str, site_ids: List[str],
                                       embeddings: np.ndarray, gauges_csv: Optional[str] = None,
                                       n_neighbors: int = 5, n_clusters: int = 5) -> pd.DataFrame:
        """
        Assembles the per-site dataframe: coordinates, attributes, thumbnails and neighbors.

        :param data_dir: Directory with the <site>.npz embedding records.
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
        :return: The visualization dataframe.
        :rtype: pd.DataFrame
        """
        normalized = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        xy = self.reduce_dimensions(normalized)
        clusters = KMeans(n_clusters=n_clusters, n_init=10, random_state=0).fit_predict(normalized)
        similarity = normalized @ normalized.T

        names = {}
        if gauges_csv is not None and os.path.exists(gauges_csv):
            gauges = pd.read_csv(gauges_csv, dtype={"site_no": str})
            names = dict(zip(gauges["site_no"], gauges["station_nm"]))

        rows = []
        for i, site in enumerate(site_ids):
            record = np.load(os.path.join(data_dir, site + ".npz"))
            attr_names = list(record["static_names"])
            static = record["static"]

            def attr(name: str) -> float:
                return float(static[attr_names.index(name)]) if name in attr_names else float("nan")

            order = np.argsort(-similarity[i])
            neighbors = [{"site": site_ids[j], "name": names.get(site_ids[j], ""),
                          "sim": round(float(similarity[i, j]), 3)}
                         for j in order[1:n_neighbors + 1]]
            rows.append({
                "site_no": site, "station_nm": names.get(site, site),
                "x": float(xy[i, 0]), "y": float(xy[i, 1]), "cluster": int(clusters[i]),
                "elev_m": attr("ELEV_MEAN_M_BASIN"), "snow_pct": attr("SNOW_PCT_PRECIP"),
                "drain_sqkm": attr("DRAIN_SQKM"), "slope_pct": attr("SLOPE_PCT"),
                "image_base64": self.sentinel_to_base64(record["image"]),
                "hydrograph_base64": self.hydrograph_to_base64(record["history"],
                                                               str(record["history_start"])),
                "neighbors": neighbors,
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
      return '<li><a href="#" onclick="return window._jumpToSite(\\'' + n.site + '\\')">' +
             n.site + '</a> ' + n.name + ' <span style="color:#888">(cos ' + n.sim + ')</span></li>';
    }).join('');
    popup.innerHTML =
      '<button onclick="this.parentElement.style.display=\\'none\\'" style="position:absolute;' +
      'top:6px;right:12px;border:none;background:none;font-size:22px;cursor:pointer">&times;</button>' +
      '<h3 style="margin:0 0 2px 0">' + cd[0] + ' &mdash; ' + cd[1] + '</h3>' +
      '<div style="color:#666;margin-bottom:10px">cluster ' + cd[2] + '</div>' +
      '<div style="display:flex;gap:14px;flex-wrap:wrap">' +
      '<div><img src="' + cd[7] + '" style="border-radius:6px"><div style="color:#888;' +
      'font-size:11px;text-align:center">Sentinel-2, 1.28 km around gauge</div></div>' +
      '<div style="min-width:260px"><table style="border-collapse:collapse">' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">basin elevation</td><td>' +
      Number(cd[3]).toFixed(0) + ' m</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">snow % of precip</td><td>' +
      Number(cd[4]).toFixed(0) + '%</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">drainage</td><td>' +
      Number(cd[5]).toFixed(0) + ' km&sup2;</td></tr>' +
      '<tr><td style="padding:2px 8px 2px 0;color:#666">basin slope</td><td>' +
      Number(cd[6]).toFixed(1) + '%</td></tr></table>' +
      '<div style="margin-top:8px;font-weight:bold">Nearest in embedding space</div>' +
      '<ul style="margin:4px 0 0 16px;padding:0">' + neighbors + '</ul></div></div>' +
      '<div style="margin-top:10px"><img src="' + cd[8] + '"><div style="color:#888;' +
      'font-size:11px">daily flow (symlog), full history</div></div>';
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
            json.dumps(row["neighbors"]),
        ], axis=1).tolist()

        hover = ("<b>%{customdata[0]}</b><br>%{customdata[1]}<br>" +
                 "elev %{customdata[3]:.0f} m | snow %{customdata[4]:.0f}%<br>" +
                 "drainage %{customdata[5]:.0f} km2<br><i>click for details</i><extra></extra>")

        colorings: Dict[str, Tuple[np.ndarray, str]] = {
            "cluster": (df["cluster"].to_numpy(), "Turbo"),
            "elevation (m)": (df["elev_m"].to_numpy(), "Earth"),
            "snow % of precip": (df["snow_pct"].to_numpy(), "Blues"),
            "log10 drainage (km2)": (np.log10(df["drain_sqkm"].clip(lower=1.0)).to_numpy(),
                                     "Viridis"),
        }
        fig = go.Figure(go.Scatter(
            x=df["x"], y=df["y"], mode="markers",
            marker=dict(size=11, color=colorings["cluster"][0], colorscale="Turbo",
                        showscale=True, line=dict(width=0.5, color="#333")),
            customdata=customdata, hovertemplate=hover))
        fig.update_layout(
            title="Catchment embeddings — click a point for imagery, hydrograph and neighbors",
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
    parser.add_argument("--gauges-csv", default=os.path.join("pilot_data", "scrapes", "CO",
                                                             "gauges.csv"))
    parser.add_argument("--output", default="catchment_embeddings.html")
    args = parser.parse_args()
    payload = torch.load(args.embeddings or os.path.join(args.data_dir, "embeddings_concat.pt"),
                         weights_only=False)
    visualizer = CatchmentVisualization(output_dir=args.data_dir)
    df = visualizer.create_visualization_dataframe(
        args.data_dir, payload["site_ids"], payload["embeddings"].numpy(),
        gauges_csv=args.gauges_csv)
    path = visualizer.create_main_visualization(df, args.output)
    print("wrote", path)


if __name__ == "__main__":
    main()
