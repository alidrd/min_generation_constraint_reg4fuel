import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
          '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']


def _load(output_db: str, table: str) -> pd.DataFrame:
    path = Path(__file__).parent.parent.parent / "cache" / "output_dbs" / output_db / f"{table}.parquet"
    return pd.read_parquet(path)


def _active_cols(dfs: list[pd.DataFrame], exclude: list[str]) -> list[str]:
    """Union of non-zero columns across all runs."""
    seen, result = set(), []
    for df in dfs:
        for c in df.columns:
            if c not in exclude and c not in seen and (df[c] != 0).any():
                seen.add(c)
                result.append(c)
    return result


def plot_curtailment_ch(runs: list[dict]) -> tuple[go.Figure, go.Figure]:
    """Returns (hourly_fig, monthly_comparison_fig)."""

    # --- Hourly: one subplot per run (unchanged) ---
    n = len(runs)
    fig_hourly = make_subplots(
        rows=n, cols=1,
        subplot_titles=[f"Hourly Curtailment & Spillage — CH — {r['name']}" for r in runs],
        vertical_spacing=0.08,
    )
    all_monthly = {}
    for run_idx, run in enumerate(runs):
        hourly  = _load(run["output_db"], "national_curtailment_hourly_c_ch_2050")
        monthly = _load(run["output_db"], "national_curtailment_monthly_c_ch_2050")
        all_monthly[run["name"]] = monthly

        cols = _active_cols([hourly], exclude=["Hour"])
        for i, col in enumerate(cols):
            color = COLORS[i % len(COLORS)]
            fig_hourly.add_trace(go.Scatter(
                x=hourly["Hour"], y=hourly[col], name=col,
                line=dict(color=color), legendgroup=col,
                showlegend=(run_idx == 0),
            ), row=run_idx + 1, col=1)
        fig_hourly.update_xaxes(title_text="Hour", row=run_idx + 1, col=1)
        fig_hourly.update_yaxes(title_text="GWh",  row=run_idx + 1, col=1)

    fig_hourly.update_layout(
        title="Hourly Curtailment & Spillage — CH",
        height=400 * n, hovermode="x unified",
    )

    # --- Monthly comparison: stacked bars side by side, one stack per run ---
    cols = _active_cols(list(all_monthly.values()), exclude=["Month"])
    months = list(range(1, 13))
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    # Build matrix: rows = runs, cols = months, values = total curtailment
    z = []
    run_names = []
    for run in runs:
        monthly = all_monthly[run["name"]]
        totals = monthly[cols].sum(axis=1).tolist()
        z.append(totals)
        run_names.append(run["name"])

    fig_monthly = go.Figure(go.Heatmap(
        z=z,
        x=month_names,
        y=run_names,
        colorscale="YlOrRd",
        colorbar=dict(title="TWh"),
        hovertemplate="<b>%{y}</b><br>%{x}: %{z:.3f} TWh<extra></extra>",
    ))

    fig_monthly.update_layout(
        title="Monthly Total Curtailment & Spillage — CH (all runs)",
        xaxis_title="Month",
        yaxis_title="Scenario",
        height=max(300, 60 * len(runs) + 150),
    )

    return fig_hourly, fig_monthly
