import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

RUN_COLORS  = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
               '#19D3F3', '#FF6692', '#B6E880']
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
_HOURS_PER_MONTH = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]

RESERVOIR_FILES = {
    "Dam Reservoir":   "Reservoirs_DamLevel_hourly_CH.csv",
    "Pump Storage":    "Reservoirs_PumpLevel_hourly_CH.csv",
    "Daily Pump":      "Reservoirs_DailyPumpLevel_hourly_CH.csv",
}


def _euler_path(run_name: str, filename: str) -> Path:
    return Path(__file__).parent.parent.parent / "cache" / "euler" / run_name / filename


def _month_labels(n_hours: int) -> list[str]:
    labels = []
    for m_idx, h in enumerate(_HOURS_PER_MONTH):
        labels.extend([MONTH_NAMES[m_idx]] * h)
    return labels[:n_hours]


def plot_storage_soc_ch(runs: list[dict]) -> tuple[go.Figure, go.Figure] | None:
    """Returns (fig_total, fig_hourly) for CH reservoir SOC from Euler files."""

    available = {label: fname for label, fname in RESERVOIR_FILES.items()
                 if any(_euler_path(r["name"], fname).exists() for r in runs)}
    if not available:
        return None

    # Load all series into memory once
    data = {}  # {run_name: {label: Series}}
    for run in runs:
        data[run["name"]] = {}
        for label, fname in available.items():
            p = _euler_path(run["name"], fname)
            if p.exists():
                df = pd.read_csv(p)
                data[run["name"]][label] = df.set_index("Hour")["GWh"]

    # Total SOC (sum of all types) — one line per run
    fig_total = go.Figure()
    for run_idx, run in enumerate(runs):
        series = list(data[run["name"]].values())
        if not series:
            continue
        total = sum(series)
        fig_total.add_trace(go.Scatter(
            x=total.index, y=total.values,
            name=run["name"], mode="lines",
            line=dict(color=RUN_COLORS[run_idx % len(RUN_COLORS)]),
            hovertemplate=run["name"] + " h%{x}: %{y:.1f} GWh<extra></extra>",
        ))
    fig_total.update_layout(
        title="Total Reservoir SOC — CH (all types)",
        xaxis_title="Hour", yaxis_title="GWh",
        height=400, hovermode="x unified", legend_title="Scenario",
    )

    # Hourly lines per reservoir type — one subplot per type, runs overlaid
    n_types = len(available)
    fig_hourly = make_subplots(rows=n_types, cols=1,
        subplot_titles=[f"Hourly SOC — {lbl}" for lbl in available],
        vertical_spacing=0.08, shared_xaxes=True)
    for t_idx, (label, fname) in enumerate(available.items()):
        for run_idx, run in enumerate(runs):
            s = data[run["name"]].get(label)
            if s is None:
                continue
            fig_hourly.add_trace(go.Scatter(
                x=s.index, y=s.values,
                name=run["name"],
                legendgroup=run["name"],
                showlegend=(t_idx == 0),
                mode="lines",
                line=dict(color=RUN_COLORS[run_idx % len(RUN_COLORS)]),
                hovertemplate=f"{label} — {run['name']} h%{{x}}: %{{y:.1f}} GWh<extra></extra>",
            ), row=t_idx + 1, col=1)
        fig_hourly.update_yaxes(title_text="GWh", row=t_idx + 1, col=1)
    fig_hourly.update_layout(
        title="Hourly Reservoir SOC by Type — CH",
        height=max(300, 320 * n_types),
        hovermode="x unified", legend_title="Scenario",
    )

    return fig_total, fig_hourly
