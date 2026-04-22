import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

COUNTRIES   = ["Switzerland", "Germany", "France", "Italy", "Austria"]
COLORS      = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
RUN_COLORS  = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
               '#19D3F3', '#FF6692', '#B6E880']
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
SEASONS     = {"Winter": [12, 1, 2], "Spring": [3, 4, 5],
               "Summer": [6, 7, 8],  "Autumn": [9, 10, 11]}
# Hours per month for a non-leap year
_HOURS_PER_MONTH = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]


def _load(output_db: str, table: str) -> pd.DataFrame:
    path = Path(__file__).parent.parent.parent / "cache" / "output_dbs" / output_db / f"{table}.parquet"
    return pd.read_parquet(path)


def _seasonal_avg(monthly: pd.DataFrame, countries: list[str]) -> pd.DataFrame:
    monthly = monthly.copy().set_index("Month")
    rows = {}
    for season, months in SEASONS.items():
        rows[season] = monthly.loc[monthly.index.isin(months), countries].mean()
    return pd.DataFrame(rows).T


def _month_labels(n_hours: int) -> list[str]:
    labels = []
    for m_idx, h in enumerate(_HOURS_PER_MONTH):
        labels.extend([MONTH_NAMES[m_idx]] * h)
    return labels[:n_hours]


def _load_all(runs: list[dict]) -> tuple[dict, dict, dict]:
    annual, monthly, hourly = {}, {}, {}
    for run in runs:
        annual[run["name"]]  = _load(run["output_db"], "national_elecprice_annual_c")
        monthly[run["name"]] = _load(run["output_db"], "national_elecprice_monthly_c_2050")
        hourly[run["name"]]  = _load(run["output_db"], "national_elecprice_hourly_c_2050")
    return annual, monthly, hourly


# ── Switzerland tab ──────────────────────────────────────────────────────────

def plot_switzerland(runs: list[dict]) -> tuple[go.Figure, go.Figure, go.Figure, go.Figure, go.Figure, go.Figure]:
    """Annual bar, seasonal bar, monthly heatmap, violin, monthly lines, hourly lines — CH only."""
    annual_data, monthly_data, hourly_data = _load_all(runs)
    n = len(runs)

    # Annual
    fig_annual = go.Figure()
    for run_idx, run in enumerate(runs):
        ann = annual_data[run["name"]].set_index("Row")["2050"]
        fig_annual.add_trace(go.Bar(
            x=[run["name"]], y=[float(ann.get("Switzerland", 0))],
            name=run["name"], marker_color=RUN_COLORS[run_idx % len(RUN_COLORS)],
            hovertemplate=run["name"] + ": %{y:.1f} €/MWh<extra></extra>",
        ))
    fig_annual.update_layout(
        title="Annual Average Price — Switzerland",
        xaxis_title="Scenario", yaxis_title="€/MWh",
        barmode="group", height=350, showlegend=False,
    )

    # Monthly heatmap — runs × months
    z = [monthly_data[run["name"]]["Switzerland"].tolist() for run in runs]
    run_names = [run["name"] for run in runs]
    fig_heatmap = go.Figure(go.Heatmap(
        z=z, x=MONTH_NAMES, y=run_names,
        colorscale="RdYlGn_r",
        colorbar=dict(title="€/MWh"),
        hovertemplate="<b>%{y}</b><br>%{x}: %{z:.1f} €/MWh<extra></extra>",
    ))
    fig_heatmap.update_layout(
        title="Monthly Average Price Heatmap — Switzerland",
        xaxis_title="Month", yaxis_title="Scenario",
        height=max(300, 60 * n + 150),
    )

    # Violin — hourly price distribution per month, one violin per scenario
    fig_violin = go.Figure()
    for run_idx, run in enumerate(runs):
        hourly = hourly_data[run["name"]]
        prices = hourly["Switzerland"].tolist()
        labels = _month_labels(len(prices))
        fig_violin.add_trace(go.Violin(
            x=labels, y=prices,
            name=run["name"],
            legendgroup=run["name"],
            line_color=RUN_COLORS[run_idx % len(RUN_COLORS)],
            meanline_visible=True,
            box_visible=False,
            opacity=0.7,
            scalegroup="ch",
            hovertemplate=run["name"] + " %{x}: %{y:.1f} €/MWh<extra></extra>",
        ))
    fig_violin.update_layout(
        title="Hourly Price Distribution by Month — Switzerland",
        xaxis=dict(categoryorder="array", categoryarray=MONTH_NAMES, title="Month"),
        yaxis_title="€/MWh",
        violinmode="group", height=500, legend_title="Scenario",
    )

    # Seasonal
    fig_seasonal = go.Figure()
    for run_idx, run in enumerate(runs):
        sea = _seasonal_avg(monthly_data[run["name"]], ["Switzerland"])
        fig_seasonal.add_trace(go.Bar(
            x=list(SEASONS.keys()), y=[float(sea.loc[s, "Switzerland"]) for s in SEASONS],
            name=run["name"], marker_color=RUN_COLORS[run_idx % len(RUN_COLORS)],
            hovertemplate=run["name"] + " %{x}: %{y:.1f} €/MWh<extra></extra>",
        ))
    fig_seasonal.update_layout(
        title="Seasonal Average Price — Switzerland",
        xaxis_title="Season", yaxis_title="€/MWh",
        barmode="group", height=350, legend_title="Scenario",
    )

    # Monthly lines
    fig_monthly = go.Figure()
    for run_idx, run in enumerate(runs):
        monthly = monthly_data[run["name"]]
        fig_monthly.add_trace(go.Scatter(
            x=MONTH_NAMES, y=monthly["Switzerland"].tolist(),
            name=run["name"], mode="lines+markers",
            line=dict(color=RUN_COLORS[run_idx % len(RUN_COLORS)]),
            hovertemplate=run["name"] + " %{x}: %{y:.1f} €/MWh<extra></extra>",
        ))
    fig_monthly.update_layout(
        title="Monthly Prices — Switzerland",
        xaxis_title="Month", yaxis_title="€/MWh",
        height=350, hovermode="x unified", legend_title="Scenario",
    )

    # Hourly
    fig_hourly = go.Figure()
    for run_idx, run in enumerate(runs):
        hourly = hourly_data[run["name"]]
        fig_hourly.add_trace(go.Scatter(
            x=hourly["Hour"], y=hourly["Switzerland"],
            name=run["name"], mode="lines",
            line=dict(color=RUN_COLORS[run_idx % len(RUN_COLORS)]),
            hovertemplate=run["name"] + " h%{x}: %{y:.1f} €/MWh<extra></extra>",
        ))
    fig_hourly.update_layout(
        title="Hourly Prices — Switzerland",
        xaxis_title="Hour", yaxis_title="€/MWh",
        height=400, hovermode="x unified", legend_title="Scenario",
    )

    return fig_annual, fig_heatmap, fig_violin, fig_seasonal, fig_monthly, fig_hourly


# ── All Countries tab ────────────────────────────────────────────────────────

def plot_all_countries(runs: list[dict]) -> tuple[go.Figure, go.Figure, go.Figure, go.Figure, go.Figure]:
    """Annual bars, seasonal bars, monthly heatmap, violin, monthly lines — all countries."""
    annual_data, monthly_data, hourly_data = _load_all(runs)
    n = len(runs)
    bar_width = 0.8 / n
    offsets = [(i - (n - 1) / 2) * bar_width for i in range(n)]
    country_idx = list(range(len(COUNTRIES)))

    # Annual
    fig_annual = go.Figure()
    for run_idx, run in enumerate(runs):
        ann = annual_data[run["name"]].set_index("Row")["2050"]
        fig_annual.add_trace(go.Bar(
            x=[i + offsets[run_idx] for i in country_idx],
            y=[float(ann.get(c, 0)) for c in COUNTRIES],
            name=run["name"], width=bar_width,
            marker_color=RUN_COLORS[run_idx % len(RUN_COLORS)],
            text=COUNTRIES,
            hovertemplate="<b>%{text}</b><br>" + run["name"] + ": %{y:.1f} €/MWh<extra></extra>",
        ))
    fig_annual.update_layout(
        title="Annual Average Prices — All Countries",
        xaxis=dict(tickvals=country_idx, ticktext=COUNTRIES, title="Country"),
        yaxis_title="€/MWh", barmode="overlay", height=400, legend_title="Scenario",
    )

    # Monthly heatmap — runs × months, one per country (shared scale)
    all_z = []
    heatmap_data = []
    for country in COUNTRIES:
        z, rnames = [], []
        for run in runs:
            monthly = monthly_data[run["name"]]
            if country in monthly.columns:
                z.append(monthly[country].tolist())
                rnames.append(run["name"])
        heatmap_data.append((z, rnames))
        all_z.extend(v for row in z for v in row)
    zmin = min(all_z) if all_z else 0
    zmax = max(all_z) if all_z else 1

    fig_heatmap = make_subplots(rows=len(COUNTRIES), cols=1,
        subplot_titles=[f"Heatmap — {c}" for c in COUNTRIES],
        vertical_spacing=0.06)
    for c_idx, (country, (z, rnames)) in enumerate(zip(COUNTRIES, heatmap_data)):
        fig_heatmap.add_trace(go.Heatmap(
            z=z, x=MONTH_NAMES, y=rnames,
            colorscale="RdYlGn_r", zmin=zmin, zmax=zmax,
            colorbar=dict(title="€/MWh", len=0.9/len(COUNTRIES),
                          y=1-(c_idx+0.5)/len(COUNTRIES)),
            hovertemplate="<b>%{y}</b><br>%{x}: %{z:.1f} €/MWh<extra></extra>",
            showscale=(c_idx == 0),
        ), row=c_idx + 1, col=1)
        fig_heatmap.update_yaxes(title_text=country, row=c_idx + 1, col=1)
    fig_heatmap.update_layout(
        title="Monthly Average Price Heatmap — All Countries",
        height=200 * len(COUNTRIES),
    )

    # Violin — one subplot per country, grouped by scenario
    fig_violin = make_subplots(rows=len(COUNTRIES), cols=1,
        subplot_titles=[f"Price Distribution — {c}" for c in COUNTRIES],
        vertical_spacing=0.06, shared_xaxes=True)
    for c_idx, country in enumerate(COUNTRIES):
        for run_idx, run in enumerate(runs):
            hourly = hourly_data[run["name"]]
            if country not in hourly.columns:
                continue
            prices = hourly[country].tolist()
            labels = _month_labels(len(prices))
            fig_violin.add_trace(go.Violin(
                x=labels, y=prices,
                name=run["name"],
                legendgroup=run["name"],
                showlegend=(c_idx == 0),
                line_color=RUN_COLORS[run_idx % len(RUN_COLORS)],
                meanline_visible=True,
                box_visible=True,
                opacity=0.7,
                scalegroup=country,
                hovertemplate=f"{country} — {run['name']} %{{x}}: %{{y:.1f}} €/MWh<extra></extra>",
            ), row=c_idx + 1, col=1)
        fig_violin.update_yaxes(title_text="€/MWh", row=c_idx + 1, col=1)
    fig_violin.update_layout(
        title="Hourly Price Distribution by Month — All Countries",
        xaxis=dict(categoryorder="array", categoryarray=MONTH_NAMES),
        violinmode="group", height=450 * len(COUNTRIES), legend_title="Scenario",
    )

    # Seasonal — one subplot per country
    fig_seasonal = make_subplots(rows=1, cols=len(COUNTRIES),
        subplot_titles=COUNTRIES, shared_yaxes=True)
    for c_idx, country in enumerate(COUNTRIES):
        for run_idx, run in enumerate(runs):
            sea = _seasonal_avg(monthly_data[run["name"]], [country])
            fig_seasonal.add_trace(go.Bar(
                x=list(SEASONS.keys()),
                y=[float(sea.loc[s, country]) for s in SEASONS],
                name=run["name"],
                marker_color=RUN_COLORS[run_idx % len(RUN_COLORS)],
                legendgroup=run["name"], showlegend=(c_idx == 0),
                hovertemplate=f"{country} — {run['name']} %{{x}}: %{{y:.1f}} €/MWh<extra></extra>",
            ), row=1, col=c_idx + 1)
    fig_seasonal.update_layout(
        title="Seasonal Average Prices — All Countries",
        yaxis_title="€/MWh", barmode="group", height=400, legend_title="Scenario",
    )

    # Monthly lines — one subplot per country
    fig_monthly = make_subplots(rows=len(COUNTRIES), cols=1,
        subplot_titles=[f"Monthly Prices — {c}" for c in COUNTRIES],
        vertical_spacing=0.06, shared_xaxes=True)
    for c_idx, country in enumerate(COUNTRIES):
        for run_idx, run in enumerate(runs):
            monthly = monthly_data[run["name"]]
            if country not in monthly.columns:
                continue
            fig_monthly.add_trace(go.Scatter(
                x=MONTH_NAMES, y=monthly[country].tolist(),
                name=run["name"], mode="lines+markers",
                line=dict(color=RUN_COLORS[run_idx % len(RUN_COLORS)]),
                legendgroup=run["name"], showlegend=(c_idx == 0),
                hovertemplate=f"{country} — {run['name']}: %{{y:.1f}} €/MWh<extra></extra>",
            ), row=c_idx + 1, col=1)
        fig_monthly.update_yaxes(title_text="€/MWh", row=c_idx + 1, col=1)
    fig_monthly.update_layout(
        title="Monthly Prices — All Countries",
        height=200 * len(COUNTRIES), hovermode="x unified", legend_title="Scenario",
    )

    return fig_annual, fig_heatmap, fig_violin, fig_seasonal, fig_monthly
