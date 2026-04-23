"""
Security of Supply — RDEM (Remaining Domestic Energy Margin).

remaining_supply(t) is computed following set_min_local_generation():

  1. Storage SOC(t)
       Current energy stored in hydro reservoirs + pump storage at hour t.
       Source: Euler hourly CSV files (MWh → /1000 → GWh).
       If Euler files are missing, this term is 0 (conservative).

  2. RES remaining from t to end of winter
       sum_{h=t}^{T_end} [ PV(h) + Wind(h) + RoR(h) ]   (GWh)
       Uses actual dispatch from the output table as a proxy for generation
       potential (valid when RES is not curtailed).

  3. Conventional potential from t to end of winter
       installed_conv_capacity_GW × (T_end − t) hours    (GWh)
       Uses the national_capacity_gw_ch output table for installed GW.

remaining_demand(t) = sum_{h=t}^{T_end} Load_Total(h)

RDEM(t) = remaining_supply(t) / remaining_demand(t)

Note on inflows: future hydro inflows (rain/snow-melt from t to T_end) are NOT
included separately — this is conservative. The SOC already reflects all past
inflows. Adding future inflows would require water-profile data from the input DB.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# ── tuneable constants ────────────────────────────────────────────────────────
WINTER_MONTHS       = [10, 11, 12, 1, 2, 3]      # Oct–Mar
DISPLAY_MONTH_ORDER = [10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9]

GEN_TABLE   = "national_generation_hourly_gwh_c_ch_2050"
CAP_TABLE   = "national_capacity_gw_ch"
LOAD_COL    = "Load (Total)"   # in GEN_TABLE, already GWh

# Columns treated as profile-based RES (actual dispatch ≈ potential)
RES_COLS = {
    "Hydro RoR",
    "Wind Onshore", "Wind Offshore",
    "PV Roof", "PV Alpine", "PV Facade", "PV Highway", "PV OpenField", "PV Agri",
    "CSP", "Marine", "Other RES",
}

# Rows in CAP_TABLE that count as dispatchable conventional capacity
CONV_CAP_ROWS = {
    "Gas CC", "Gas CC-CCS", "Gas CC-Syn", "Gas Other",
    "Coal", "Lignite", "Oil",
    "Nuclear",
    "Biogas", "Biomass", "Waste",
    "Geothermal", "Geothermal-Advanced",
    "CAES", "LAES", "P2G2P",
}

# Euler SOC files: label → filename (values in MWh, divide by 1000 for GWh)
SOC_FILES = {
    "Dam":        "Reservoirs_DamLevel_hourly_CH.csv",
    "Pump":       "Reservoirs_PumpLevel_hourly_CH.csv",
    "Daily Pump": "Reservoirs_DailyPumpLevel_hourly_CH.csv",
}

HOURS_PER_MONTH = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]
MONTH_NAMES     = ["Jan","Feb","Mar","Apr","May","Jun",
                   "Jul","Aug","Sep","Oct","Nov","Dec"]

RUN_COLORS = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
              '#19D3F3', '#FF6692', '#B6E880']

CAT_COLORS = {
    "hydro":   "#1a9fd4",
    "nuclear": "#f4a261",
    "thermal": "#e63946",
    "wind":    "#2a9d8f",
    "solar":   "#e9c46a",
    "other":   "#adb5bd",
}

# Stacked component colours for the absolute RDEM panel
COMP_COLORS = {
    "SOC (storage)":       "#1a9fd4",
    "RES remaining":       "#2a9d8f",
    "Conv. potential":     "#e63946",
}

# ── index helpers ─────────────────────────────────────────────────────────────

def _month_start_hours() -> dict[int, int]:
    h, starts = 0, {}
    for m_idx, n in enumerate(HOURS_PER_MONTH):
        starts[m_idx + 1] = h
        h += n
    return starts


def _hours_for_months(months: list[int]) -> list[int]:
    starts = _month_start_hours()
    result = []
    for m in months:
        s = starts[m]
        result.extend(range(s, s + HOURS_PER_MONTH[m - 1]))
    return result


def _display_indices() -> list[int]:
    return _hours_for_months(DISPLAY_MONTH_ORDER)


def _winter_indices() -> list[int]:
    return _hours_for_months(WINTER_MONTHS)


def _display_month_boundaries() -> list[int]:
    boundaries = [0]
    for m in DISPLAY_MONTH_ORDER:
        boundaries.append(boundaries[-1] + HOURS_PER_MONTH[m - 1])
    return boundaries


# ── data loaders ──────────────────────────────────────────────────────────────

def _cache_root() -> Path:
    return Path(__file__).parent.parent.parent / "cache"


def _load_output(output_db: str, table: str) -> pd.DataFrame | None:
    p = _cache_root() / "output_dbs" / output_db / f"{table}.parquet"
    return pd.read_parquet(p) if p.exists() else None


def _load_euler_soc(run_name: str) -> pd.Series | None:
    """
    Load and sum all three reservoir SOC CSVs for a run.
    Files store values in MWh; we return GWh (÷1000).
    Returns a Series indexed 0..8759, or None if no files found.
    """
    euler_dir = _cache_root() / "euler" / run_name
    total: pd.Series | None = None
    for label, fname in SOC_FILES.items():
        p = euler_dir / fname
        if not p.exists():
            continue
        df = pd.read_csv(p).set_index("Hour")["GWh"]
        series = df.reindex(range(8760), fill_value=0.0) / 1000.0  # MWh → GWh
        total = series if total is None else total + series
    return total


def _load_conv_cap_gw(output_db: str) -> float:
    """
    Return total installed conventional capacity (GW) for CH from CAP_TABLE.
    Rows with negative values (charging) are skipped.
    """
    df = _load_output(output_db, CAP_TABLE)
    if df is None:
        return 0.0
    year_col = [c for c in df.columns if c != "Row"]
    if not year_col:
        return 0.0
    col = year_col[-1]
    mask = df["Row"].isin(CONV_CAP_ROWS)
    return float(df.loc[mask, col].clip(lower=0).sum())


# ── column helpers ────────────────────────────────────────────────────────────

_NON_SUPPLY = {"Exports", "Imports", "Imports (Net)", "Load (Net)", "Load (Total)"}


def _is_consumption(col: str) -> bool:
    return col.endswith("(Load)") or col.endswith("(Up)")


def _supply_cols(cols: list[str]) -> list[str]:
    return [c for c in cols if c not in _NON_SUPPLY and not _is_consumption(c)]


def _classify_cols(cols: list[str]) -> dict[str, list[str]]:
    cats: dict[str, list[str]] = {k: [] for k in CAT_COLORS}
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ["hydro", "pump", "ror"]):
            cats["hydro"].append(c)
        elif "nuclear" in cl:
            cats["nuclear"].append(c)
        elif any(k in cl for k in ["gas", "coal", "lignite", "oil", "waste",
                                    "biomass", "biogas", "geotherm"]):
            cats["thermal"].append(c)
        elif "wind" in cl:
            cats["wind"].append(c)
        elif any(k in cl for k in ["pv", "csp", "solar"]):
            cats["solar"].append(c)
        else:
            cats["other"].append(c)
    return cats


# ── RDEM computation ──────────────────────────────────────────────────────────

def _compute_rdem(
    gen_df: pd.DataFrame,
    demand: pd.Series,
    winter_idx: list[int],
    soc: pd.Series | None,
    conv_cap_gw: float,
) -> dict:
    """
    Three-component rolling RDEM following set_min_local_generation().

    Returns dict with keys:
      rdem, rem_supply, rem_demand   — {hour: float}
      comp_soc, comp_res, comp_conv  — {hour: float}  (supply breakdown)
    """
    res_cols = [c for c in gen_df.columns if c in RES_COLS]
    res_gen  = gen_df[res_cols].clip(lower=0).sum(axis=1) if res_cols else pd.Series(0.0, index=gen_df.index)

    out = {k: {} for k in ["rdem", "rem_supply", "rem_demand",
                            "comp_soc", "comp_res", "comp_conv"]}

    for pos, h in enumerate(winter_idx):
        remaining_h = winter_idx[pos:]

        # Component 1: storage SOC at this exact hour
        soc_val = float(soc.iloc[h]) if soc is not None else 0.0

        # Component 2: remaining RES generation from h to end of winter
        res_val = float(res_gen.iloc[remaining_h].sum())

        # Component 3: conventional potential = installed GW × hours remaining
        conv_val = conv_cap_gw * len(remaining_h)

        sup = soc_val + res_val + conv_val
        dem = float(demand.iloc[remaining_h].sum())

        out["comp_soc"][h]  = soc_val
        out["comp_res"][h]  = res_val
        out["comp_conv"][h] = conv_val
        out["rem_supply"][h] = sup
        out["rem_demand"][h] = dem
        out["rdem"][h]       = sup / dem if dem > 0 else float("nan")

    return out


# ── main plot function ────────────────────────────────────────────────────────

def plot_security_supply(runs: list[dict]) -> tuple | None:
    """
    Returns (fig_rdem, fig_comp, fig_worst) or None if no data in cache.

    fig_rdem  — 2-row: RDEM ratio (top) + absolute GWh stacked by component (bottom)
    fig_comp  — hourly supply mix vs demand over full year (x starts Oct)
    fig_worst — bar chart: worst-hour RDEM per scenario
    """
    all_gen:    dict[str, pd.DataFrame] = {}
    all_demand: dict[str, pd.Series]   = {}
    all_cats:   dict[str, dict]        = {}
    all_soc:    dict[str, pd.Series | None] = {}
    all_conv:   dict[str, float]       = {}

    for run in runs:
        raw = _load_output(run["output_db"], GEN_TABLE)
        if raw is None:
            continue
        raw = raw.drop(columns=[c for c in raw.columns if c.lower() == "hour"],
                       errors="ignore").reset_index(drop=True)

        all_demand[run["name"]] = (raw[LOAD_COL] if LOAD_COL in raw.columns
                                   else raw[_supply_cols(list(raw.columns))].clip(lower=0).sum(axis=1))

        scols = [c for c in _supply_cols(list(raw.columns)) if (raw[c] != 0).any()]
        if not scols:
            continue
        all_gen[run["name"]]  = raw[scols]
        all_cats[run["name"]] = _classify_cols(scols)
        all_soc[run["name"]]  = _load_euler_soc(run["name"])
        all_conv[run["name"]] = _load_conv_cap_gw(run["output_db"])

    if not all_gen:
        return None

    display_idx = _display_indices()
    winter_idx  = _winter_indices()
    boundaries  = _display_month_boundaries()
    tick_vals   = boundaries[:-1]
    tick_text   = [MONTH_NAMES[m - 1] for m in DISPLAY_MONTH_ORDER]
    display_pos = {h: pos for pos, h in enumerate(display_idx)}
    winter_end_pos = sum(HOURS_PER_MONTH[m - 1] for m in WINTER_MONTHS)

    def _xpos(h: int) -> int:
        return display_pos.get(h, h)

    # ── fig 1: RDEM ratio + absolute breakdown ────────────────────────────────
    fig_rdem = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=[
            "Rolling RDEM  =  (SOC + RES remaining + Conv. potential)  ÷  remaining demand",
            "Absolute remaining energy (GWh)  —  stacked supply components vs demand",
        ],
        vertical_spacing=0.10,
        row_heights=[0.40, 0.60],
    )

    worst_rdem: dict[str, float] = {}

    for run_idx, run in enumerate(runs):
        if run["name"] not in all_gen:
            continue

        rd = _compute_rdem(
            gen_df      = all_gen[run["name"]],
            demand      = all_demand[run["name"]],
            winter_idx  = winter_idx,
            soc         = all_soc[run["name"]],
            conv_cap_gw = all_conv[run["name"]],
        )

        worst_h = min(rd["rdem"], key=rd["rdem"].get)
        worst_v = rd["rdem"][worst_h]
        worst_rdem[run["name"]] = worst_v
        color   = RUN_COLORS[run_idx % len(RUN_COLORS)]
        x_pos   = [_xpos(h) for h in winter_idx]

        # ── row 1: ratio line ────────────────────────────────────────────────
        fig_rdem.add_trace(go.Scatter(
            x=x_pos, y=[rd["rdem"][h] for h in winter_idx],
            name=run["name"], legendgroup=run["name"],
            mode="lines", line=dict(color=color),
            hovertemplate=run["name"] + ": RDEM=%{y:.3f}<extra></extra>",
        ), row=1, col=1)
        fig_rdem.add_trace(go.Scatter(
            x=[_xpos(worst_h)], y=[worst_v],
            mode="markers+text",
            marker=dict(color=color, size=10, symbol="star"),
            text=[f"  {worst_v:.3f}"],
            textposition="middle right",
            showlegend=False, legendgroup=run["name"],
            hovertemplate=f"{run['name']} worst: RDEM={worst_v:.3f}<extra></extra>",
        ), row=1, col=1)

        # ── row 2: stacked absolute components ──────────────────────────────
        # Use a unique stackgroup per run so scenarios don't stack on each other
        sg = f"run_{run_idx}"
        soc_available = all_soc[run["name"]] is not None

        fig_rdem.add_trace(go.Scatter(
            x=x_pos, y=[rd["comp_soc"][h]  for h in winter_idx],
            name="SOC" + (" (no Euler)" if not soc_available else ""),
            legendgroup=f"comp_soc_{run_idx}",
            showlegend=(run_idx == 0),
            stackgroup=sg, mode="lines",
            line=dict(width=0, color=COMP_COLORS["SOC (storage)"]),
            fillcolor=COMP_COLORS["SOC (storage)"],
            hovertemplate="SOC: %{y:,.0f} GWh<extra></extra>",
        ), row=2, col=1)
        fig_rdem.add_trace(go.Scatter(
            x=x_pos, y=[rd["comp_res"][h]  for h in winter_idx],
            name="RES remaining",
            legendgroup=f"comp_res_{run_idx}",
            showlegend=(run_idx == 0),
            stackgroup=sg, mode="lines",
            line=dict(width=0, color=COMP_COLORS["RES remaining"]),
            fillcolor=COMP_COLORS["RES remaining"],
            hovertemplate="RES remaining: %{y:,.0f} GWh<extra></extra>",
        ), row=2, col=1)
        fig_rdem.add_trace(go.Scatter(
            x=x_pos, y=[rd["comp_conv"][h] for h in winter_idx],
            name="Conv. potential",
            legendgroup=f"comp_conv_{run_idx}",
            showlegend=(run_idx == 0),
            stackgroup=sg, mode="lines",
            line=dict(width=0, color=COMP_COLORS["Conv. potential"]),
            fillcolor=COMP_COLORS["Conv. potential"],
            hovertemplate="Conv. potential: %{y:,.0f} GWh<extra></extra>",
        ), row=2, col=1)
        fig_rdem.add_trace(go.Scatter(
            x=x_pos, y=[rd["rem_demand"][h] for h in winter_idx],
            name=run["name"] + " demand",
            legendgroup=run["name"],
            showlegend=False,
            mode="lines", line=dict(color=color, width=2, dash="dot"),
            hovertemplate=run["name"] + " demand: %{y:,.0f} GWh<extra></extra>",
        ), row=2, col=1)

    fig_rdem.add_hline(y=1.0, line_dash="dash", line_color="red",
                       annotation_text="RDEM = 1", row=1, col=1)
    fig_rdem.add_vrect(x0=0, x1=winter_end_pos, fillcolor="lightblue",
                       opacity=0.08, line_width=0, row="all", col=1)
    for row in (1, 2):
        fig_rdem.update_xaxes(tickvals=tick_vals, ticktext=tick_text, row=row, col=1)
    fig_rdem.update_yaxes(title_text="RDEM (ratio)", row=1, col=1)
    fig_rdem.update_yaxes(title_text="GWh remaining", row=2, col=1)
    fig_rdem.update_layout(
        title="Security of Supply — Rolling RDEM over Winter (Oct–Mar)",
        height=680, hovermode="x unified", legend_title="Component / Scenario",
    )

    # ── fig 2: hourly supply mix vs demand (full year, starts Oct) ────────────
    n_runs = sum(1 for r in runs if r["name"] in all_gen)
    fig_comp = make_subplots(
        rows=n_runs, cols=1,
        subplot_titles=[r["name"] for r in runs if r["name"] in all_gen],
        shared_xaxes=True, vertical_spacing=0.06,
    )
    x_all = list(range(len(display_idx)))
    row = 1
    for run in runs:
        if run["name"] not in all_gen:
            continue
        gen    = all_gen[run["name"]]
        demand = all_demand[run["name"]]
        cats   = all_cats[run["name"]]
        for cat, cols in cats.items():
            if not cols:
                continue
            y = gen[cols].clip(lower=0).sum(axis=1).iloc[display_idx].reset_index(drop=True)
            if (y == 0).all():
                continue
            fig_comp.add_trace(go.Scatter(
                x=x_all, y=list(y),
                name=cat.capitalize(), legendgroup=cat, showlegend=(row == 1),
                stackgroup=f"supply_{row}", mode="lines",
                line=dict(width=0, color=CAT_COLORS.get(cat, "#adb5bd")),
                fillcolor=CAT_COLORS.get(cat, "#adb5bd"),
                hovertemplate=f"{cat}: %{{y:.2f}} GWh<extra></extra>",
            ), row=row, col=1)
        demand_r = demand.iloc[display_idx].reset_index(drop=True)
        fig_comp.add_trace(go.Scatter(
            x=x_all, y=list(demand_r),
            name="Demand", legendgroup="demand", showlegend=(row == 1),
            mode="lines", line=dict(color="black", width=2, dash="dot"),
            hovertemplate="Demand: %{y:.2f} GWh<extra></extra>",
        ), row=row, col=1)
        fig_comp.update_xaxes(tickvals=tick_vals, ticktext=tick_text, row=row, col=1)
        fig_comp.update_yaxes(title_text="GWh", row=row, col=1)
        for r in range(1, n_runs + 1):
            fig_comp.add_vrect(x0=0, x1=winter_end_pos, fillcolor="lightblue",
                               opacity=0.08, line_width=0, row=r, col=1)
        row += 1
    fig_comp.update_layout(
        title="Hourly Supply Mix vs Demand (year starts Oct)",
        height=max(300, 280 * n_runs),
        hovermode="x unified", legend_title="Technology",
    )

    # ── fig 3: worst-hour RDEM bar ────────────────────────────────────────────
    fig_worst = go.Figure(go.Bar(
        x=list(worst_rdem.keys()), y=list(worst_rdem.values()),
        marker_color=[RUN_COLORS[i % len(RUN_COLORS)] for i in range(len(worst_rdem))],
        hovertemplate="%{x}<br>Worst RDEM: %{y:.3f}<extra></extra>",
    ))
    fig_worst.add_hline(y=1.0, line_dash="dash", line_color="red",
                        annotation_text="RDEM = 1")
    fig_worst.update_layout(
        title="Worst-Hour RDEM per Scenario",
        xaxis_title="Scenario", yaxis_title="RDEM (worst hour)", height=380,
    )

    return fig_rdem, fig_comp, fig_worst
