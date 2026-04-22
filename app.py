import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import yaml
import streamlit as st
from manifest_parser import parse_manifest, apply_order

st.set_page_config(page_title="Min Generation Constraint", layout="wide")

# --- Load config ---
with open(Path(__file__).parent / "plots.yaml") as f:
    config = yaml.safe_load(f)

project_root = Path(__file__).parent
runs_all     = apply_order(parse_manifest(project_root / config["manifest"]))
run_map      = {r.name: r for r in runs_all}

selected_names = config.get("runs_to_compare", [r.name for r in runs_all])
active_plots   = config.get("active_plots", [])
selected_runs  = [{"name": n, "output_db": run_map[n].output_db}
                  for n in selected_names if n in run_map]

# --- Sidebar ---
PLOT_LABELS = {
    "security_supply":    "1. Security of Supply (RDEM)",
    "shadow_prices":      "2. Shadow Prices",
    "hydro_soc":          "3. Hydro SOC",
    "investment":         "4. Investment Patterns",
    "market_dispatch":    "5. Market Dispatch",
    "market_prices":      "6. Market Prices",
    "trade":              "7. Trade Patterns",
    "profits":            "8. Profits by Firm Type",
    "system_cost":        "9. Total System Cost",
    "efficiency_security":"10. Efficiency vs Security",
    "curtailment":        "11. Curtailment & Spillage",
}

available = [k for k in PLOT_LABELS if k in active_plots]

with st.sidebar:
    st.title("Min Generation Constraint")
    st.caption(f"Runs: {len(selected_runs)}  |  Active plots: {len(available)}")
    st.divider()
    if available:
        selected_plot = st.radio(
            "Navigate to",
            options=available,
            format_func=lambda k: PLOT_LABELS[k],
        )
    else:
        st.warning("No active plots in plots.yaml")
        selected_plot = None
    st.divider()
    with st.expander("Runs"):
        for r in selected_runs:
            st.caption(r["name"])

# --- Main area ---
st.title("Min Generation Constraint — reg4fuel")

if selected_plot is None:
    st.info("Enable plots in plots.yaml to get started.")

elif selected_plot == "curtailment":
    from plots.curtailment import plot_curtailment_ch
    st.header(PLOT_LABELS["curtailment"])
    fig_hourly, fig_monthly = plot_curtailment_ch(selected_runs)
    st.subheader("Monthly overview — heatmap (all runs)")
    st.plotly_chart(fig_monthly, use_container_width=True)
    st.subheader("Hourly detail (per run)")
    st.plotly_chart(fig_hourly, use_container_width=True)

elif selected_plot == "security_supply":
    st.header(PLOT_LABELS["security_supply"])
    st.info("Coming soon.")

elif selected_plot == "shadow_prices":
    st.header(PLOT_LABELS["shadow_prices"])
    st.info("Coming soon.")

elif selected_plot == "hydro_soc":
    from plots.hydro_soc import plot_storage_soc_ch
    st.header(PLOT_LABELS["hydro_soc"])
    result = plot_storage_soc_ch(selected_runs)
    if result is None:
        st.warning("No reservoir SOC files found in cache — run the pipeline with Euler enabled.")
    else:
        fig_total, fig_hourly = result
        st.subheader("Total SOC — all reservoir types combined")
        st.plotly_chart(fig_total, use_container_width=True)
        st.subheader("Hourly SOC by reservoir type")
        st.plotly_chart(fig_hourly, use_container_width=True)

elif selected_plot == "investment":
    st.header(PLOT_LABELS["investment"])
    st.info("Coming soon.")

elif selected_plot == "market_dispatch":
    st.header(PLOT_LABELS["market_dispatch"])
    st.info("Coming soon.")

elif selected_plot == "market_prices":
    from plots.market_prices import plot_switzerland, plot_all_countries
    st.header(PLOT_LABELS["market_prices"])
    tab_ch, tab_all = st.tabs(["✚ Switzerland", "🌐 All Countries"])
    with tab_ch:
        fig_annual, fig_heatmap, fig_violin, fig_seasonal, fig_monthly, fig_hourly = plot_switzerland(selected_runs)
        st.subheader("Annual average (overview)")
        st.plotly_chart(fig_annual, use_container_width=True)
        st.subheader("Monthly heatmap (runs × months)")
        st.plotly_chart(fig_heatmap, use_container_width=True)
        st.subheader("Price distribution by month")
        st.plotly_chart(fig_violin, use_container_width=True)
        st.subheader("Seasonal averages")
        st.plotly_chart(fig_seasonal, use_container_width=True)
        st.subheader("Monthly — runs compared")
        st.plotly_chart(fig_monthly, use_container_width=True)
        st.subheader("Hourly detail")
        st.plotly_chart(fig_hourly, use_container_width=True)
    with tab_all:
        fig_annual, fig_heatmap, fig_violin, fig_seasonal, fig_monthly = plot_all_countries(selected_runs)
        st.subheader("Annual averages (overview)")
        st.plotly_chart(fig_annual, use_container_width=True)
        st.subheader("Monthly heatmap (runs × months)")
        st.plotly_chart(fig_heatmap, use_container_width=True)
        st.subheader("Price distribution by month")
        st.plotly_chart(fig_violin, use_container_width=True)
        st.subheader("Seasonal averages")
        st.plotly_chart(fig_seasonal, use_container_width=True)
        st.subheader("Monthly — runs compared")
        st.plotly_chart(fig_monthly, use_container_width=True)

elif selected_plot == "trade":
    st.header(PLOT_LABELS["trade"])
    st.info("Coming soon.")

elif selected_plot == "profits":
    st.header(PLOT_LABELS["profits"])
    st.info("Coming soon.")

elif selected_plot == "system_cost":
    st.header(PLOT_LABELS["system_cost"])
    st.info("Coming soon.")

elif selected_plot == "efficiency_security":
    st.header(PLOT_LABELS["efficiency_security"])
    st.info("Coming soon.")
