# Min Generation Constraint — reg4fuel

Streamlit dashboard for analysing nexus-e energy model simulation results,
focused on the effect of minimum local generation constraints in Switzerland.

## Overview

The workflow has three stages:

1. **Pipeline** — fetches data from remote sources and caches it locally as parquet/CSV
2. **Cache** — local storage at `cache/` (git-ignored); input/output MySQL tables and Euler result files
3. **Dashboard** — Streamlit app that reads only from cache and plots results

## Data sources

| Source | What | How accessed |
|--------|------|--------------|
| MySQL input DB (`base_3_26_reg4fuel`) | Model configuration (generators, loads, topology) | SSH tunnel via paramiko → pymysql |
| MySQL output DB (one per run) | Simulation results (prices, generation, curtailment, …) | Same |
| Euler HPC (`eu-login-44`) | Reservoir SOC files too large for DB (aggregated on Euler before download) | paramiko SFTP/SSH |

Credentials are read from `C:\DB\UserDBInfoNew.txt` (output DB) and
`C:\DB\UserDBInfoNew_Input.txt` (input DB) — 4-line format: host, port, user, password.

## Repository structure

```
app.py                        Streamlit entry point
pipeline.py                   CLI data-fetch pipeline
plots.yaml                    Active scenarios and plots (edit to switch runs)
run_order.yaml                Display order for runs (composite key: input_db|run_name)
manifests/                    nexus-e manifest files (one per multi-run batch)
src/
  manifest_parser.py          Parse manifest files, extract completed runs
  db_connection.py            MySQL connection (reads credential files)
  euler_connection.py         SSH connection to Euler
  fetcher.py                  Fetch DB tables and Euler files into cache
  cache.py                    Read/write parquet and Euler file cache
  euler_aggregate_reservoirs.py  Aggregation script uploaded and run on Euler
  plots/
    security_supply.py        Plot 1 — Security of Supply (RDEM)
    market_prices.py          Plot 6 — electricity market prices
    hydro_soc.py              Plot 3 — reservoir state of charge
    curtailment.py            Plot 11 — curtailment & spillage
```

## Setup

```bash
pip install -r requirements.txt
```

## Running the pipeline

```bash
# Fetch DB + Euler files for specific runs
python pipeline.py --manifest manifests/<file>.manifest --runs centiv_2050_minLocal10,centiv_2050_minLocal20

# Skip Euler (DB only)
python pipeline.py --manifest manifests/<file>.manifest --skip-euler

# Force re-download everything
python pipeline.py --manifest manifests/<file>.manifest --force
```

## Running the dashboard

```bash
streamlit run app.py
```

Then edit `plots.yaml` to switch which runs and plots are active — no restart needed,
just reload the browser.

## Configuring runs

**`plots.yaml`** — controls what the dashboard shows:
```yaml
manifest: manifests/multiRun_20260421_162606.manifest
runs_to_compare:
  - centiv_2050_minLocal10
  - centiv_2050_minLocal20
active_plots:
  - market_prices
  - hydro_soc
  - curtailment
  # ... (comment out to hide)
```

**`run_order.yaml`** — controls display order in all plots:
```yaml
order:
  - "base_3_26_reg4fuel|centiv_2050_minLocal10"
  - "base_3_26_reg4fuel|centiv_2050_minLocal20"
```

## Security of Supply — RDEM definition

The **Security of Supply** plot (`security_supply`) computes a rolling
**Remaining Domestic Energy Margin (RDEM)** for each hour *t* in the winter
period (October–March).

### Equation

```
RDEM(t) = remaining_supply(t) / remaining_demand(t)
```

where the **remaining supply** has three components, following the structure of
the `set_min_local_generation` constraint in the nexus-e model:

```
remaining_supply(t) = SOC(t)
                    + Σ_{h=t}^{T_end}  [ PV(h) + Wind(h) + RoR(h) ]
                    + P_conv  ×  (T_end − t)
```

| Term | Description | Source |
|------|-------------|--------|
| **SOC(t)** | Total energy stored in Swiss hydro reservoirs (dam + pump + daily-pump) at hour *t*, in GWh | Euler hourly CSV files (MWh ÷ 1000) |
| **Σ RES(h)** | Remaining renewable generation potential from *t* to end of winter: PV (all types), Wind (on/offshore), Run-of-River | Actual hourly dispatch from output DB (proxy for potential) |
| **P_conv × (T_end − t)** | Conventional generation potential: installed capacity (GW) × hours remaining | `national_capacity_gw_ch` output table; includes Gas, Nuclear, Waste, Coal, Geothermal, etc. |

**Remaining demand:**

```
remaining_demand(t) = Σ_{h=t}^{T_end}  Load_Total(h)
```

from the `national_generation_hourly_gwh_c_ch_2050` output table
(`Load (Total)` column, GWh).

### Interpretation

- RDEM > 1 → Switzerland has surplus domestic energy potential for the rest of winter.
- RDEM < 1 → domestic potential is insufficient; imports required to cover demand.
- The **worst hour** (minimum RDEM) is marked with a star on the ratio plot.
- Higher minimum local generation targets (`minLocalXX`) raise the constraint
  floor on domestic production; this is expected to raise RDEM, particularly
  by increasing conventional (thermal/storage) dispatch during winter.

### Simplifications

- Future hydro inflows from *t* to end-of-winter are not included. The SOC already
  reflects inflows up to *t*; adding expected future inflows would require water-profile
  data from the input DB and is left as a future improvement.
- RES remaining uses actual dispatch, not curtailment-corrected potential. In hours
  where RES is curtailed, actual < potential, so RDEM is slightly underestimated.

## Notes

- MySQL database names are capped at 64 characters; output DB folder names in `cache/` may appear truncated — this is expected.
- The manifest `Results:` path does not match the actual Euler directory structure. `fetcher.py` resolves this automatically by navigating one level up and matching subdirectory names.
- Reservoir SOC data (`Reservoirs_hourly_CH_LP.xlsx`) is aggregated on Euler before download to avoid transferring the full 13 MB per-plant file. The aggregation script (`euler_aggregate_reservoirs.py`) is uploaded and executed automatically by the pipeline.
