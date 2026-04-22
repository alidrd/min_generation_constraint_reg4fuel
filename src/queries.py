import pandas as pd
from db_connection import get_connection


def get_base_demand_by_country(input_db: str) -> pd.DataFrame:
    """Sum of Pd (MW) per country from loaddata, for each config year."""
    conn = get_connection("input", input_db)
    df = pd.read_sql("""
        SELECT lci.year,
               SUBSTRING_INDEX(ld.LoadType, '_', 1) AS country,
               SUM(ld.Pd) AS base_demand_MW
        FROM loadconfiguration lc
        JOIN loaddata ld ON lc.idLoad = ld.idLoad
        JOIN loadconfiginfo lci ON lc.idLoadConfig = lci.idLoadConfig
        GROUP BY lci.year, country
        HAVING base_demand_MW > 0
        ORDER BY lci.year, country
    """, conn)
    conn.close()
    # Annual TWh = avg_MW * 8760 / 1000
    df["annual_demand_TWh"] = df["base_demand_MW"] * 8760 / 1000
    return df


def get_demand_projections(input_db: str) -> pd.DataFrame:
    """Demand projection factors by item and scenario over all years."""
    conn = get_connection("input", input_db)
    df = pd.read_sql("""
        SELECT item, scenario, year, value
        FROM projections
        WHERE item IN ('dem_ele', 'dem_ene')
        ORDER BY item, scenario, year
    """, conn)
    conn.close()
    return df
