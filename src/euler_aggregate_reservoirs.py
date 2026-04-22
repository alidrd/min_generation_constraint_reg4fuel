"""
Run on Euler: reads Reservoirs_hourly_CH_LP.xlsx, sums all plant columns per hour,
writes one CSV per sheet into the specified output directory.

Usage: python3 euler_aggregate_reservoirs.py <xlsx_path> <out_dir>
"""
import sys
import pandas as pd
from pathlib import Path

xlsx_path = Path(sys.argv[1])
out_dir   = Path(sys.argv[2])
out_dir.mkdir(parents=True, exist_ok=True)

xl = pd.ExcelFile(xlsx_path)
for sheet in xl.sheet_names:
    df = xl.parse(sheet)
    numeric = df.select_dtypes(include="number")
    total = numeric.sum(axis=1)
    total.index.name = "Hour"
    total.name = "GWh"
    out_path = out_dir / f"Reservoirs_{sheet}.csv"
    total.to_csv(out_path, header=True)
    print(f"  {sheet}: {len(total)} rows -> {out_path.name}")

print("Done.")
