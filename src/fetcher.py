import io
import pandas as pd
import pymysql
from pathlib import Path

from db_connection import get_connection
from euler_connection import get_client
from cache import (
    is_table_cached, save_table,
    is_euler_file_cached, save_euler_file,
)


def fetch_all_tables(db_type: str, db_name: str, force: bool = False) -> list[str]:
    """Fetch all tables from a MySQL DB into cache. Skips already-cached tables unless force=True."""
    conn = get_connection(db_type, db_name)
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = [r[0] for r in cur.fetchall()]
    conn.close()

    fetched = []
    for table in tables:
        if not force and is_table_cached(db_type, db_name, table):
            print(f"  [skip] {db_type}/{db_name}/{table} (cached)")
            continue
        print(f"  [fetch] {db_type}/{db_name}/{table} ...", end=" ", flush=True)
        conn = get_connection(db_type, db_name)
        df = pd.read_sql(f"SELECT * FROM `{table}`", conn)
        conn.close()
        save_table(db_type, db_name, table, df)
        print(f"{len(df)} rows")
        fetched.append(table)

    return fetched


def _resolve_euler_path(sftp, euler_path: str, run_name: str) -> str | None:
    """Resolve the actual Euler results path.

    The manifest Results: line points to a non-existent path like:
      .../Results/run_{input_db}_{playlist}
    The real files live two levels deeper:
      .../Results/{only_subdir}/{playlist_subdir}/
    There is exactly one subdir under Results/; within it we pick the subdir
    whose name is a case-insensitive prefix of run_name.
    """
    # Try the manifest path directly first
    try:
        sftp.stat(euler_path)
        return euler_path
    except FileNotFoundError:
        pass

    results_dir = euler_path.rsplit("/", 1)[0]
    try:
        subdirs = sftp.listdir(results_dir)
    except FileNotFoundError:
        return None

    if not subdirs:
        return None

    # There is only one subdir under Results/
    timestamp_dir = f"{results_dir}/{subdirs[0]}"
    try:
        inner = sftp.listdir(timestamp_dir)
    except FileNotFoundError:
        return None

    run_lower = run_name.lower()
    # Pick playlist subdir whose name is a case-insensitive prefix of run_name
    for sub in inner:
        if run_lower.startswith(sub.lower()):
            return f"{timestamp_dir}/{sub}"
    # Fallback: first subdir that isn't an investment run
    for sub in inner:
        if "investment" not in sub.lower():
            return f"{timestamp_dir}/{sub}"

    return None


_SCRIPT_LOCAL  = Path(__file__).parent / "euler_aggregate_reservoirs.py"
_SCRIPT_REMOTE = "/cluster/home/adarudi/euler_aggregate_reservoirs.py"

# Aggregations to run: (local_xlsx_name, remote_out_subdir)
_AGGREGATIONS = [
    ("Reservoirs_hourly_CH_LP.xlsx", "reservoirs_agg"),
]


def run_euler_aggregations(run_name: str, euler_path: str, force: bool = False) -> list[str]:
    """Run aggregation scripts on Euler and download resulting CSVs into cache."""
    fetched = []
    with get_client() as client:
        sftp = client.open_sftp()

        resolved = _resolve_euler_path(sftp, euler_path, run_name)
        if resolved is None:
            print(f"  [warn] Euler path not found for aggregation: {euler_path}")
            sftp.close()
            return []

        # Upload aggregation script (always refresh to pick up any local edits)
        sftp.put(str(_SCRIPT_LOCAL), _SCRIPT_REMOTE)

        for xlsx_name, out_subdir in _AGGREGATIONS:
            xlsx_remote = f"{resolved}/{xlsx_name}"
            out_remote  = f"/cluster/home/adarudi/{out_subdir}/{run_name}"

            # Check if all output CSVs are already cached
            _, stdout, _ = client.exec_command(f"ls {out_remote}/*.csv 2>/dev/null")
            remote_csvs = [Path(p.strip()).name for p in stdout.read().decode().splitlines() if p.strip()]
            if not force and remote_csvs and all(is_euler_file_cached(run_name, f) for f in remote_csvs):
                for f in remote_csvs:
                    print(f"  [skip] euler/{run_name}/{f} (cached)")
                continue

            print(f"  [aggregate] {xlsx_name} ...", flush=True)
            _, stdout, stderr = client.exec_command(
                f"python3 {_SCRIPT_REMOTE} \"{xlsx_remote}\" \"{out_remote}\""
            )
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            if out:
                for line in out.splitlines():
                    print(f"    {line}")

            # Download produced CSVs
            try:
                csv_files = [f for f in sftp.listdir(out_remote) if f.endswith(".csv")]
            except FileNotFoundError:
                print(f"  [warn] Aggregation output dir not found: {out_remote}")
                if err:
                    print(f"  STDERR: {err}")
                continue

            for fname in csv_files:
                if not force and is_euler_file_cached(run_name, fname):
                    print(f"  [skip] euler/{run_name}/{fname} (cached)")
                    continue
                with sftp.open(f"{out_remote}/{fname}") as f:
                    content = f.read()
                save_euler_file(run_name, fname, content)
                print(f"  [fetch] euler/{run_name}/{fname}  ({len(content) // 1024} KB)")
                fetched.append(fname)

        sftp.close()
    return fetched


def fetch_euler_files(run_name: str, euler_path: str, force: bool = False) -> list[str]:
    """Download CSV/Excel files from an Euler results folder into cache."""
    fetched = []
    with get_client() as client:
        sftp = client.open_sftp()

        resolved = _resolve_euler_path(sftp, euler_path, run_name)
        if resolved is None:
            print(f"  [warn] Euler path not found: {euler_path}")
            sftp.close()
            return []
        if resolved != euler_path:
            print(f"  [info] resolved Euler path: {resolved}")

        try:
            entries = sftp.listdir_attr(resolved)
        except FileNotFoundError:
            print(f"  [warn] Euler path not found: {resolved}")
            sftp.close()
            return []

        for entry in entries:
            filename = entry.filename
            if not any(filename.endswith(ext) for ext in (".csv", ".xlsx", ".xls")):
                continue
            if not force and is_euler_file_cached(run_name, filename):
                print(f"  [skip] euler/{run_name}/{filename} (cached)")
                continue
            print(f"  [fetch] euler/{run_name}/{filename} ...", end=" ", flush=True)
            with sftp.open(f"{resolved}/{filename}") as f:
                content = f.read()
            save_euler_file(run_name, filename, content)
            print(f"{len(content) // 1024} KB")
            fetched.append(filename)

        sftp.close()
    return fetched
