"""
pipeline.py  —  fetch and cache data for nexus-e runs

Usage:
    python pipeline.py --manifest manifests/20260421_095550.txt
    python pipeline.py --manifest manifests/20260421_095550.txt --runs centiv_2050_minLocal10,centiv_2050_minLocal20
    python pipeline.py --manifest manifests/20260421_095550.txt --force
    python pipeline.py --manifest manifests/20260421_095550.txt --skip-euler
"""
import argparse
import sys
import socket
from pathlib import Path
import paramiko

sys.path.insert(0, str(Path(__file__).parent / "src"))

from manifest_parser import parse_manifest, unique_input_dbs
from fetcher import fetch_all_tables, fetch_euler_files, run_euler_aggregations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest",   required=True, help="Path to manifest file")
    parser.add_argument("--runs",       default="",   help="Comma-separated run names to process (default: all completed)")
    parser.add_argument("--force",      action="store_true", help="Re-download even if cached")
    parser.add_argument("--skip-euler", action="store_true", help="Skip Euler file download")
    parser.add_argument("--skip-db",    action="store_true", help="Skip MySQL download")
    args = parser.parse_args()

    all_runs = parse_manifest(args.manifest)
    if not all_runs:
        print("No completed runs found in manifest.")
        return

    # Filter to requested runs
    if args.runs:
        names = {n.strip() for n in args.runs.split(",")}
        runs = [r for r in all_runs if r.name in names]
        if not runs:
            print(f"No matching runs found for: {names}")
            return
    else:
        runs = all_runs

    print(f"\nManifest: {args.manifest}")
    print(f"Runs to process: {[r.name for r in runs]}")

    # --- Input DBs (shared, download once per unique db) ---
    if not args.skip_db:
        seen_input = set()
        for run in runs:
            if run.input_db in seen_input:
                continue
            seen_input.add(run.input_db)
            print(f"\n[input DB] {run.input_db}")
            fetch_all_tables("input", run.input_db, force=args.force)

        # --- Output DBs (one per run) ---
        for run in runs:
            print(f"\n[output DB] {run.output_db}  ({run.name})")
            fetch_all_tables("output", run.output_db, force=args.force)

    # --- Euler files ---
    if not args.skip_euler:
        for run in runs:
            print(f"\n[Euler] {run.name}  ->  {run.euler_path}")
            try:
                fetch_euler_files(run.name, run.euler_path, force=args.force)
                run_euler_aggregations(run.name, run.euler_path, force=args.force)
            except (TimeoutError, ConnectionError, OSError,
                    socket.error, paramiko.SSHException) as e:
                print(f"  [warn] Euler connection failed for {run.name}: {e}")
                print(f"  [warn] Skipping Euler step — re-run pipeline when connectivity is restored.")

    print("\nDone.")


if __name__ == "__main__":
    main()
