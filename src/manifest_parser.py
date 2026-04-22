import re
import yaml
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ORDER_FILE = Path(__file__).parent.parent / "run_order.yaml"


@dataclass
class Run:
    name: str
    input_db: str
    output_db: str
    euler_path: str
    job_id: str


def parse_manifest(path: str | Path) -> list[Run]:
    """Parse a nexus-e manifest file and return only completed runs."""
    text = Path(path).read_text()

    blocks = re.split(r"(?=\[COMPLETE\])", text)

    runs = []
    for block in blocks:
        if not block.startswith("[COMPLETE]"):
            continue

        header = block.splitlines()[0]

        job_match       = re.search(r"Job (\d+)",         header)
        db_match        = re.search(r"DB:\s+(\S+)",       header)
        playlist_match  = re.search(r"Playlist:\s+(\S+)", header)
        results_match   = re.search(r"Results:\s+(\S+)",  block)
        webviewer_match = re.search(r"Webviewer:\s+\S+/([^\s/]+)\s*$", block, re.MULTILINE)

        if not all([job_match, db_match, playlist_match, results_match, webviewer_match]):
            continue

        runs.append(Run(
            name       = playlist_match.group(1),
            input_db   = db_match.group(1),
            output_db  = webviewer_match.group(1),
            # TODO: manifest Results: path points to run_XXXX/Results/<run_name> but actual files
            # are one level deeper under Results/<input_db>_<timestamp>/<playlist>/
            # e.g. Results/base_3_26_reg4fuel_2026-04-21T16-27-48/CentIv_2050/
            # Needs investigation — euler_path below may be wrong for file fetching.
            euler_path = results_match.group(1),
            job_id     = job_match.group(1),
        ))

    return runs


def apply_order(runs: list[Run], order_file: str | Path = DEFAULT_ORDER_FILE) -> list[Run]:
    """Sort runs by the order defined in run_order.yaml.
    Keys are 'input_db|name'. Unlisted runs are appended at the end in manifest order."""
    order_path = Path(order_file)
    if not order_path.exists():
        return runs

    with open(order_path) as f:
        order = yaml.safe_load(f).get("order", [])

    rank = {key: i for i, key in enumerate(order)}
    n = len(order)

    return sorted(runs, key=lambda r: (rank.get(f"{r.input_db}|{r.name}", n), r.name))


def unique_input_dbs(runs: list[Run]) -> list[str]:
    seen = set()
    return [r.input_db for r in runs if not (r.input_db in seen or seen.add(r.input_db))]


if __name__ == "__main__":
    import sys
    runs = parse_manifest(sys.argv[1])
    runs = apply_order(runs)
    for r in runs:
        print(r)
    print(f"\n{len(runs)} completed runs | input DBs: {unique_input_dbs(runs)}")
