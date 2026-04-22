from pathlib import Path
import pandas as pd

CACHE_ROOT = Path(__file__).parent.parent / "cache"


def _input_dir(input_db: str) -> Path:
    return CACHE_ROOT / "input_dbs" / input_db


def _output_dir(output_db: str) -> Path:
    return CACHE_ROOT / "output_dbs" / output_db


def _euler_dir(run_name: str) -> Path:
    return CACHE_ROOT / "euler" / run_name


def is_table_cached(db_type: str, db_name: str, table: str) -> bool:
    root = _input_dir(db_name) if db_type == "input" else _output_dir(db_name)
    return (root / f"{table}.parquet").exists()


def is_euler_file_cached(run_name: str, filename: str) -> bool:
    return (_euler_dir(run_name) / filename).exists()


def save_table(db_type: str, db_name: str, table: str, df: pd.DataFrame) -> None:
    root = _input_dir(db_name) if db_type == "input" else _output_dir(db_name)
    root.mkdir(parents=True, exist_ok=True)
    df.to_parquet(root / f"{table}.parquet", index=False)


def load_table(db_type: str, db_name: str, table: str) -> pd.DataFrame:
    root = _input_dir(db_name) if db_type == "input" else _output_dir(db_name)
    return pd.read_parquet(root / f"{table}.parquet")


def save_euler_file(run_name: str, filename: str, content: bytes) -> Path:
    dest = _euler_dir(run_name) / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)
    return dest


def euler_file_path(run_name: str, filename: str) -> Path:
    return _euler_dir(run_name) / filename


def cached_tables(db_type: str, db_name: str) -> list[str]:
    root = _input_dir(db_name) if db_type == "input" else _output_dir(db_name)
    if not root.exists():
        return []
    return [p.stem for p in root.glob("*.parquet")]
