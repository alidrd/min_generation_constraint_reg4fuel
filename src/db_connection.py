import os
from pathlib import Path
import pymysql

# Override these paths via environment variables or edit here
CREDENTIALS_OUTPUT = Path(os.getenv("DB_CREDS_OUTPUT", r"C:\DB\UserDBInfoNew.txt"))
CREDENTIALS_INPUT  = Path(os.getenv("DB_CREDS_INPUT",  r"C:\DB\UserDBInfoNew_Input.txt"))


def _parse_credentials(path: Path) -> dict:
    lines = [l.strip() for l in path.read_text().splitlines() if l.strip()]
    # Expected line order: host, port, user, password
    return {
        "host":     lines[0],
        "port":     int(lines[1]),
        "user":     lines[2],
        "password": lines[3],
    }


def get_connection(db_type: str = "output", database: str = None) -> pymysql.Connection:
    path = CREDENTIALS_OUTPUT if db_type == "output" else CREDENTIALS_INPUT
    creds = _parse_credentials(path)
    return pymysql.connect(
        host=creds["host"],
        port=creds["port"],
        user=creds["user"],
        password=creds["password"],
        database=database,
        connect_timeout=10,
    )


def test_connections(
    input_db: str = "base_3_26_reg4fuel",
    output_db: str = "2026-04-21t09-57-09_run_base_3_26_centiv_2050_minlocal10",
) -> None:
    for db_type, database in (("input", input_db), ("output", output_db)):
        try:
            conn = get_connection(db_type, database)
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                version = cur.fetchone()[0]
                cur.execute("SHOW TABLES")
                tables = [r[0] for r in cur.fetchall()]
            conn.close()
            print(f"[{db_type}] OK — MySQL {version} | db={database} | tables={tables}")
        except Exception as e:
            print(f"[{db_type}] FAILED — {e}")


if __name__ == "__main__":
    test_connections()
