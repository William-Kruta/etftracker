import json
import os
import platform
from pathlib import Path

import duckdb
import pandas as pd

ENV_VAR = "ETFTRACKER_DB"
CONFIG_DIR = "etftracker"
CONFIG_FILE = "config.json"
DEFAULT_DB = "etftracker.duckdb"
TABLE_NAME = "etf_holdings"


def _get_config_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        base = os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming")
    elif system == "Darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")

    return Path(base) / CONFIG_DIR


def get_db_path() -> Path:
    env_path = os.environ.get(ENV_VAR, "")
    if env_path:
        return Path(env_path)

    config_dir = _get_config_dir()
    config_path = config_dir / CONFIG_FILE

    if config_path.exists():
        try:
            with open(config_path, encoding="utf-8") as file:
                config = json.load(file)
            db_path = config.get("database", "")
            if db_path:
                return Path(db_path)
        except (json.JSONDecodeError, OSError):
            pass

    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / DEFAULT_DB


def _parse_scaled_number(value: str) -> float | None:
    if value is None:
        return None

    cleaned = str(value).strip().replace("$", "").replace(",", "")
    if not cleaned or cleaned == "--":
        return None

    multiplier = 1.0
    suffix = cleaned[-1].upper()
    if suffix == "K":
        multiplier = 1_000.0
        cleaned = cleaned[:-1]
    elif suffix == "M":
        multiplier = 1_000_000.0
        cleaned = cleaned[:-1]
    elif suffix == "B":
        multiplier = 1_000_000_000.0
        cleaned = cleaned[:-1]
    elif suffix == "T":
        multiplier = 1_000_000_000_000.0
        cleaned = cleaned[:-1]

    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def _parse_percent(value: str) -> float | None:
    if value is None:
        return None

    cleaned = str(value).strip().replace("%", "")
    if not cleaned or cleaned == "--":
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_pandas(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df.to_pandas()
    if isinstance(df, pd.DataFrame):
        return df.copy()
    raise TypeError("df must be a pandas.DataFrame or polars.DataFrame")


def normalize_holdings_frame(
    df: pd.DataFrame | pl.DataFrame, etf_symbol: str
) -> pd.DataFrame:
    holdings = _to_pandas(df)
    required_columns = ["symbol", "name", "weight", "shares_owned", "shares_value"]
    missing = [column for column in required_columns if column not in holdings.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    normalized = holdings.loc[:, required_columns].copy()
    normalized["etf_ticker"] = etf_symbol.upper()
    normalized["collected_at"] = pd.Timestamp.utcnow()
    normalized["weight_pct"] = normalized["weight"].map(_parse_percent)
    normalized["shares_owned_num"] = normalized["shares_owned"].map(
        _parse_scaled_number
    )
    normalized["shares_value_num"] = normalized["shares_value"].map(
        _parse_scaled_number
    )

    return normalized[
        [
            "etf_ticker",
            "collected_at",
            "symbol",
            "name",
            "weight",
            "weight_pct",
            "shares_owned",
            "shares_owned_num",
            "shares_value",
            "shares_value_num",
        ]
    ]


def get_connection(db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    resolved_path = Path(db_path) if db_path else get_db_path()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved_path))


def create_holdings_table(
    conn: duckdb.DuckDBPyConnection, table_name: str = TABLE_NAME
) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            etf_ticker TEXT NOT NULL,
            collected_at TIMESTAMP NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            weight TEXT,
            weight_pct DOUBLE,
            shares_owned TEXT,
            shares_owned_num DOUBLE,
            shares_value TEXT,
            shares_value_num DOUBLE,
            PRIMARY KEY (etf_ticker, symbol)
        )
        """
    )


def save_holdings(
    df: pd.DataFrame | pl.DataFrame,
    etf_symbol: str,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
) -> int:
    normalized = normalize_holdings_frame(df=df, etf_symbol=etf_symbol)
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(conn=conn, table_name=table_name)
        conn.register("holdings_df", normalized)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                etf_ticker,
                collected_at,
                symbol,
                name,
                weight,
                weight_pct,
                shares_owned,
                shares_owned_num,
                shares_value,
                shares_value_num
            )
            SELECT
                etf_ticker,
                collected_at,
                symbol,
                name,
                weight,
                weight_pct,
                shares_owned,
                shares_owned_num,
                shares_value,
                shares_value_num
            FROM holdings_df
            """
        )
    finally:
        conn.close()

    return len(normalized)


def read_holdings(
    etf_ticker: str,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(conn=conn, table_name=table_name)
        result = conn.execute(
            f"SELECT * FROM {table_name} WHERE etf_ticker = ? ORDER BY collected_at DESC",
            [etf_ticker.upper()],
        )
        return result.fetch_df()
    finally:
        conn.close()


def delete_holding(
    etf_ticker: str,
    symbol: str,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
) -> int:
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(conn=conn, table_name=table_name)
        result = conn.execute(
            f"DELETE FROM {table_name} WHERE etf_ticker = ? AND symbol = ?",
            [etf_ticker.upper(), symbol.upper()],
        )
        return result.rowcount
    finally:
        conn.close()
