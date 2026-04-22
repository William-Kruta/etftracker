from __future__ import annotations

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
COMPANY_TABLE_NAME = "companies"


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


def _to_pandas(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df.copy()
    raise TypeError("df must be a pandas.DataFrame or polars.DataFrame")


def normalize_holdings_frame(df: pd.DataFrame, etf_symbol: str) -> pd.DataFrame:
    holdings = _to_pandas(df)
    required_columns = ["symbol", "name", "weight", "shares_owned", "shares_value"]
    missing = [column for column in required_columns if column not in holdings.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    normalized = holdings.loc[:, required_columns].copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
    normalized["name"] = normalized["name"].astype(str).str.strip()
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
    conn: duckdb.DuckDBPyConnection,
    table_name: str = TABLE_NAME,
    company_table_name: str = COMPANY_TABLE_NAME,
) -> None:
    _migrate_legacy_holdings_table(
        conn=conn, table_name=table_name, company_table_name=company_table_name
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {company_table_name} (
            symbol TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            etf_ticker TEXT NOT NULL,
            collected_at TIMESTAMP NOT NULL,
            symbol TEXT NOT NULL,
            weight TEXT,
            weight_pct DOUBLE,
            shares_owned TEXT,
            shares_owned_num DOUBLE,
            shares_value TEXT,
            shares_value_num DOUBLE,
            PRIMARY KEY (etf_ticker, collected_at, symbol),
            FOREIGN KEY (symbol) REFERENCES {company_table_name}(symbol)
        )
        """
    )


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    )
    return result.fetchone()[0] > 0


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    if not _table_exists(conn=conn, table_name=table_name):
        return []

    result = conn.execute(f"PRAGMA table_info('{table_name}')")
    return [row[1] for row in result.fetchall()]


def _migrate_legacy_holdings_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    company_table_name: str,
) -> None:
    columns = _table_columns(conn=conn, table_name=table_name)
    if not columns or "name" not in columns:
        return

    backup_table_name = f"{table_name}_legacy"
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {company_table_name} (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {company_table_name} (symbol, name)
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                MAX(COALESCE(NULLIF(TRIM(name), ''), UPPER(TRIM(symbol)))) AS name
            FROM {table_name}
            WHERE symbol IS NOT NULL
                AND TRIM(symbol) != ''
            GROUP BY UPPER(TRIM(symbol))
            """
        )
        conn.execute(f"ALTER TABLE {table_name} RENAME TO {backup_table_name}")
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                etf_ticker TEXT NOT NULL,
                collected_at TIMESTAMP NOT NULL,
                symbol TEXT NOT NULL,
                weight TEXT,
                weight_pct DOUBLE,
                shares_owned TEXT,
                shares_owned_num DOUBLE,
                shares_value TEXT,
                shares_value_num DOUBLE,
                PRIMARY KEY (etf_ticker, collected_at, symbol),
                FOREIGN KEY (symbol) REFERENCES {company_table_name}(symbol)
            )
            """
        )
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                etf_ticker,
                collected_at,
                symbol,
                weight,
                weight_pct,
                shares_owned,
                shares_owned_num,
                shares_value,
                shares_value_num
            )
            SELECT
                UPPER(TRIM(etf_ticker)) AS etf_ticker,
                collected_at,
                UPPER(TRIM(symbol)) AS symbol,
                weight,
                weight_pct,
                shares_owned,
                shares_owned_num,
                shares_value,
                shares_value_num
            FROM {backup_table_name}
            WHERE symbol IS NOT NULL
                AND TRIM(symbol) != ''
            """
        )
        conn.execute(f"DROP TABLE {backup_table_name}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def save_holdings(
    df: pd.DataFrame,
    etf_symbol: str,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
    company_table_name: str = COMPANY_TABLE_NAME,
) -> int:
    normalized = normalize_holdings_frame(df=df, etf_symbol=etf_symbol)
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(
            conn=conn, table_name=table_name, company_table_name=company_table_name
        )
        conn.register("holdings_df", normalized)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {company_table_name} (symbol, name)
            SELECT
                symbol,
                MAX(COALESCE(NULLIF(name, ''), symbol)) AS name
            FROM holdings_df
            GROUP BY symbol
            """
        )
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                etf_ticker,
                collected_at,
                symbol,
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
    company_table_name: str = COMPANY_TABLE_NAME,
) -> pd.DataFrame:
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(
            conn=conn, table_name=table_name, company_table_name=company_table_name
        )
        result = conn.execute(
            f"""
            SELECT
                h.etf_ticker,
                h.collected_at,
                h.symbol,
                c.name,
                h.weight,
                h.weight_pct,
                h.shares_owned,
                h.shares_owned_num,
                h.shares_value,
                h.shares_value_num
            FROM {table_name} h
            JOIN {company_table_name} c ON h.symbol = c.symbol
            WHERE h.etf_ticker = ?
                AND h.collected_at = (
                    SELECT MAX(collected_at)
                    FROM {table_name}
                    WHERE etf_ticker = ?
                )
            ORDER BY h.weight_pct DESC NULLS LAST, h.symbol
            """,
            [etf_ticker.upper(), etf_ticker.upper()],
        )
        return result.fetch_df()
    finally:
        conn.close()


def read_holdings_history(
    etf_ticker: str,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
    company_table_name: str = COMPANY_TABLE_NAME,
) -> pd.DataFrame:
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(
            conn=conn, table_name=table_name, company_table_name=company_table_name
        )
        result = conn.execute(
            f"""
            SELECT
                h.etf_ticker,
                h.collected_at,
                h.symbol,
                c.name,
                h.weight,
                h.weight_pct,
                h.shares_owned,
                h.shares_owned_num,
                h.shares_value,
                h.shares_value_num
            FROM {table_name} h
            JOIN {company_table_name} c ON h.symbol = c.symbol
            WHERE h.etf_ticker = ?
            ORDER BY h.collected_at DESC, h.weight_pct DESC NULLS LAST, h.symbol
            """,
            [etf_ticker.upper()],
        )
        return result.fetch_df()
    finally:
        conn.close()


def _delete_holdings_by_where(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    where_clause: str = "",
    params: list[str] | tuple[str, ...] = (),
) -> int:
    count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
    count = conn.execute(count_query, params).fetchone()[0]
    if count:
        conn.execute(f"DELETE FROM {table_name} {where_clause}", params)
    return int(count)


def delete_holding(
    etf_ticker: str,
    symbol: str,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
    company_table_name: str = COMPANY_TABLE_NAME,
) -> int:
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(
            conn=conn, table_name=table_name, company_table_name=company_table_name
        )
        return _delete_holdings_by_where(
            conn=conn,
            table_name=table_name,
            where_clause="WHERE UPPER(TRIM(etf_ticker)) = ? AND UPPER(TRIM(symbol)) = ?",
            params=[etf_ticker.strip().upper(), symbol.strip().upper()],
        )
    finally:
        conn.close()


def delete_etf_holdings(
    etf_symbols: str | list[str] | tuple[str, ...] | set[str] | None = None,
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
    company_table_name: str = COMPANY_TABLE_NAME,
) -> int:
    conn = get_connection(db_path=db_path)
    try:
        create_holdings_table(
            conn=conn, table_name=table_name, company_table_name=company_table_name
        )
        if etf_symbols is None:
            return _delete_holdings_by_where(conn=conn, table_name=table_name)

        if isinstance(etf_symbols, str):
            normalized_symbols = [etf_symbols.strip().upper()]
        else:
            normalized_symbols = []
            seen: set[str] = set()
            for symbol in etf_symbols:
                normalized = str(symbol).strip().upper()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                normalized_symbols.append(normalized)

        if not normalized_symbols:
            return 0

        placeholders = ", ".join(["?"] * len(normalized_symbols))
        return _delete_holdings_by_where(
            conn=conn,
            table_name=table_name,
            where_clause=f"WHERE UPPER(TRIM(etf_ticker)) IN ({placeholders})",
            params=normalized_symbols,
        )
    finally:
        conn.close()


def delete_all_holdings(
    db_path: str | Path | None = None,
    table_name: str = TABLE_NAME,
    company_table_name: str = COMPANY_TABLE_NAME,
) -> int:
    return delete_etf_holdings(
        etf_symbols=None,
        db_path=db_path,
        table_name=table_name,
        company_table_name=company_table_name,
    )
