"""Public package interface for etftracker."""

from importlib.metadata import PackageNotFoundError, version

from .db import (
    delete_all_holdings,
    delete_etf_holdings,
    delete_holding,
    get_connection,
    get_db_path,
    read_holdings,
    read_holdings_history,
    save_holdings,
)
from .etftracker import get_etf_holdings, get_currently_tracked_etfs
from .scraper import pipeline
from .export import export_to_csv

try:
    __version__ = version("etftracker")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "__version__",
    "delete_all_holdings",
    "delete_etf_holdings",
    "delete_holding",
    "export_to_csv",
    "get_connection",
    "get_db_path",
    "get_etf_holdings",
    "get_currently_tracked_etfs",
    "pipeline",
    "read_holdings",
    "read_holdings_history",
    "save_holdings",
]
