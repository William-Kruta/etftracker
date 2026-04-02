"""Public package interface for etftracker."""

from importlib.metadata import PackageNotFoundError, version

from .db import delete_holding, get_connection, get_db_path, read_holdings, save_holdings
from .etftracker import get_etf_holdings
from .scraper import pipeline

try:
    __version__ = version("etftracker")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "__version__",
    "delete_holding",
    "get_connection",
    "get_db_path",
    "get_etf_holdings",
    "pipeline",
    "read_holdings",
    "save_holdings",
]
