"""utils/logger.py — Structured logging for VenaNow (stdlib fallback)."""
import logging
import sys

logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("venanow")
__all__ = ["logger"]
