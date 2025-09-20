
import logging
import os
from logging.handlers import RotatingFileHandler

# Centralized logging setup to avoid repeated basicConfig calls across modules
# and to ensure logs go to both console and a rotating file with a stable path.

LOG_LEVEL_ENV_NAME = "LOG_LEVEL"
LOG_FILE_ENV_NAME = "LOG_FILE"


def _resolve_log_file_path() -> str:
    """Return an absolute log file path.

    Defaults to a file named etl_pipeline.log at the project root (one level up
    from this config directory). Can be overridden with the LOG_FILE env var.
    """
    env_path = os.getenv(LOG_FILE_ENV_NAME)
    if env_path:
        return os.path.abspath(env_path)

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(project_root, "etl_pipeline.log")


def setup_logging() -> None:
    """Configure root logging once with console and rotating file handlers."""
    if getattr(setup_logging, "_configured", False):
        return

    # Determine level
    level_name = os.getenv(LOG_LEVEL_ENV_NAME, "INFO").upper()
    numeric_level = getattr(logging, level_name, logging.INFO)

    # Clear existing handlers to ensure idempotent configuration
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(numeric_level)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler (rotating)
    log_file_path = _resolve_log_file_path()
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    file_handler = RotatingFileHandler(log_file_path, maxBytes=5 * 1024 * 1024, backupCount=3)
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    setup_logging._configured = True

