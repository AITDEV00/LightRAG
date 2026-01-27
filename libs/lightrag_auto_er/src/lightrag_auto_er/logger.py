import logging
import sys
import warnings
from .config import settings

def setup_logging():
    """Configures logging to file and stream, and routes warnings/exceptions."""
    # Ensure run dir exists
    settings.run_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(settings.log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger("er_service")

    # Route Python warnings into the logger
    def _log_warning(message, category, filename, lineno, file=None, line=None):
        logger.warning("%s in %s:%s: %s", category.__name__, filename, lineno, message)

    warnings.showwarning = _log_warning

    # Route uncaught exceptions into the logger
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception
    
    return logger

# Create a global logger instance (lazy init effectively, but usually called at startup)
logger = logging.getLogger("er_service")
