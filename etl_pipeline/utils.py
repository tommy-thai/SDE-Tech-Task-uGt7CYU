import logging
import traceback

def log(message, level="info"):
    logger = logging.getLogger(__name__)

    if level == "info":
        logger.info(message)
    elif level == "error":
        traceback.format_exc()
        logger.error(message)
    elif level == "warning":
        logger.warning(message)
    else:
        raise ValueError(
            "Invalid log level. Please choose 'info', 'error', or 'warning'."
        )