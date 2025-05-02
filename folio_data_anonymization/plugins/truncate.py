import logging


logger = logging.getLogger(__name__)


def truncate():
    logger.info("Will truncate.")
    return "Truncating"
