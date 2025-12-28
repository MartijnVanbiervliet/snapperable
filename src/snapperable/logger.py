import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Add a NullHandler to avoid warnings if no logging is configured by the application
logger.addHandler(logging.NullHandler())
