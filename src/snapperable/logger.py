import logging

# Create a logger for the snapperable module
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)  # Set default logging level

# Add a NullHandler to avoid warnings if no logging is configured by the application
logger.addHandler(logging.NullHandler())
