import logging
from logging import NullHandler

logger = logging.getLogger("feaflow")
logger.addHandler(NullHandler())
