import logging
import pathlib as pl
from logging import handlers

class LogHandler():
    """Class for managing log"""

    def __init__(self, directory='log', filename='log.log'):
        self.filename=filename
        self.path=pl.Path(__file__).parent.parent / directory / filename #Create full path
        pl.Path.mkdir(self.path.parent, parents=True, exist_ok=True) #Make directory for logging files
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()
        self.handler=logging.handlers.RotatingFileHandler(self.path, maxBytes=10485760, backupCount=5)
        self.handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(self.handler)