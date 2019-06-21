import json
import os
from lib.log import setup_logger


class Config(object):
    def __init__(self, configPath):
        self.logger = setup_logger(__name__)
        self.logger.info("creating config file ")
        self.logger.info("current working directory is " + os.getcwd())
        self.logger.info("Config file is in " + os.getcwd() + configPath)
        with open(configPath) as f:
            self._data = json.load(f)

    @property
    def data(self):
        return self._data

    def get_tables(self):
        return self._data['tables']
