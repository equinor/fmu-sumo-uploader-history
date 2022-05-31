import logging

from sumo.wrapper import CallSumoApi
from sumo.wrapper import SumoClient


class SumoConnection:
    """Object to hold authentication towards Sumo"""

    def __init__(self, env=None):
        self._api = None
        self._env = env

        info = "Connection to Sumo on environment: {}".format(self.env)
        logging.info(info)

    @property
    def env(self):
        if self._env is None:
            self._env = "dev"

        return self._env

    @property
    def api(self):
        if self._api is None:
            self._api = self._establish_connection()

        return self._api

    def refresh(self):
        """Re-create the connection"""
        self._api = self._establish_connection()

    def _establish_connection(self):
        """Establish the connection with Sumo API, take user through 2FA."""
        return SumoClient(env=self.env)
