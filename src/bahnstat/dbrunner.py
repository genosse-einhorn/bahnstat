from bahnstat.database import DatabaseConnection, DatabaseAccessor
from bahnstat.datatypes import *
from bahnstat.dbtimetableclient import DbTimetableClient

from datetime import datetime, timedelta
from time import sleep
from typing import Optional, Callable, List, Union, Iterable
import logging

_log = logging.getLogger(__name__)

class _RunnerWorker:
    def __init__(self, db: DatabaseAccessor, client: DbTimetableClient) -> None:
        self.db = db
        self.client = client

    def perform(self) -> None:
        board = self.client.current_board(datetime.now())

        for dep in board.departures:
            self.db.persist_departure(self.client.station, dep)

        for arr in board.arrivals:
            self.db.persist_arrival(self.client.station, arr)


class Runner:
    def __init__(self, dbfile: str, stops: Iterable[WatchedStop],
                 apikey: str, watchdog_func:Callable=None) -> None:
        self.apikey = apikey
        self.db = DatabaseAccessor(DatabaseConnection(dbfile))

        self.stops = list(stops)
        self._workers = [ _RunnerWorker(self.db, DbTimetableClient(s, auth=self.apikey, lookahead=timedelta(hours=2)))
                         for s in self.stops if s.active ]

        self._watchdog_func = watchdog_func

    def _watchdog(self) -> None:
        if self._watchdog_func is not None:
            self._watchdog_func()

    def run(self) -> None:
        for s in self.stops:
            self.db.persist_watched_stop(s)

        for w in self._workers:
            _log.debug('initial sync for stop {} '.format(w.client.station.name))

            w.perform()
            self._watchdog()

            # we have 20 request / min, intial sync is 5 requests
            sleep(15)

        while True:
            for w in self._workers:
                _log.debug('sync for stop {} '.format(w.client.station.name))

                w.perform()
                self._watchdog()

                # we have 20 request / min, update is at most 2 requests
                sleep(6)
