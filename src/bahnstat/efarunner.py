from bahnstat.mechanize_mini import Browser
from bahnstat.database import DatabaseConnection, DatabaseAccessor
from bahnstat.datatypes import *
from bahnstat.efaxmlclient import *

from uuid import UUID
from typing import Sequence, List, Iterable, Optional, Callable, Union
from datetime import datetime, timedelta
import logging
import time
import random

_log = logging.getLogger(__name__)

class DepartureWatcher:
    def __init__(self, stop: WatchedStop, client: EfaXmlClient, db: DatabaseAccessor) -> None:
        self.stop = stop
        self.next_check = datetime.utcnow().timestamp() + random.randrange(0, 60*2)
        self.client = client
        self.db = db

    def perform(self) -> None:
        dm = self.client.departure_monitor(self.stop)
        _log.debug('retrieved departure monitor for {} at {}'.format(dm.stop_name, dm.now))

        for dep in dm.departures:
            self.db.persist_departure(self.stop, dep)

    def reschedule(self) -> None:
        self.next_check = self.next_check + 60*2 + random.randrange(0, 15)

class ArrivalWatcher:
    def __init__(self, stop: WatchedStop, client: EfaXmlClient, db: DatabaseAccessor) -> None:
        self.stop = stop
        self.next_check = datetime.utcnow().timestamp() + random.randrange(0, 60*2)
        self.client = client
        self.db = db

    def perform(self) -> None:
        dm = self.client.arrival_monitor(self.stop)
        _log.debug('retrieved arrival monitor for {} at {}'.format(dm.stop_name, dm.now))

        for dep in dm.arrivals:
            self.db.persist_arrival(self.stop, dep)

    def reschedule(self) -> None:
        self.next_check = self.next_check + 60*2 + random.randrange(0, 15)

class Runner:
    def __init__(self, dbfile: str, stops: Iterable[WatchedStop],
                 user_agent: str, watchdog_func:Callable=None) -> None:
        self.client = EfaXmlClient(user_agent)
        self.db = DatabaseAccessor(DatabaseConnection(dbfile))

        self.stops = list(stops)
        self.watchers = [] # type: List[Union[DepartureWatcher, ArrivalWatcher]]
        self.watchers.extend(DepartureWatcher(s, self.client, self.db) for s in self.stops)
        self.watchers.extend(ArrivalWatcher(s, self.client, self.db) for s in self.stops)
        self._watchdog_func = watchdog_func

    def _watchdog(self) -> None:
        if self._watchdog_func is not None:
            self._watchdog_func()

    def run(self) -> None:
        for s in self.stops:
            self.db.persist_watched_stop(s)
            _log.debug('stop {} '.format(s.name))

        for m in self.watchers:
            m.perform()

        while True:
            now = datetime.utcnow().timestamp()

            for m in self.watchers:
                if m.next_check < now:
                    m.perform()
                    m.reschedule()
                    self._watchdog()


            time.sleep(1)
