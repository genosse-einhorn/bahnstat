from bahnstat.mechanize_mini import Browser
from bahnstat.database import DatabaseConnection, DatabaseAccessor

from bahnstat.datatypes import *
from bahnstat.efaxmlparser import *

from uuid import UUID

from datetime import datetime, timedelta
import logging
import time
import random

_log = logging.getLogger(__name__)

class DepartureWatcher:
    def __init__(self, stop):
        self.stop = stop
        self.next_check = datetime.utcnow().timestamp() + random.randrange(0, 60*2)

    def perform(self, db, browser):
        doc = browser.open(self.stop.dm_url(mode='dep'))
        dm = departure_monitor_from_response(doc.document_element)

        _log.debug('retrieved departure monitor for {} at {}'.format(dm.stop_name, dm.now))

        for dep in dm.departures:
            db.persist_departure(self.stop, dep)

    def reschedule(self):
        self.next_check = self.next_check + 60*2 + random.randrange(0, 15)

class ArrivalWatcher:
    def __init__(self, stop):
        self.stop = stop
        self.next_check = datetime.utcnow().timestamp() + random.randrange(0, 60*2)

    def perform(self, db, browser):
        doc = browser.open(self.stop.dm_url(mode='arr'))
        dm = arrival_monitor_from_response(doc.document_element)

        _log.debug('retrieved arrival monitor for {} at {}'.format(dm.stop_name, dm.now))

        for dep in dm.arrivals:
            db.persist_arrival(self.stop, dep)

    def reschedule(self):
        self.next_check = self.next_check + 60*2 + random.randrange(0, 15)

class Runner:
    def __init__(self, dbfile, stops, user_agent, watchdog_func=None):
        self.browser = Browser(user_agent)
        self.db = DatabaseAccessor(DatabaseConnection(dbfile))

        self.stops = list(stops)
        self.watchers = []
        self.watchers.extend(DepartureWatcher(s) for s in stops)
        self.watchers.extend(ArrivalWatcher(s) for s in stops)
        self.watchdog_func = watchdog_func

    def watchdog(self):
        if self.watchdog_func is not None:
            self.watchdog_func()

    def run(self):
        for s in self.stops:
            self.db.persist_watched_stop(s)
            _log.debug('stop {} with url {}'.format(s.name, s.dm_url()))

        for m in self.watchers:
            m.perform(self.db, self.browser)

        while True:
            now = datetime.utcnow().timestamp()

            for m in self.watchers:
                if m.next_check < now:
                    m.perform(self.db, self.browser)
                    m.reschedule()
                    self.watchdog()


            time.sleep(1)

if __name__ == '__main__':
    Runner().run()
