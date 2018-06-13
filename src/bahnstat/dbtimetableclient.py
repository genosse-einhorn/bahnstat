from urllib.request import urlopen, Request
from xml.dom.minidom import parse as domparse
from datetime import datetime, date, timedelta
from bahnstat.datatypes import *
from typing import Optional, Dict, Sequence, Iterator, Iterable, List, Set, Tuple
from functools import lru_cache
import re
import itertools
import logging
import time

_log = logging.getLogger(__name__)

def _parse_fulltime(timestr):
    m = re.fullmatch('([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])', timestr)
    assert m is not None

    return datetime(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5)))

class DbTimetableTripLabel:
    def __init__(self, filter_flags, trip_type, category, number):
        self.filter_flags = filter_flags
        self.trip_type = trip_type
        self.category = category
        self.number = number

    @staticmethod
    def from_domnode(tl):
        return DbTimetableTripLabel(tl.getAttribute('f'), tl.getAttribute('t'),
                                    tl.getAttribute('c'), tl.getAttribute('n'))

class DbTimetableEvent:
    def __init__(self, planned_path, changed_path, planned_time, changed_time,
                 planned_status, changed_status, planned_platform, changed_platform, line, clt):
        self.planned_path = planned_path
        self.changed_path = changed_path
        self.planned_time = planned_time
        self.changed_time = changed_time
        self.planned_status = planned_status
        self.changed_status = changed_status
        self.planned_platform = planned_platform
        self.changed_platform = changed_platform
        self.line = line
        self.cancellation_time = clt

    @property
    def delay(self):
        if self.changed_time is not None and self.planned_time is not None:
            return (self.changed_time - self.planned_time) / timedelta(minutes=1)
        elif self.status == 'c':
            return float('inf')
        else:
            return 0

    @property
    def actual_time(self):
        if self.changed_time is not None:
            return self.changed_time
        elif self.planned_time is not None:
            return self.planned_time
        else:
            return None

    @property
    def status(self):
        if self.changed_status is not None:
            return self.changed_status
        elif self.planned_status is not None:
            return self.planned_status
        else:
            return 'p'

    @property
    def platform(self):
        if self.changed_platform is not None:
            return self.changed_platform
        elif self.planned_platform is not None:
            return self.planned_platform
        else:
            return None

    @property
    def cancelled(self):
        return self.status == 'c'

    @property
    def complete(self):
        return self.planned_path is not None and self.planned_time is not None

    @property
    def hide_after_timestamp(self):
        """ returns a timestamp which marks the time where this event should be hidden from view"""

        assert self.complete

        end = self.planned_time

        if self.changed_time is not None:
            end = max(self.changed_time, self.planned_time)

        if self.cancelled:
            if self.cancellation_time is not None and self.cancellation_time > end:
                end = self.cancellation_time

            # show cancelled trains for 5 minutes longer, like the WBT does
            end = end + timedelta(minutes=5)

        return end

    @classmethod
    def from_domnode(clazz, dp):
        if dp.hasAttribute('pt'):
            pt = _parse_fulltime(dp.getAttribute('pt'))
        else:
            pt = None

        if dp.hasAttribute('ct'):
            ct = _parse_fulltime(dp.getAttribute('ct'))
        else:
            ct = None

        if dp.hasAttribute('ppth'):
            ppth = dp.getAttribute('ppth').split('|')
        else:
            ppth = None

        if dp.hasAttribute('cpth'):
            cpth = dp.getAttribute('cpth').split('|')
        else:
            cpth = None

        if dp.hasAttribute('ps'):
            ps = dp.getAttribute('ps')
        else:
            ps = None

        if dp.hasAttribute('cs'):
            cs = dp.getAttribute('cs')
        else:
            cs = None

        if dp.hasAttribute('pp'):
            pp = dp.getAttribute('pp')
        else:
            pp = None

        if dp.hasAttribute('cp'):
            cp = dp.getAttribute('cp')
        else:
            cp = None

        if dp.hasAttribute('l'):
            l = dp.getAttribute('l')
        else:
            l = None

        if dp.hasAttribute('clt'):
            clt = _parse_fulltime(dp.getAttribute('clt'))
        else:
            clt = None

        return clazz(ppth, cpth, pt, ct, ps, cs, pp, cp, l, clt)

    @classmethod
    def merged(clazz, base, change):
        pt = base.planned_time
        if change.planned_time is not None:
            pt = change.planned_time

        ct = base.changed_time
        if change.changed_time is not None:
            ct = change.changed_time

        ppth = base.planned_path
        if change.planned_path is not None:
            ppth = change.planned_path

        cpth = base.changed_path
        if change.changed_path is not None:
            cpth = change.changed_path

        ps = base.planned_status
        if change.planned_status is not None:
            ps = change.planned_status

        cs = base.changed_status
        if change.changed_status is not None:
            cs = change.changed_status

        pp = base.planned_platform
        if change.planned_platform is not None:
            pp = change.planned_platform

        cp = base.changed_platform
        if change.changed_platform is not None:
            cp = change.changed_platform

        l = base.line
        if change.line is not None:
            l = change.line

        clt = base.cancellation_time
        if change.cancellation_time is not None:
            clt = change.cancellation_time

        return clazz(ppth, cpth, pt, ct, ps, cs, pp, cp, l, clt)

class DbTimetableDeparture(DbTimetableEvent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def destination(self):
        if self.planned_path is not None:
            return self.planned_path[-1]
        else:
            return None

class DbTimetableArrival(DbTimetableEvent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def origin(self):
        if self.planned_path is not None:
            return self.planned_path[0]
        else:
            return None

class DbTimetableStop:
    def __init__(self, id_tripno, id_starttime, id_stopno, arrival, departure, label):
        self.id_trip = id_tripno
        self.id_start = id_starttime
        self.id_stop = id_stopno
        self.arrival = arrival
        self.departure = departure
        self.label = label

    @staticmethod
    def from_domnode(s):
        idstr = s.getAttribute('id')
        m = re.fullmatch('(-?[0-9]+)-([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])-([0-9]+)', idstr)
        assert m is not None

        tripid = int(m.group(1))
        startdate = datetime(2000 + int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5)), int(m.group(6)))
        stopno = int(m.group(7))

        departures = next((DbTimetableDeparture.from_domnode(dp) for dp in s.getElementsByTagName('dp')), None)
        arrivals = next((DbTimetableArrival.from_domnode(ar) for ar in s.getElementsByTagName('ar')), None)
        label = next((DbTimetableTripLabel.from_domnode(tl) for tl in s.getElementsByTagName('tl')), None)

        return DbTimetableStop(tripid, startdate, stopno, arrivals, departures, label)

    @property
    def id(self):
        return '{}-{:02}{:02}{:02}{:02}{:02}-{}'.format(self.id_trip, self.id_start.year % 100,
                                                        self.id_start.month, self.id_start.day,
                                                        self.id_start.hour, self.id_start.minute,
                                                        self.id_stop)

    @property
    def complete(self):
        return self.label is not None and (
            (self.arrival is not None and self.arrival.complete)
            or (self.departure is not None and self.departure.complete))

    @property
    def hide_after_timestamp(self):
        assert self.complete

        end = datetime(1, 1, 1)

        if self.arrival is not None and self.arrival.complete:
            end = max(end, self.arrival.hide_after_timestamp)

        if self.departure is not None and self.departure.complete:
            end = max(end, self.departure.hide_after_timestamp)

        return end

    @staticmethod
    def merged(base, change):
        assert base.id == change.id

        # FIXME! actually merge trip label
        l = base.label
        if change.label is not None:
            l = change.label

        if base.arrival is not None and change.arrival is None:
            arr = base.arrival
        elif base.arrival is None and change.arrival is not None:
            arr = change.arrival
        elif base.arrival is not None and change.arrival is not None:
            arr = DbTimetableArrival.merged(base.arrival, change.arrival)
        else:
            arr = None

        if base.departure is not None and change.departure is None:
            dep = base.departure
        elif base.departure is None and change.departure is not None:
            dep = change.departure
        elif base.departure is not None and change.departure is not None:
            dep = DbTimetableDeparture.merged(base.departure, change.departure)
        else:
            dep = None

        return DbTimetableStop(base.id_trip, base.id_start, base.id_stop, arr, dep, l)

class _ApiClient:
    def __init__(self, eva_id: int, apiurl: str, apikey: str = None) -> None:
        self.eva_id = eva_id
        self.apiurl = apiurl
        self.headers = {'User-Agent': 'db-timetable-api-client/0.01 (dbclient@genosse-einhorn.de)'}

        if apikey is not None:
            self.headers['Authorization'] = 'Bearer ' + apikey

    @lru_cache(maxsize=12)
    def plan(self, timeslice: datetime) -> Sequence[DbTimetableStop]:
        _log.debug('{}/plan/{}/{:02}{:02}{:02}/{:02}'.format(self.apiurl,
                self.eva_id, timeslice.year % 100, timeslice.month, timeslice.day, timeslice.hour))
        with urlopen(Request('{}/plan/{}/{:02}{:02}{:02}/{:02}'.format(self.apiurl,
                self.eva_id, timeslice.year % 100, timeslice.month, timeslice.day, timeslice.hour),
                headers=self.headers)) as u:
            d = domparse(u)
            return [DbTimetableStop.from_domnode(s) for s in d.getElementsByTagName('s')]

    def fchg(self) -> Sequence[DbTimetableStop]:
        # TODO: time-based cache
        _log.debug('{}/fchg/{}'.format(self.apiurl, self.eva_id))
        with urlopen(Request('{}/fchg/{}'.format(self.apiurl, self.eva_id), headers=self.headers)) as u:
            d = domparse(u)
            return [DbTimetableStop.from_domnode(s) for s in d.getElementsByTagName('s')]

    def rchg(self) -> Sequence[DbTimetableStop]:
        _log.debug('{}/rchg/{}'.format(self.apiurl, self.eva_id))
        with urlopen(Request('{}/rchg/{}'.format(self.apiurl, self.eva_id), headers=self.headers)) as u:
            d = domparse(u)
            return [DbTimetableStop.from_domnode(s) for s in d.getElementsByTagName('s')]

class _TimetableChangeIntegrator:
    def __init__(self, client: _ApiClient) -> None:
        self._api = client
        self._last_change_time = None # type: Optional[float]
        self._timeslices = set() # type: Set[datetime]
        self._stop_cache = dict() # type: Dict[str, DbTimetableStop]

    @staticmethod
    def _integrate_changes(stops: Dict[str, DbTimetableStop], chg: Iterable[DbTimetableStop]) -> Dict[str, DbTimetableStop]:
        stops = dict(stops)

        for c in chg:
            if c.id in stops:
                stops[c.id] = DbTimetableStop.merged(stops[c.id], c)
            elif c.complete:
                stops[c.id] = c

        return stops

    def stops_with_changes(self, timeslices: Set[datetime]) -> Sequence[DbTimetableStop]:
        now = time.monotonic()
        if self._timeslices == timeslices and self._last_change_time is not None and now - self._last_change_time < 90:
            self._stop_cache = self._integrate_changes(self._stop_cache, self._api.rchg())
            self._last_change_time = now
        else:
            stops = dict() # type: Dict[str, DbTimetableStop]

            for t in timeslices:
                stops = self._integrate_changes(stops, self._api.plan(t))

            self._stop_cache = self._integrate_changes(stops, self._api.fchg())
            self._last_change_time = now
            self._timeslices = set(timeslices)

        return [v for c,v in self._stop_cache.items()]

class DbTimetableBoard:
    """Timetable query result"""
    def __init__(self, raw: Iterable[DbTimetableStop], time_range: Tuple[datetime, datetime],
                 departures: Iterable[Departure], arrivals: Iterable[Arrival]) -> None:
        self.raw = list(raw)
        self.time_range = time_range
        self.arrivals = sorted(arrivals, key=lambda a: a.time)
        self.departures = sorted(departures, key=lambda d: d.time)


class DbTimetableClient:
    """Client for the DB timetable API

    TODO: document how plan range and changes work
    """
    def __init__(self, station_eva_id: int, *,
                 lookbehind: timedelta = timedelta(hours=1), lookahead: timedelta = timedelta(hours=1),
                 apiurl:str='https://api.deutschebahn.com/timetables/v1', auth:str=None) -> None:
        self.station_eva_id = station_eva_id
        self.lookahead = lookahead
        self.lookbehind = lookbehind
        self._timetable_retriever = _TimetableChangeIntegrator(_ApiClient(station_eva_id, apiurl, auth))

    @staticmethod
    def _timeslices(range_min: datetime, range_max: datetime) -> Set[datetime]:
        s = set() # type: Set[datetime]

        d = datetime(range_min.year, range_min.month, range_min.day, range_min.hour)
        while d < range_max:
            s = s | {d}

            d = d + timedelta(hours=1)

        return s

    def current_board(self, current_time: datetime) -> DbTimetableBoard:
        # make timeslices
        timerange_min = current_time - self.lookbehind
        timerange_max = current_time + self.lookahead
        t = self._timeslices(timerange_min, timerange_max)

        # retrieve raw data
        s = self._timetable_retriever.stops_with_changes(t)

        # build response
        return DbTimetableBoard(s, (timerange_min, timerange_max),
                                self._departures(s, current_time),
                                self._arrivals(s, current_time))

    def _departures(self, stops: Iterable[DbTimetableStop], current_time: datetime) -> Iterator[Departure]:
        for s in stops:
            if (not s.complete) or (s.departure is None) or (not s.departure.complete):
                continue

            if s.departure.hide_after_timestamp < current_time:
                continue

            if s.departure.actual_time > current_time + self.lookahead:
                continue

            if s.departure.line is not None:
                train_name = '{} {} ({})'.format(s.label.category, s.departure.line, s.label.number)
            else:
                train_name = '{} {}'.format(s.label.category, s.label.number)

            trip_code = (100000000 * (s.id_start.year % 100) + 1000000 * s.id_start.month
                         + 10000 * s.id_start.day + 100 * s.id_start.hour + s.id_start.minute)

            yield Departure(s.departure.planned_time, train_name, s.departure.destination,
                            self.station_eva_id, trip_code, s.id_trip, s.departure.delay)

    def _arrivals(self, stops: Iterable[DbTimetableStop], current_time: datetime) -> Iterator[Arrival]:
        for s in stops:
            if (not s.complete) or (s.arrival is None) or (not s.arrival.complete):
                continue

            if s.arrival.hide_after_timestamp < current_time:
                continue

            if s.arrival.actual_time > current_time + self.lookahead:
                continue

            if s.arrival.line is not None:
                train_name = '{} {} ({})'.format(s.label.category, s.arrival.line, s.label.number)
            else:
                train_name = '{} {}'.format(s.label.category, s.label.number)

            trip_code = (100000000 * (s.id_start.year % 100) + 1000000 * s.id_start.month
                         + 10000 * s.id_start.day + 100 * s.id_start.hour + s.id_start.minute)

            yield Arrival(s.arrival.planned_time, train_name, s.arrival.origin,
                            self.station_eva_id, trip_code, s.id_trip, s.arrival.delay)

