from uuid import UUID
from datetime import datetime
import random
from urllib.parse import quote as urlquote

class Departure:
    def __init__(self, time, train_name, destination, stopid, trip_code, line_code, delay = None):
        self.time = time
        self.train_name = train_name
        self.destination = destination
        self.stop_id = stopid
        self.trip_code = trip_code
        self.line_code = line_code
        self.delay = delay

    @property
    def tripstoptimes_url(self):
        return 'https://www.efa-bw.de/nvbw/XML_TRIPSTOPTIMES_REQUEST?tripCode={tripcode}&stopID={stopid}&time={stoph:02}{stopm:02}&date={year:04}{month:02}{day:02}&line={stateless}&tStOTType=all&useRealtime=1'.format(
            tripcode = self.trip_code,
            stopid = self.stop_id,
            stoph = self.time.hour,
            stopm = self.time.minute,
            year = self.time.year,
            month = self.time.month,
            day = self.time.day,
            stateless = urlquote(self.line_code))

class DepartureMonitor:
    def __init__(self, now, gid, name, departures):
        self.now = now
        self.stop_gid = gid
        self.stop_name = name
        self.departures = list(departures)

class Arrival:
    def __init__(self, time, train_name, origin, stopid, tripcode, linecode, delay):
        self.time = time
        self.train_name = train_name
        self.origin = origin
        self.stop_id = stopid
        self.trip_code = tripcode
        self.line_code = linecode
        self.delay = delay

    @property
    def tripstoptimes_url(self):
        return 'https://www.efa-bw.de/nvbw/XML_TRIPSTOPTIMES_REQUEST?tripCode={tripcode}&stopID={stopid}&time={stoph:02}{stopm:02}&date={year:04}{month:02}{day:02}&line={stateless}&tStOTType=all&useRealtime=1'.format(
            tripcode = self.trip_code,
            stopid = self.stop_id,
            stoph = self.time.hour,
            stopm = self.time.minute,
            year = self.time.year,
            month = self.time.month,
            day = self.time.day,
            stateless = urlquote(self.line_code))

class ArrivalMonitor:
    def __init__(self, now, gid, name, arrivals):
        self.now = now
        self.stop_gid = gid
        self.stop_name = name
        self.arrivals = list(arrivals)

class WatchedStop:
    def __init__(self, id, efa_stop_id, name):
        self.id = id
        self.efa_stop_id = efa_stop_id
        self.name = name

    def dm_url(self, *, mode='dep'):
        return 'https://www.efa-bw.de/nvbw/XML_DM_REQUEST?language=de&name_dm={}&type_dm=any&mode=direct&useRealtime=1&itdDateTimeDepArr={}'.format(self.efa_stop_id, mode)

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.id == other.id

        return False


class Trip:
    def __init__(self, origin, destination, date, dep_time, dep_delay, arr_time, arr_delay, train_name):
        self.origin = origin
        self.destionation = destination
        self.date = date
        self.dep_time = dep_time
        self.dep_delay = dep_delay
        self.arr_time = arr_time
        self.arr_delay = arr_delay
        self.train_name = train_name

class AggregatedTrip:
    def __init__(self, train_name, dep_time, dep_delay_median, arr_time, arr_delay_median, count):
        self.train_name = train_name
        self.dep_time = dep_time
        self.dep_delay_median = dep_delay_median
        self.arr_time = arr_time
        self.arr_delay_median = arr_delay_median
        self.count = count

class AggregatedDeparture:
    def __init__(self, train_name, destination, time, delay_median, count):
        self.destination = destination
        self.train_name = train_name
        self.time = time
        self.delay_median = delay_median
        self.count = count

class AggregateDateRange:
    def __init__(self, count, first, last):
        self.count = count
        self.first = first
        self.last = last
