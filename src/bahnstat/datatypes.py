from uuid import UUID
from datetime import datetime, date, time
from typing import Sequence, List, Optional, Iterable, Any
import random

__all__ = [ 'Departure', 'Arrival', 'WatchedStop', 'Trip',
            'AggregatedTrip', 'AggregatedDeparture', 'AggregateDateRange' ]

class Departure:
    def __init__(self, time: datetime, train_name: str, destination: str, stopid: Any,
                 trip_code: int, line_code: str, delay: float = None) -> None:
        self.time = time
        self.train_name = train_name
        self.destination = destination
        self.stop_id = stopid
        self.trip_code = trip_code
        self.line_code = line_code
        self.delay = delay

class Arrival:
    def __init__(self, time: datetime, train_name: str, origin: str, stopid: Any,
                 tripcode: int, linecode: str, delay: float = None) -> None:
        self.time = time
        self.train_name = train_name
        self.origin = origin
        self.stop_id = stopid
        self.trip_code = tripcode
        self.line_code = linecode
        self.delay = delay

class WatchedStop:
    def __init__(self, id: UUID, backend_stop_id: int, name: str, active: bool = True) -> None:
        self.id = id
        self.backend_stop_id = backend_stop_id
        self.name = name
        self.active = active

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.id == other.id

        return False


class Trip:
    def __init__(self, origin: WatchedStop, destination: WatchedStop, date: date,
                 dep_time: time, dep_delay: Optional[float],
                 arr_time: time, arr_delay: Optional[float],
                 train_name: str) -> None:
        self.origin = origin
        self.destionation = destination
        self.date = date
        self.dep_time = dep_time
        self.dep_delay = dep_delay
        self.arr_time = arr_time
        self.arr_delay = arr_delay
        self.train_name = train_name

class AggregatedTrip:
    def __init__(self, train_name: str,
                 dep_time: time,
                 dep_delay_median: Optional[float],
                 dep_delay_90perc: Optional[float],
                 dep_delay_stdev: Optional[float],
                 arr_time: time,
                 arr_delay_median: Optional[float],
                 arr_delay_90perc: Optional[float],
                 arr_delay_stdev: Optional[float],
                 count: int) -> None:
        self.train_name = train_name
        self.dep_time = dep_time
        self.dep_delay_median = dep_delay_median
        self.dep_delay_90perc = dep_delay_90perc
        self.dep_delay_stdev  = dep_delay_stdev
        self.arr_time = arr_time
        self.arr_delay_median = arr_delay_median
        self.arr_delay_90perc = arr_delay_90perc
        self.arr_delay_stdev  = arr_delay_stdev
        self.count = count

class AggregatedDeparture:
    def __init__(self, train_name: str, destination: UUID, time: time,
                 delay_median: Optional[float], count: int) -> None:
        self.destination = destination
        self.train_name = train_name
        self.time = time
        self.delay_median = delay_median
        self.count = count

class AggregateDateRange:
    def __init__(self, count: int, first: date, last: date) -> None:
        self.count = count
        self.first = first
        self.last = last
