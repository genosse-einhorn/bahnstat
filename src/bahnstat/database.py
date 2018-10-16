import sqlite3
from uuid import UUID, uuid5
from datetime import datetime, time
import random
import statistics
import math
from typing import Iterable, Iterator, Optional

from bahnstat.datatypes import *
from bahnstat.holidays_bw import *

sqlite3.enable_callback_tracebacks(True)

# uuid converter
sqlite3.register_converter('UUID', lambda s: UUID(str(s, encoding='ascii')))
sqlite3.register_adapter(UUID, lambda u: str(u))

class MedianAggregate:
    def __init__(self):
        self.l = []

    def step(self, value):
        if value is not None:
            self.l.append(value)

    def finalize(self):
        if len(self.l) < 1:
            return None
        else:
            return statistics.median(self.l)

class PercentileAggregate:
    def __init__(self):
        self.l = []
        self.percentile = 1

    def step(self, percentile, value):
        if value is not None:
            self.l.append(value)

        self.percentile = percentile

    def finalize(self):
        self.l.sort()

        if len(self.l) < 1:
            return None

        k = len(self.l) * self.percentile / 100
        c = math.ceil(k)

        if k == c:
            return (self.l[int(c)-1] + self.l[int(c)])/2
        else:
            return self.l[int(c) - 1]

class StdevAggregate:
    def __init__(self):
        self.l = []

    def step(self, value):
        if value is not None and math.isfinite(value):
            self.l.append(value)

    def finalize(self):
        if len(self.l) < 2:
            return None
        else:
            return statistics.stdev(self.l)

def date_type(datestr):
    if datestr is None:
        return None

    d = datetime.strptime(datestr, '%Y-%m-%d').date()

    if d in HOLIDAYS:
        return 'sun'

    wd = d.weekday()
    if 0 <= wd <= 4:
        return 'mofr'
    elif wd == 5:
        return 'sat'
    elif wd == 6:
        return 'sun'
    else:
        assert False

def DATE_TYPE_SQL_CHECK(fieldname, t):
    if t == 'any' or t == 'all':
        return ' (1 = 1) '
    else:
        return " (DATE_TYPE({}) = '{}') ".format(fieldname, t)


DBVER_CURRENT = 5

class DatabaseConnection:
    """low-level database access"""
    def __init__(self, dbfile: str) -> None:
        self.conn = sqlite3.connect(dbfile, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.isolation_level = None

        self.conn.create_aggregate('median', 1, MedianAggregate)
        self.conn.create_aggregate('percentile', 2, PercentileAggregate)
        self.conn.create_aggregate('stdev', 1, StdevAggregate)

        self.conn.create_function('date_type', 1, date_type)

        self.conn.execute('PRAGMA foreign_keys = ON')
        self.conn.execute('PRAGMA recursive_triggers = ON')
        self.conn.execute('PRAGMA temp_store = MEMORY')

        dbver = self.conn.execute('PRAGMA user_version').fetchone()[0]
        if dbver < DBVER_CURRENT:
            self._migrate_db()

        self._setup_temps()

    def exec(self, sql, **params):
        """ execute sql, with named parameters """
        return self.conn.execute(sql, params)

    def __enter__(self):
        self.conn.execute('BEGIN IMMEDIATE')

        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            self.conn.rollback()
            return False
        else:
            self.conn.commit()
            return True

    def _migrate_db(self):
        should_vacuum = False

        with self:
            dbver = self.exec('PRAGMA user_version').fetchone()[0]

            if dbver < 1:
                self.exec('''
                    CREATE TABLE WatchedStop(
                        id UUID NOT NULL PRIMARY KEY,
                        efa_stop_id INTEGER NOT NULL,
                        name TEXT NOT NULL)
                    ''')
                self.exec('''
                    CREATE TABLE Departure(
                        stop UUID REFERENCES WatchedStop(id),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        train_name TEXT NOT NULL,
                        UNIQUE(stop,time,trip_code,line_code))
                    ''')
                self.exec('''
                    CREATE TABLE Arrival(
                        stop UUID REFERENCES WatchedStop(id),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code TEXT NOT NULL,
                        origin TEXT NOT NULL,
                        train_name TEXT NOT NULL,
                        UNIQUE(stop,time,trip_code,line_code))
                    ''')

                dbver = 1

            if dbver < 2:
                # migration to db schema v2: use integer primary keys instead of UUIDs
                # for inter-table relations to save space
                self.exec('''ALTER TABLE WatchedStop RENAME TO WatchedStop_TMP''')
                self.exec('''
                    CREATE TABLE WatchedStop(
                        pk INTEGER PRIMARY KEY,
                        id UUID NOT NULL UNIQUE,
                        efa_stop_id INTEGER NOT NULL,
                        name TEXT NOT NULL)
                    ''')
                self.exec('''
                    INSERT INTO WatchedStop (id, efa_stop_id, name)
                    SELECT id, efa_stop_id, name FROM WatchedStop_TMP
                    ''')

                self.exec('''ALTER TABLE Departure RENAME TO Departure_TMP''')
                self.exec('''
                    CREATE TABLE Departure(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        train_name TEXT NOT NULL,
                        UNIQUE(stop_pk,time,trip_code,line_code))
                    ''')
                self.exec('''
                    INSERT INTO Departure (stop_pk, time, delay, trip_code, line_code, destination, train_name)
                    SELECT WatchedStop.pk, time, delay, trip_code, line_code, destination, train_name
                    FROM Departure_TMP
                    INNER JOIN WatchedStop WHERE WatchedStop.id = Departure_TMP.stop
                    ''')

                self.exec('''ALTER TABLE Arrival RENAME TO Arrival_TMP''')
                self.exec('''
                    CREATE TABLE Arrival(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code TEXT NOT NULL,
                        origin TEXT NOT NULL,
                        train_name TEXT NOT NULL,
                        UNIQUE(stop_pk,time,trip_code,line_code))
                    ''')
                self.exec('''
                    INSERT INTO Arrival (stop_pk, time, delay, trip_code, line_code, origin, train_name)
                    SELECT WatchedStop.pk, time, delay, trip_code, line_code, origin, train_name
                    FROM Arrival_TMP
                    INNER JOIN WatchedStop WHERE WatchedStop.id = Arrival_TMP.stop
                    ''')

                self.exec('DROP TABLE Arrival_TMP')
                self.exec('DROP TABLE Departure_TMP')
                self.exec('DROP TABLE WatchedStop_TMP')

                should_vacuum = True

                dbver = 2

            if dbver < 3:
                # db schema v3: do manual dictionary compression for line codes
                self.exec('''
                    CREATE TABLE LineCodeDictionary(
                        pk INTEGER PRIMARY KEY,
                        line_code TEXT UNIQUE NOT NULL)
                    ''')
                self.exec('''
                    INSERT OR IGNORE INTO LineCodeDictionary (line_code)
                    SELECT line_code FROM Departure
                    ''')
                self.exec('''
                    INSERT OR IGNORE INTO LineCodeDictionary (line_code)
                    SELECT line_code FROM Arrival
                    ''')

                self.exec('''ALTER TABLE Departure RENAME TO Departure_TMP''')
                self.exec('''
                    CREATE TABLE Departure(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code_pk INTEGER REFERENCES LineCodeDictionary(pk),
                        destination TEXT NOT NULL,
                        train_name TEXT NOT NULL,
                        UNIQUE(stop_pk,time,trip_code,line_code_pk))
                    ''')
                self.exec('''
                    INSERT INTO Departure (stop_pk, time, delay, trip_code, line_code_pk, destination, train_name)
                    SELECT stop_pk, time, delay, trip_code, LineCodeDictionary.pk, destination, train_name
                    FROM Departure_TMP
                    INNER JOIN LineCodeDictionary WHERE LineCodeDictionary.line_code = Departure_TMP.line_code
                    ''')

                self.exec('''ALTER TABLE Arrival RENAME TO Arrival_TMP''')
                self.exec('''
                    CREATE TABLE Arrival(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code_pk INTEGER REFERENCES LineCodeDictionary(pk),
                        origin TEXT NOT NULL,
                        train_name TEXT NOT NULL,
                        UNIQUE(stop_pk,time,trip_code,line_code_pk))
                    ''')
                self.exec('''
                    INSERT INTO Arrival (stop_pk, time, delay, trip_code, line_code_pk, origin, train_name)
                    SELECT stop_pk, time, delay, trip_code, LineCodeDictionary.pk, origin, train_name
                    FROM Arrival_TMP
                    INNER JOIN LineCodeDictionary WHERE LineCodeDictionary.line_code = Arrival_TMP.line_code
                    ''')

                self.exec('DROP TABLE Arrival_TMP')
                self.exec('DROP TABLE Departure_TMP')

                should_vacuum = True
                dbver = 3

            if dbver < 4:
                # db schema v4: manual dictionary compression for origin and destination names
                self.exec('''
                    CREATE TABLE OriginDestinationDictionary(
                        pk INTEGER PRIMARY KEY,
                        name TEXT UNIQUE NOT NULL)
                    ''')
                self.exec('''
                    INSERT OR IGNORE INTO OriginDestinationDictionary (name)
                    SELECT destination FROM Departure
                    ''')
                self.exec('''
                    INSERT OR IGNORE INTO OriginDestinationDictionary (name)
                    SELECT origin FROM Arrival
                    ''')

                self.exec('''ALTER TABLE Departure RENAME TO Departure_TMP''')
                self.exec('''
                    CREATE TABLE Departure(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code_pk INTEGER REFERENCES LineCodeDictionary(pk),
                        destination_pk INTEGER REFERENCES OriginDestinationDictionary(pk),
                        train_name TEXT NOT NULL,
                        UNIQUE(stop_pk,time,trip_code,line_code_pk))
                    ''')
                self.exec('''
                    INSERT INTO Departure (stop_pk, time, delay, trip_code, line_code_pk, destination_pk, train_name)
                    SELECT stop_pk, time, delay, trip_code, line_code_pk, OriginDestinationDictionary.pk, train_name
                    FROM Departure_TMP
                    INNER JOIN OriginDestinationDictionary WHERE OriginDestinationDictionary.name = Departure_TMP.destination
                    ''')

                self.exec('''ALTER TABLE Arrival RENAME TO Arrival_TMP''')
                self.exec('''
                    CREATE TABLE Arrival(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code_pk INTEGER REFERENCES LineCodeDictionary(pk),
                        origin_pk INTEGER REFERENCES OriginDestinationDictionary(pk),
                        train_name TEXT NOT NULL,
                        UNIQUE(stop_pk,time,trip_code,line_code_pk))
                    ''')
                self.exec('''
                    INSERT INTO Arrival (stop_pk, time, delay, trip_code, line_code_pk, origin_pk, train_name)
                    SELECT stop_pk, time, delay, trip_code, line_code_pk, OriginDestinationDictionary.pk, train_name
                    FROM Arrival_TMP
                    INNER JOIN OriginDestinationDictionary ON OriginDestinationDictionary.name = Arrival_TMP.origin
                    ''')

                self.exec('DROP TABLE Arrival_TMP')
                self.exec('DROP TABLE Departure_TMP')

                dbver = 4
                should_vacuum = True

            if dbver < 5:
                # db schema v5: manual dictionary compression for train names
                self.exec('''
                    CREATE TABLE TrainNameDictionary(
                        pk INTEGER PRIMARY KEY,
                        train_name TEXT UNIQUE NOT NULL)
                    ''')
                self.exec('''
                    INSERT OR IGNORE INTO TrainNameDictionary(train_name)
                    SELECT train_name FROM Departure
                    ''')
                self.exec('''
                    INSERT OR IGNORE INTO TrainNameDictionary(train_name)
                    SELECT train_name FROM Arrival
                    ''')

                self.exec('''ALTER TABLE Departure RENAME TO Departure_TMP''')
                self.exec('''
                    CREATE TABLE Departure(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code_pk INTEGER REFERENCES LineCodeDictionary(pk),
                        destination_pk INTEGER REFERENCES OriginDestinationDictionary(pk),
                        train_name_pk INTEGER REFERENCES TrainNameDictionary(pk),
                        UNIQUE(stop_pk,time,trip_code,line_code_pk))
                    ''')
                self.exec('''
                    INSERT INTO Departure (stop_pk, time, delay, trip_code, line_code_pk, destination_pk, train_name_pk)
                    SELECT stop_pk, time, delay, trip_code, line_code_pk, destination_pk, TrainNameDictionary.pk
                    FROM Departure_TMP
                    INNER JOIN TrainNameDictionary WHERE TrainNameDictionary.train_name = Departure_TMP.train_name
                    ''')

                self.exec('''ALTER TABLE Arrival RENAME TO Arrival_TMP''')
                self.exec('''
                    CREATE TABLE Arrival(
                        stop_pk INTEGER REFERENCES WatchedStop(pk),
                        time TIMESTAMP NOT NULL,
                        delay REAL,
                        trip_code TEXT NOT NULL,
                        line_code_pk INTEGER REFERENCES LineCodeDictionary(pk),
                        origin_pk INTEGER REFERENCES OriginDestinationDictionary(pk),
                        train_name_pk INTEGER REFERENCES TrainNameDictionary(pk),
                        UNIQUE(stop_pk,time,trip_code,line_code_pk))
                    ''')
                self.exec('''
                    INSERT INTO Arrival (stop_pk, time, delay, trip_code, line_code_pk, origin_pk, train_name_pk)
                    SELECT stop_pk, time, delay, trip_code, line_code_pk, origin_pk, TrainNameDictionary.pk
                    FROM Arrival_TMP
                    INNER JOIN TrainNameDictionary ON TrainNameDictionary.train_name = Arrival_TMP.train_name
                    ''')

                self.exec('DROP TABLE Arrival_TMP')
                self.exec('DROP TABLE Departure_TMP')

                dbver = 5
                should_vacuum = True

            assert dbver == DBVER_CURRENT
            self.exec('PRAGMA user_version = {}'.format(dbver))

        if should_vacuum:
            self.exec('VACUUM')

    def _setup_temps(self):
        self.exec('''
            CREATE TEMP VIEW trip
            AS SELECT TrainNameDictionary.train_name as train_name,
                      WatchedStop_Departure.id as origin,
                      WatchedStop_Arrival.id as destination,
                      strftime('%Y-%m-%d', Departure.time) as date,
                      strftime('%H:%M', Departure.time) as dep_time,
                      Departure.delay as dep_delay,
                      strftime('%H:%M', Arrival.time) as arr_time,
                      Arrival.delay as arr_delay
            FROM Departure
            JOIN Arrival ON Arrival.line_code_pk = Departure.line_code_pk
             AND Arrival.trip_code = Departure.trip_code
             AND Arrival.time > Departure.time
             AND julianday(Arrival.time) - julianday(Departure.time) < 0.5
            JOIN WatchedStop WatchedStop_Departure ON WatchedStop_Departure.pk = Departure.stop_pk
            JOIN WatchedStop WatchedStop_Arrival ON WatchedStop_Arrival.pk = Arrival.stop_pk
            JOIN TrainNameDictionary ON TrainNameDictionary.pk = Departure.train_name_pk''')

    def _materialize_trip_view(self):
        with self:
            self.exec('''
                CREATE TEMP TABLE Trip_ AS
                SELECT train_name AS train_name,
                       origin AS origin,
                       destination AS destination,
                       date AS date,
                       dep_time AS dep_time,
                       dep_delay AS dep_delay,
                       arr_time AS arr_time,
                       arr_delay AS arr_delay
                FROM Trip''')
            self.exec('DROP VIEW Trip')
            self.exec('ALTER TABLE Trip_ RENAME TO Trip')
            self.exec('CREATE INDEX Trip_Index_1 ON Trip(origin, destination, dep_time)')

class DatabaseAccessor:
    """high-level database access"""

    def __init__(self, connection: DatabaseConnection) -> None:
        self.connection = connection

    def materialize_trips(self) -> None:
        """creates a temporary table out of all the trips. This speeds up trip-related OLAP."""
        self.connection._materialize_trip_view()

    def persist_watched_stop(self, stop: WatchedStop) -> None:
        with self.connection:
            self.connection.exec('''
                INSERT OR REPLACE INTO WatchedStop (id, efa_stop_id, name)
                VALUES (:uuid, :sid, :name)''',
                uuid=stop.id, sid=stop.backend_stop_id, name=stop.name)

    def _watched_stop_pk(self, stop: WatchedStop) -> int:
        stop_pk, = self.connection.exec('SELECT pk FROM WatchedStop WHERE id = :uuid', uuid=stop.id).fetchone()
        return stop_pk

    def _persist_line_code(self, line_code: str) -> int:
        self.connection.exec('''INSERT OR IGNORE
                INTO LineCodeDictionary(line_code)
                VALUES (:lc)''', lc=line_code)
        line_code_pk, = self.connection.exec('''
            SELECT pk FROM LineCodeDictionary WHERE line_code = :lc''',
            lc=line_code).fetchone()
        return line_code_pk

    def _persist_train_name(self, train_name: str) -> int:
        self.connection.exec('''INSERT OR IGNORE
                INTO TrainNameDictionary(train_name)
                VALUES (:name)''', name=train_name)
        train_name_pk, = self.connection.exec('''
            SELECT pk FROM TrainNameDictionary WHERE train_name = :tn''',
            tn=train_name).fetchone()
        return train_name_pk

    def _persist_origin_destination(self, name: str) -> int:
        self.connection.exec('''INSERT OR IGNORE
                INTO OriginDestinationDictionary(name)
                VALUES (:name)''', name=name)
        name_pk, = self.connection.exec('''
            SELECT pk FROM OriginDestinationDictionary WHERE name = :name''',
            name=name).fetchone()
        return name_pk


    def persist_departure(self, stop: WatchedStop, dep: Departure) -> None:
        with self.connection:
            # save scheduled data, unless it is already saved
            stop_pk = self._watched_stop_pk(stop)
            line_code_pk = self._persist_line_code(dep.line_code)
            train_name_pk = self._persist_train_name(dep.train_name)
            destination_pk = self._persist_origin_destination(dep.destination)

            self.connection.exec('''INSERT OR IGNORE
                INTO Departure (stop_pk, time, trip_code, line_code_pk, destination_pk, train_name_pk)
                VALUES (:sid, :time, :tc, :lc, :dest, :name)''',
                sid=stop_pk, time=dep.time, tc=dep.trip_code, lc=line_code_pk,
                dest=destination_pk, name=train_name_pk)

            # then overwrite delay if we have one
            if dep.delay is not None:
                self.connection.exec('''UPDATE Departure SET delay = :delay
                    WHERE stop_pk=:sid AND time=:time AND trip_code=:tc AND line_code_pk=:lc''',
                    delay=dep.delay, sid=stop_pk, time=dep.time, tc=dep.trip_code, lc=line_code_pk)

    def persist_arrival(self, stop: WatchedStop, arr: Arrival) -> None:
        with self.connection:
            # save scheduled data, unless it is already saved
            stop_pk = self._watched_stop_pk(stop)
            line_code_pk = self._persist_line_code(arr.line_code)
            train_name_pk = self._persist_train_name(arr.train_name)
            origin_pk = self._persist_origin_destination(arr.origin)

            self.connection.exec('''INSERT OR IGNORE
                INTO Arrival (stop_pk, time, trip_code, line_code_pk, origin_pk, train_name_pk)
                VALUES (:sid, :time, :tc, :lc, :orig, :name)''',
                sid=stop_pk, time=arr.time, tc=arr.trip_code, lc=line_code_pk,
                orig=origin_pk, name=train_name_pk)

            # then overwrite delay if we have one
            if arr.delay is not None:
                self.connection.exec('''UPDATE Arrival SET delay = :delay
                    WHERE stop_pk=:sid AND time=:time AND trip_code=:tc AND line_code_pk=:lc''',
                    delay=arr.delay, sid=stop_pk, time=arr.time, tc=arr.trip_code, lc=line_code_pk)

    def all_watched_stops(self) -> Iterator[WatchedStop]:
        for id, efa_stop_id, name in self.connection.exec('SELECT id, efa_stop_id, name FROM WatchedStop'):
            yield WatchedStop(id, efa_stop_id, name)

    def watched_stop_by_id(self, id) -> WatchedStop:
        id, efa_stop_id, name = self.connection.exec(
            'SELECT id, efa_stop_id, name FROM WatchedStop WHERE id = :id', id=id).fetchone()
        return WatchedStop(id, efa_stop_id, name)

    def trips(self, origin: WatchedStop, dest: WatchedStop) -> Iterator[Trip]:
        for train_name, date, dep_time, dep_delay, arr_time, arr_delay in self.connection.exec(
                '''SELECT train_name, date, dep_time, dep_delay, arr_time, arr_delay FROM trip
                   WHERE origin = :origin AND destination = :destination
                   ORDER BY dep_time ASC''', origin=origin.id, destination=dest.id):
            yield Trip(origin, dest, date, dep_time, dep_delay, arr_time, arr_delay, train_name)

    def aggregated_trips(self, origin: WatchedStop, dest: WatchedStop, datetype:str='any') -> Iterator[AggregatedTrip]:
        for train_name, dep_time, dep_delay, dep_delay_perc, dep_delay_stdev, \
            arr_time, arr_delay, arr_delay_perc, arr_delay_stdev, count in self.connection.exec(
                '''SELECT train_name, dep_time, median(dep_delay), percentile(90, dep_delay), stdev(dep_delay),
                          arr_time, median(arr_delay), percentile(90, arr_delay), stdev(arr_delay), COUNT(*)
                   FROM trip
                   WHERE origin = :origin AND destination = :destination
                   AND''' + DATE_TYPE_SQL_CHECK('date', datetype) +'''
                   GROUP BY train_name, dep_time, arr_time
                   ORDER BY dep_time ASC''', origin=origin.id, destination=dest.id):
            yield AggregatedTrip(train_name,
                                 datetime.strptime(dep_time, '%H:%M').time(),
                                 dep_delay, dep_delay_perc, dep_delay_stdev,
                                 datetime.strptime(arr_time, '%H:%M').time(),
                                 arr_delay, arr_delay_perc, arr_delay_stdev, count)

    def aggregated_trip_dates(self, origin: WatchedStop, dest: WatchedStop, datetype:str='any') -> AggregateDateRange:
        count, min, max = self.connection.exec(
            '''SELECT COUNT(distinct date), MIN(date), MAX(date) FROM Trip
               WHERE origin = :origin AND destination = :destination AND'''
                    + DATE_TYPE_SQL_CHECK('date', datetype),
                origin=origin.id, destination=dest.id).fetchone()

        if min is not None:
            min = datetime.strptime(min, '%Y-%m-%d').date()
        if max is not None:
            max = datetime.strptime(max, '%Y-%m-%d').date()

        return AggregateDateRange(int(count), min, max)

    def aggregated_departures(self, stop: WatchedStop) -> Iterator[AggregatedDeparture]:
        for train_name, destination, hour, minute, delay, count in self.connection.exec(
                '''SELECT train_name, OriginDestinationDictionary.name, strftime('%H', time) as hour,
                        strftime('%M', time) as minute, median(delay), COUNT(*)
                   FROM Departure
                   JOIN WatchedStop ON WatchedStop.pk = Departure.stop_pk
                   JOIN TrainNameDictionary ON TrainNameDictionary.pk = Departure.train_name_pk
                   JOIN OriginDestinationDictionary ON OriginDestinationDictionary.pk = Departure.destination_pk
                   WHERE WatchedStop.id = :stop
                   GROUP BY train_name, OriginDestinationDictionary.name, hour, minute
                   ORDER BY hour, minute ASC''', stop=stop.id):
            yield AggregatedDeparture(train_name, destination, time(hour=int(hour), minute=int(minute)), delay, count)

    def aggregated_departure_dates(self, stop: WatchedStop) -> AggregateDateRange:
        count, min, max = self.connection.exec(
                '''SELECT COUNT(distinct strftime('%Y-%m-%d', time)),
                          MIN(strftime('%Y-%m-%d', time)),
                          MAX(strftime('%Y-%m-%d', time))
                   FROM Departure
                   JOIN WatchedStop ON WatchedStop.pk = Departure.stop_pk
                   WHERE WatchedStop.id = :stop''', stop=stop.id).fetchone()

        return AggregateDateRange(int(count),
                                  datetime.strptime(min, '%Y-%m-%d').date(),
                                  datetime.strptime(max, '%Y-%m-%d').date())
