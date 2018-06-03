import sqlite3
from uuid import UUID, uuid5
from datetime import datetime, time
import random
import statistics

from bahnstat.datatypes import *

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

DBVER_CURRENT = 1

class DatabaseConnection:
    """low-level database access"""
    def __init__(self, dbfile):
        self.conn = sqlite3.connect(dbfile, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.isolation_level = None

        self.conn.create_aggregate('median', 1, MedianAggregate)

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

            assert dbver == DBVER_CURRENT
            self.exec('PRAGMA user_version = {}'.format(dbver))

    def _setup_temps(self):
        self.exec('''
            CREATE TEMP VIEW trip
            AS SELECT Departure.train_name as train_name,
                      Departure.stop as origin,
                      Arrival.stop as destination,
                      strftime('%Y-%m-%d', Departure.time) as date,
                      strftime('%H:%M', Departure.time) as dep_time,
                      Departure.delay as dep_delay,
                      strftime('%H:%M', Arrival.time) as arr_time,
                      Arrival.delay as arr_delay
            FROM Departure
            JOIN Arrival ON Arrival.line_code = Departure.line_code
             AND Arrival.trip_code = Departure.trip_code
             AND Arrival.time > Departure.time
             AND julianday(Arrival.time) - julianday(Departure.time) < 0.5''')

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

    def __init__(self, connection):
        self.connection = connection

    def materialize_trips(self):
        """creates a temporary table out of all the trips. This speeds up trip-related OLAP."""
        self.connection._materialize_trip_view()

    def persist_watched_stop(self, stop):
        with self.connection:
            self.connection.exec('''
                INSERT OR REPLACE INTO WatchedStop (id, efa_stop_id, name)
                VALUES (:uuid, :sid, :name)''',
                uuid=stop.id, sid=stop.efa_stop_id, name=stop.name)

    def persist_departure(self, stop, dep):
        with self.connection:
            # save scheduled data, unless it is already saved
            self.connection.exec('''INSERT OR IGNORE
                INTO Departure (stop, time, trip_code, line_code, destination, train_name)
                VALUES (:sid, :time, :tc, :lc, :dest, :name)''',
                sid=stop.id, time=dep.time, tc=dep.trip_code, lc=dep.line_code,
                dest=dep.destination, name=dep.train_name)

            # then overwrite delay if we have one
            if dep.delay is not None:
                self.connection.exec('''UPDATE Departure SET delay = :delay
                    WHERE stop=:sid AND time=:time AND trip_code=:tc AND line_code=:lc''',
                    delay=dep.delay, sid=stop.id, time=dep.time, tc=dep.trip_code, lc=dep.line_code)

    def persist_arrival(self, stop, arr):
        with self.connection:
            # save scheduled data, unless it is already saved
            self.connection.exec('''INSERT OR IGNORE
                INTO Arrival (stop, time, trip_code, line_code, origin, train_name)
                VALUES (:sid, :time, :tc, :lc, :orig, :name)''',
                sid=stop.id, time=arr.time, tc=arr.trip_code, lc=arr.line_code,
                orig=arr.origin, name=arr.train_name)

            # then overwrite delay if we have one
            if arr.delay is not None:
                self.connection.exec('''UPDATE Arrival SET delay = :delay
                    WHERE stop=:sid AND time=:time AND trip_code=:tc AND line_code=:lc''',
                    delay=arr.delay, sid=stop.id, time=arr.time, tc=arr.trip_code, lc=arr.line_code)

    def all_watched_stops(self):
        for id, efa_stop_id, name in self.connection.exec('SELECT id, efa_stop_id, name FROM WatchedStop'):
            yield WatchedStop(id, efa_stop_id, name)

    def watched_stop_by_id(self, id):
        id, efa_stop_id, name = self.connection.exec(
            'SELECT id, efa_stop_id, name FROM WatchedStop WHERE id = :id', id=id).fetchone()
        return WatchedStop(id, efa_stop_id, name)

    def trips(self, origin, dest):
        for train_name, date, dep_time, dep_delay, arr_time, arr_delay in self.connection.exec(
                '''SELECT train_name, date, dep_time, dep_delay, arr_time, arr_delay FROM trip
                   WHERE origin = :origin AND destination = :destination
                   ORDER BY dep_time ASC''', origin=origin.id, destination=dest.id):
            yield Trip(origin, dest, date, dep_time, dep_delay, arr_time, arr_delay, train_name)

    def aggregated_trips(self, origin, dest):
        for train_name, dep_time, dep_delay, arr_time, arr_delay, count in self.connection.exec(
                '''SELECT train_name, dep_time, median(dep_delay), arr_time, median(arr_delay), COUNT(*)
                   FROM trip
                   WHERE origin = :origin AND destination = :destination
                   GROUP BY train_name, dep_time, arr_time
                   ORDER BY dep_time ASC''', origin=origin.id, destination=dest.id):
            yield AggregatedTrip(train_name, datetime.strptime(dep_time, '%H:%M').time(), dep_delay,
                                 datetime.strptime(arr_time, '%H:%M').time(), arr_delay, count)

    def aggregated_trip_dates(self, origin, dest):
        count, min, max = self.connection.exec(
            '''SELECT COUNT(distinct date), MIN(date), MAX(date) FROM Trip
               WHERE origin = :origin AND destination = :destination''',
                origin=origin.id, destination=dest.id).fetchone()

        return AggregateDateRange(int(count),
                                  datetime.strptime(min, '%Y-%m-%d').date(),
                                  datetime.strptime(max, '%Y-%m-%d').date())

    def aggregated_departures(self, stop):
        for train_name, destination, hour, minute, delay, count in self.connection.exec(
                '''SELECT train_name, destination, strftime('%H', time) as hour,
                        strftime('%M', time) as minute, median(delay), COUNT(*)
                   FROM Departure
                   WHERE stop = :stop
                   GROUP BY train_name, destination, hour, minute
                   ORDER BY hour, minute ASC''', stop=stop.id):
            yield AggregatedDeparture(train_name, destination, time(hour=int(hour), minute=int(minute)), delay, count)

    def aggregated_departure_dates(self, stop):
        count, min, max = self.connection.exec(
                '''SELECT COUNT(distinct strftime('%Y-%m-%d', time)),
                          MIN(strftime('%Y-%m-%d', time)),
                          MAX(strftime('%Y-%m-%d', time))
                   FROM Departure
                   WHERE stop = :stop''', stop=stop.id).fetchone()

        return AggregateDateRange(int(count),
                                  datetime.strptime(min, '%Y-%m-%d').date(),
                                  datetime.strptime(max, '%Y-%m-%d').date())
