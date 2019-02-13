#!/usr/bin/env python3

from bahnstat.datatypes import *
from bahnstat.database import *

from argparse import ArgumentParser
from datetime import datetime

ap = ArgumentParser()
ap.add_argument('--db-file', required=True)
args = ap.parse_args()


db = DatabaseAccessor(DatabaseConnection(args.db_file))

stations = list(db.all_watched_stops())

if len(stations) < 2:
    print('WARN: less than two watched stops')

for s in stations:
    if not s.active:
        continue

    deps = db.departures(s, daterange=1)
    if next(deps, None) is None:
        print('WARN: no departures at {} in the last 24 hours'.format(s.name))

    arrs = db.arrivals(s, daterange=1)
    if next(arrs, None) is None:
        print('WARN: no arrivals at {} in the last 24 hours'.format(s.name))

