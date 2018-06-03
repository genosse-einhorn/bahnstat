#!/usr/bin/env python3

from bahnstat.datatypes import *
from bahnstat.database import *
from bahnstat.htmlstatgen import *

import os
import logging
from argparse import ArgumentParser

def writefile(path, text):
    logging.debug('writing file {}'.format(path))

    os.makedirs(os.path.dirname(path), mode=0o755, exist_ok=True)
    with open(path, 'w') as f:
        f.write(text)


ap = ArgumentParser()

ap.add_argument('--db-file', required=True)
ap.add_argument('--outdir', required=True)
ap.add_argument('--log', default='WARN')

args = ap.parse_args()

num_loglevel = getattr(logging, args.log.upper(), None)
if not isinstance(num_loglevel, int):
    raise ValueError('Invalid log level: {}'.format(args.log))

logging.basicConfig(level=num_loglevel)

db = DatabaseAccessor(DatabaseConnection(args.db_file))
outdir = args.outdir
gen = HtmlStatGen(db)

logging.debug('materializing trip view')
db.materialize_trips()
logging.debug('done materializing trip view')

stations = list(db.all_watched_stops())

writefile(os.path.join(outdir, 'index.html'), gen.station_list())
for s in stations:
    writefile(os.path.join(outdir, str(s.id), 'dep', 'all.html'), gen.dep_list(s))

    for d in stations:
        if d == s:
            continue

        writefile(os.path.join(outdir, str(s.id), str(d.id), 'all.html'), gen.trip_list(s, d))


