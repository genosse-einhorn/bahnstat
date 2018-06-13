#!/usr/bin/env python3

from bahnstat.dbtimetableclient import *
from datetime import datetime, date, timedelta
import time
import os
import logging

assert __name__ == '__main__'

logging.basicConfig(level=logging.DEBUG)


os.environ['TZ'] = 'Europe/Berlin'
time.tzset()

KARLSRUHE = 8000191
HAMBURG_S = 8098549
OFFENBURG = 8000290
client = DbTimetableClient(KARLSRUHE, auth='XXX')

def dep_train_name(stop):
    if stop.departure.line is not None:
        return '{} {} ({})'.format(stop.label.category, stop.departure.line, stop.label.number)
    else:
        return '{} {}'.format(stop.label.category, stop.label.number)

def arr_train_name(stop):
    if stop.arrival.line is not None:
        return '{} {} ({})'.format(stop.label.category, stop.arrival.line, stop.label.number)
    else:
        return '{} {}'.format(stop.label.category, stop.label.number)

def print_dep_monitor(client):
    print('')
    print('Departure Monitor {}'.format(NOW))
    print('')

    deps = client.departures

    c = 0
    for dep in deps:
        print('{:02}:{:02} +{}    {} ({}) line={} tripcode={}'.format(dep.time.hour, dep.time.minute, dep.delay, dep.destination, dep.train_name, dep.line_code, dep.trip_code))

        c = c + 1

        if c > 20:
            break

def print_arr_monitor(client):
    print('')
    print('Arrival Monitor {}'.format(NOW))
    print('')

    arrs = client.arrivals

    c = 0
    for arr in arrs:
        print('{:02}:{:02} +{}    {} ({}) line={} tripcode={}'.format(arr.time.hour, arr.time.minute, arr.delay, arr.origin, arr.train_name, arr.line_code, arr.trip_code))

        c = c + 1

        if c > 20:
            break

while True:
    NOW = datetime.now()

    board = client.current_board(NOW)

    print_dep_monitor(board)
    print_arr_monitor(board)

    time.sleep(30)
