#!/usr/bin/env python3

from uuid import uuid4

from bahnstat.mechanize_mini import Browser

from bahnstat.efaxmlclient import EfaXmlClient
from bahnstat.datatypes import *


assert __name__ == '__main__'

ka = WatchedStop(uuid4(), 7000090, 'Karlsruhe Hbf')


client = EfaXmlClient('Mozilla/4.0 (compatible; MSIE 6.0; Windows 98)')

dm = client.departure_monitor(ka)

print('Departure Monitor for {} at {}'.format(dm.stop_name, dm.now))
print('')

for dep in dm.departures:
    print('{:02}:{:02} +{}    {} ({})'.format(dep.time.hour, dep.time.minute, dep.delay, dep.destination, dep.train_name))

print('');print('');print('')

am = client.arrival_monitor(ka)

print('Arrival Monitor for {} at {}'.format(am.stop_name, am.now))
print('')
for arr in am.arrivals:
    print('{:02}:{:02} +{}    {} ({})'.format(arr.time.hour, arr.time.minute, arr.delay, arr.origin, arr.train_name))
