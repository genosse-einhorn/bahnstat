#!/usr/bin/env python3

from mechanize_mini import Browser

from efaxmlparser import *
from datatypes import *


assert __name__ == '__main__'

ka = WatchedStop(None, 7000090, 'Karlsruhe Hbf')


b = Browser('Mozilla/4.0 (compatible; MSIE 6.0; Windows 98)')

dm = departure_monitor_from_response(b.open(ka.dm_url()).document_element)

print('Departure Monitor for {} at {}'.format(dm.stop_name, dm.now))
print('')

for dep in dm.departures:
    print('{:02}:{:02} +{}    {} ({})'.format(dep.time.hour, dep.time.minute, dep.delay, dep.destination, dep.train_name))

print('');print('');print('')

am = arrival_monitor_from_response(b.open(ka.dm_url(mode='arr')).document_element)

print('Arrival Monitor for {} at {}'.format(am.stop_name, am.now))
print('')
for arr in am.arrivals:
    print('{:02}:{:02} +{}    {} ({})'.format(arr.time.hour, arr.time.minute, arr.delay, arr.origin, arr.train_name))
