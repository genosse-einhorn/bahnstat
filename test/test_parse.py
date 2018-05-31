#!/usr/bin/env python3

import unittest
import os
import math
from datetime import datetime, date
from uuid import UUID

from efaxmlparser import departure_monitor_from_response
import mechanize_mini

def TestCaseXml(filename):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'testcases', filename), 'r', encoding='latin-1') as f:
        return mechanize_mini.HTML(f.read())

class TestDmParser(unittest.TestCase):
    def test_basic(self):
        xml = TestCaseXml('XML_DM_REQUEST zugausfall rb neustadt.xml')
        dm = departure_monitor_from_response(xml)

        self.assertEqual(dm.now, datetime(2018, 5, 22, 8, 15, 9))
        self.assertEqual(dm.stop_gid, 'de:08212:90')
        self.assertEqual(dm.stop_name, 'Karlsruhe, Karlsruhe Hbf')
        self.assertEqual(len(dm.departures), 40)

        self.assertEqual(dm.departures[0].destination, 'Mannheim, Hauptbahnhof')
        self.assertEqual(dm.departures[0].time, datetime(2018, 5, 22, 8, 25))
        self.assertEqual(dm.departures[0].train_name, 'RB 38824')
        self.assertEqual(dm.departures[0].delay, 0)
        self.assertEqual(dm.departures[0].line_code, 'ddb:90700: :R:j18')
        self.assertEqual(dm.departures[0].trip_code, '38824')
        self.assertEqual(dm.departures[0].stop_id, '7000090')

        self.assertEqual(dm.departures[2].destination, 'Neustadt, Hauptbahnhof')
        self.assertEqual(dm.departures[2].time, datetime(2018, 5, 22, 8, 33))
        self.assertEqual(dm.departures[2].train_name, 'RB 12438')
        self.assertEqual(dm.departures[2].delay, math.inf)
        self.assertEqual(dm.departures[2].line_code, 'ddb:90S51: :R:j18')
        self.assertEqual(dm.departures[2].trip_code, '12438')
        self.assertEqual(dm.departures[2].stop_id, '7000090')
        #print(dm.departures[2].tripstoptimes_url)

        self.assertEqual(dm.departures[3].destination, 'S81 Rastatt')
        self.assertEqual(dm.departures[3].time, datetime(2018, 5, 22, 8, 43))
        self.assertEqual(dm.departures[3].train_name, 'S81 (AVG)')
        self.assertEqual(dm.departures[3].delay, None)
        self.assertEqual(dm.departures[3].line_code, 'kvv:22081:E:H:j18')
        self.assertEqual(dm.departures[3].trip_code, '19506')
        self.assertEqual(dm.departures[3].stop_id, '7000090')

if __name__ == '__main__':
    unittest.main()
