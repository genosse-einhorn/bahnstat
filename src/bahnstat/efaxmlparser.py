from datetime import datetime, date, timedelta
from bahnstat.datatypes import *

def datetime_from_itdDateTime(itdDateTime):
    itdDate = itdDateTime.query_selector('itdDate')
    assert itdDate is not None

    itdTime = itdDateTime.query_selector('itdTime')
    assert itdTime is not None

    year = int(itdDate.get('year'))
    month = int(itdDate.get('month'))
    day = int(itdDate.get('day'))
    hour = int(itdTime.get('hour'))
    minute = int(itdTime.get('minute'))

    if year > 0 and month > 0 and day > 0 and hour >= 0 and minute >= 0:
        return datetime(year, month, day, hour, minute)
    else:
        return None

def departure_from_itdDeparture(itdDeparture):
    if itdDeparture.query_selector('itdServingLine').get('trainType') is not None:
        train_name = '{} {}'.format(itdDeparture.query_selector('itdServingLine').get('trainType', ''),
                itdDeparture.query_selector('itdServingLine').get('trainNum', ''))
    elif itdDeparture.query_selector('itdServingLine').get('symbol') is not None:
        train_name = itdDeparture.query_selector('itdServingLine').get('symbol')
    else:
        train_name = None

    delay = None
    if itdDeparture.query_selector('itdNoTrain').get('delay') is not None:
        delay = int(itdDeparture.query_selector('itdNoTrain').get('delay'))
        if delay == -9999: # train canceled -> we model it as infinite delay
            delay = float('inf')

    return Departure(
        datetime_from_itdDateTime(itdDeparture.query_selector('itdDateTime')),
        train_name,
        itdDeparture.query_selector('itdServingLine').get('direction'),
        itdDeparture.get('stopID'),
        itdDeparture.query_selector('itdServingLine').get('key'),
        itdDeparture.query_selector('itdServingLine').get('stateless'),
        delay)

def arrival_from_itdArrival(itdArrival):
    if itdArrival.query_selector('itdServingLine').get('trainType') is not None:
        train_name = '{} {}'.format(itdArrival.query_selector('itdServingLine').get('trainType', ''),
                itdArrival.query_selector('itdServingLine').get('trainNum', ''))
    elif itdArrival.query_selector('itdServingLine').get('symbol') is not None:
        train_name = itdArrival.query_selector('itdServingLine').get('symbol')
    else:
        train_name = None

    delay = None
    if itdArrival.query_selector('itdNoTrain').get('delay') is not None:
        delay = int(itdArrival.query_selector('itdNoTrain').get('delay'))
        if delay == -9999: # train canceled -> we model it as infinite delay
            delay = float('inf')

    return Arrival(
        datetime_from_itdDateTime(itdArrival.query_selector('itdDateTime')),
        train_name,
        itdArrival.query_selector('itdServingLine').get('directionFrom'),
        itdArrival.get('stopID'),
        itdArrival.query_selector('itdServingLine').get('key'),
        itdArrival.query_selector('itdServingLine').get('stateless'),
        delay)

def departure_monitor_from_response(xmlnode):
    if xmlnode.tag != 'itdrequest':
        xmlnode = xmlnode.query_selector('itdRequest')
        assert xmlnode is not None

    timestr = xmlnode.get('now')
    time = datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S')

    stop_gid = xmlnode.query_selector('itdDepartureMonitorRequest itdOdv itdOdvName odvNameElem').get('gid')
    stop_name = xmlnode.query_selector('itdDepartureMonitorRequest itdOdv itdOdvName odvNameElem').text_content

    deps = [departure_from_itdDeparture(d) for d in xmlnode.query_selector_all('itdDepartureList itdDeparture')]

    return DepartureMonitor(time, stop_gid, stop_name, deps)

def arrival_monitor_from_response(xmlnode):
    if xmlnode.tag != 'itdrequest':
        xmlnode = xmlnode.query_selector('itdRequest')
        assert xmlnode is not None

    timestr = xmlnode.get('now')
    time = datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S')

    stop_gid = xmlnode.query_selector('itdDepartureMonitorRequest itdOdv itdOdvName odvNameElem').get('gid')
    stop_name = xmlnode.query_selector('itdDepartureMonitorRequest itdOdv itdOdvName odvNameElem').text_content

    deps = [arrival_from_itdArrival(d) for d in xmlnode.query_selector_all('itdArrivalList itdArrival')]

    return ArrivalMonitor(time, stop_gid, stop_name, deps)
