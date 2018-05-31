import html
import math
from datetime import date, time, datetime

CSS =   '''
        .ontime {
            color: #78BE14;
        }
        .delayed {
            color: #ff0000;
        }
        .lowdata {
            color: #666;
        }
        html, body {
            background-color: #fff;
            color: #111;
            font-family: sans-serif;
        }
        td, th {
            padding: 0.2em;
            white-space: nowrap;
        }
        td {
            background-color: #eee;
        }
        th {
            text-align: left;
            background-color: #ddd;
        }
        '''

HTML_PREAMBLE = '<!DOCTYPE html><meta charset=UTF-8><meta name="viewport" content="width=device-width, initial-scale=1"><style>'+CSS+'</style>'

HTML_POSTAMBLE = '<hr><p>Generiert: {} UTC</p>'.format(datetime.utcnow())

def TITLE(t):
    return '<title>{}</title>'.format(html.escape(t))

def H1(h):
    return '<h1>{}</h1>'.format(html.escape(h))

def A(href, text):
    return '<a href="{}">{}</a>'.format(html.escape(href), html.escape(text))

def DELAY(d):
    if d is None:
        return ''
    elif d == math.inf:
        return '<span class=delayed>+∞</span>'
    elif d < 5:
        return '<span class=ontime>+{:.0f}</span>'.format(d)
    else:
        return '<span class=delayed>+{:.0f}</span>'.format(d)

def highest_smaller(target, haystack):
    r = None
    for i in haystack:
        if i <= target and (r is None or r < i):
            r = i

    return r

LEGEND = '''
    <h2>Legende</h2>
    <dl>
        <dt>(n)</dt>
        <dd>Anzahl Datensätze für diesen Zug. Sind signifikant weniger Datensätze als Verkehrstage beobachtet worden,
            dann wird die Zeile heller gefärbt.</dd>
        <dt>50%</dt>
        <dd>Median der Verspätung: 50% der beobachteten Züge hatten höchstens so viel Verspätung.</dd>
        <dt>90%</dt>
        <dd>90-Perzentil der Verspätung: 90% der beobachteten Züge hatten höchstens so viel Verspätung.</dd>
        <dt>σ</dt>
        <dd>Stichproben-Standardabweichung der Verspätung.</dd>
        <dt>∞</dt>
        <dd>Fiktiver Verspätungswert für einen ausgefallenen Zug.</dd>
    </dl>'''

class HtmlStatGen:
    def __init__(self, db):
        self.db = db

    def station_list(self):
        l = [HTML_PREAMBLE, TITLE('Bahnstatistik auswählen')]

        station_list = list(self.db.all_watched_stops())

        l.append(H1('Route auswählen'))

        l.append('<table>')
        l.append('<tr><th>Von<th>Nach')

        for f in station_list:
            l.append('<tr>')
            l.append('<td>')
            l.append(html.escape(f.name))
            l.append('<td>')

            l.append('<ul style="list-style: none;padding-left: 0;">')
            for t in station_list:
                if t == f:
                    continue

                l.append('<li>')
                l.append(A('{}/{}/all.html'.format(f.id, t.id), '➔ {}'.format(t.name)))

            l.append('</ul>')

        l.append('</table>')

        l.append(H1('Abfahrtsstatistik'))
        l.append('<ul>')
        for s in station_list:
            l.append('<li>')
            l.append(A('{}/dep/all.html'.format(s.id), s.name))

        l.append('</ul>')

        l.append(HTML_POSTAMBLE)

        return ''.join(l)

    def trip_list(self, origin, dest):
        l = [HTML_PREAMBLE, TITLE('Statistik {} ➔ {}'.format(origin.name, dest.name))]

        l.append(H1('Statistik {} ➔ {}'.format(origin.name, dest.name)))

        dates = self.db.aggregated_trip_dates(origin, dest)
        l.append('<p>Statistik über {} Verkehrstage von {} bis {}</p>'.format(dates.count, dates.first, dates.last))

        jumptimes = [time(2,0), time(4,0), time(6,0), time(8, 0), time(10, 0), time(12, 0),
                     time(14, 0), time(16, 0), time(18, 0), time(20, 0), time(22, 0)]

        #l.append('<p>Springe zu')
        #for t in jumptimes:
            #l.append(' | ')
            #l.append(A('#time_{:02}_{:02}'.format(t.hour, t.minute), '{:02}:{:02}'.format(t.hour, t.minute)))
        #l.append('</p>')

        l.append('<table>')
        l.append('<tr>')
        l.append('<th colspan=2><th colspan=4>{}'.format(html.escape(origin.name)))
        l.append('<th>➔<th colspan=4>{}'.format(html.escape(dest.name)))
        l.append('<tr>')
        l.append('<th>Zug<th>(n)<th>Plan<th>50%<th>90%<th>σ<th><th>Plan<th>50%<th>90%<th>σ')

        last_time = time(23,59)
        for t in self.db.aggregated_trips(origin, dest):

            jt = highest_smaller(t.dep_time, jumptimes)
            if jt is not None and jt > last_time:
                l.append('<tr id="time_{:02}_{:02}">'.format(jt.hour, jt.minute))
                l.append('<th colspan=12>')
                l.append('{:02}:{:02} Uhr'.format(jt.hour, jt.minute))

            last_time = t.dep_time

            if t.count < 0.5*dates.count:
                l.append('<tr class=lowdata>')
            else:
                l.append('<tr>')

            l.append('<td>')
            l.append(html.escape('{}'.format(t.train_name)))
            l.append('<td>')
            l.append('({})'.format(t.count))
            l.append('<td>')
            l.append(html.escape('{:02}:{:02}'.format(t.dep_time.hour, t.dep_time.minute)))
            l.append('<td>')
            l.append(DELAY(t.dep_delay_median))
            l.append('<td>')
            # TODO: 90th percentile
            l.append('<td>')
            # TODO: stdev
            l.append('<td>➔')
            l.append('<td>')
            l.append(html.escape('{:02}:{:02}'.format(t.arr_time.hour, t.arr_time.minute)))
            l.append('<td>')
            l.append(DELAY(t.arr_delay_median))
            l.append('<td>')
            # TODO: 90th percentile
            l.append('<td>')
            # TODO: stdev

        l.append('</table>')

        l.append(LEGEND)

        l.append(HTML_POSTAMBLE)

        return ''.join(l)

    def dep_list(self, station):
        l = [HTML_PREAMBLE, TITLE('Abfahrtsstatistik {}'.format(station.name))]

        l.append(H1('Abfahrtsstatistik {}'.format(station.name)))

        dates = self.db.aggregated_departure_dates(station)
        l.append('<p>Statistik über {} Verkehrstage von {} bis {}</p>'.format(dates.count, dates.first, dates.last))

        jumptimes = [time(2,0), time(4,0), time(6,0), time(8, 0), time(10, 0), time(12, 0),
                     time(14, 0), time(16, 0), time(18, 0), time(20, 0), time(22, 0)]

        l.append('<table>')
        l.append('<tr>')
        l.append('<th>Zug<th>Ziel<th>(n)<th>Plan<th>50%')

        last_time = time(23, 59)
        for d in self.db.aggregated_departures(station):
            jt = highest_smaller(d.time, jumptimes)
            if jt is not None and jt > last_time:
                l.append('<tr id="time_{:02}_{:02}">'.format(jt.hour, jt.minute))
                l.append('<th colspan=5>')
                l.append('{:02}:{:02} Uhr'.format(jt.hour, jt.minute))

            last_time = d.time

            if d.count < 0.5*dates.count:
                l.append('<tr class=lowdata>')
            else:
                l.append('<tr>')

            l.append('<td>')
            l.append(html.escape('{}'.format(d.train_name)))
            l.append('<td>')
            l.append(html.escape('{}'.format(d.destination)))
            l.append('<td>')
            l.append('{}'.format(d.count))
            l.append('<td>')
            l.append(html.escape('{:02}:{:02}'.format(d.time.hour, d.time.minute)))
            l.append('<td>')
            l.append(DELAY(d.delay_median))

        l.append('</table>')

        l.append(LEGEND)

        l.append(HTML_POSTAMBLE)

        return ''.join(l)