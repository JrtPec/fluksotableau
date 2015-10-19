import os, inspect, sys, time
import pandas as pd
from dateutil import rrule
import datetime as dt
import filecmp

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import config, datalayer
from fluksotableau.library import metadata2 as md
c = config.Config()

path_to_data = c.get('data', 'csvfolder2')
if not os.path.exists(path_to_data):
    raise IOError("Provide your path to the data in the config file")

sys.path.append(c.get('tmpo','folder'))
import tmpo

metadata = md.Metadata()
fluksos = metadata.fluksos

try:
    tmpos = tmpo.Session(path = c.get('tmpo','path'))
except:
    tmpos = tmpo.Session()
    
dl = datalayer.DataLayer(tmpos)

def _weekset(start, end):
    """
        Takes a start and end date and returns a set containing all dates between start and end with a week in between

        Parameters
        ----------
        start: datetime object
        end: datetime object

        Returns
        -------
        set of datetime objects
    """
    res = []
    for dt in rrule.rrule(rrule.WEEKLY, dtstart=start, until=end):
        res.append(dt)
    return sorted(set(res))

weekset = _weekset(start=dt.date(year=2015,month=4,day=1), end=dt.date.today())
weekindex = pd.DatetimeIndex(weekset)

for i, f in enumerate(fluksos):
    print "Flukso {} of {}".format(i+1, len(fluksos))
    for j, s in enumerate(f.sensors):
        print "Sensor {} of {}".format(j+1, len(f.sensors))
        for week in weekindex:
            print "week {}, fetching data".format(week),
            ts = dl.tmpo_series(s.sensor_id, head=week, tail= week + pd.Timedelta(days=7))
            if ts is None:
                print "No data"
                continue
            print "success"
            ts = pd.concat([ts], axis=1)
            ts.columns = ['consumption']

            ts = ts.resample('15min', how='mean')
            ts['meterID'] = s.sensor_id

            for group in ts.groupby(ts.index.day):
                filename = "15min.{}.{}.csv".format(s.sensor_id,group[1].first_valid_index().date())

                #save file locally
                group[1].to_csv('temp.csv', header=False)

                if os.path.isfile(os.path.join(path_to_data,filename)) and (filecmp.cmp('temp.csv', os.path.join(path_to_data, filename))):
                    print "file already exists, not saving",
                    continue
                else:
                    print "saving new file",
                group[1].to_csv(os.path.join(path_to_data, filename), header=False)

                print ".",
            print "week done"