import os, inspect, sys, time
import pandas as pd
from dateutil import rrule
import datetime as dt

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import config, datalayer
from fluksotableau.library import metadata2 as md
c = config.Config()

path_to_data = c.get('data', 'csvfolder')
if not os.path.exists(path_to_data):
    raise IOError("Provide your path to the data in the config file")

sys.path.append(c.get('tmpo','folder'))
import tmpo

metadata = md.Metadata()
fluksos = metadata.fluksos
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

for f in fluksos:
    for s in f.sensors:
        for week in weekindex:
            ts = dl.tmpo_dataframe([s.sensor_id], head=week, tail= week + pd.Timedelta(days=7))
            if ts is None: continue
            ts.columns = ['consumption']
            ts['meterID'] = s.sensor_id

            for group in ts.groupby(ts.index.day):
                group[1].to_csv(os.path.join(path_to_data, "{}.{}.csv".format(s.sensor_id,group[1].first_valid_index().date())))