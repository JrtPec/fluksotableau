import os, inspect, sys, time
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import config, datalayer
from fluksotableau.library import metadata2 as md
c = config.Config()

path_to_data = c.get('data', 'folder')
if not os.path.exists(path_to_data):
    raise IOError("Provide your path to the data in the config file")

sys.path.append(c.get('tmpo','folder'))
import tmpo

metadata = md.Metadata()
fluksos = metadata.fluksos
tmpos = tmpo.Session()
dl = datalayer.DataLayer(tmpos)

res = []
for f in fluksos:
    for s in f.sensors:
        ts = dl.tmpo_dataframe([s.sensor_id])
        if ts is None: continue
        ts.columns = ['consumption']
        ts['meterID'] = s.sensor_id
        res.append(ts)
s = pd.concat(res)
s.to_csv(os.path.join(path_to_data,"{name:s}.csv".format(name=c.get('csv_filename','1min'))))