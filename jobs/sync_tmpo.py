import os, inspect, sys

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import metadata, config
c = config.Config()

sys.path.append(c.get('tmpo','folder'))
import tmpo

fluksos = metadata.get_Fluksos()
tmpos = tmpo.Session()

#add sensors and sync
for f in fluksos:
    for s in f.sensors:
        tmpos.add(s.sensor_id,s.token)
tmpos.sync()