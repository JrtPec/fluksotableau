import sys, os, inspect

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import metadata2

MD = metadata2.Metadata()

for flukso in MD.fluksos:
    print flukso.flukso_id, flukso.address
    for sensor in flukso.sensors:
        print sensor.sensor_id, sensor.sensortype
    print "\n"