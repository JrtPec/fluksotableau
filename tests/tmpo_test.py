print "Loading config and imports"
import sys, os, inspect

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import config, metadata
c = config.Config()

sys.path.append(c.get('tmpo','folder'))
import tmpo

print "Init tmpo"
tmpos = tmpo.Session()

print "Fetching Metadata..."
fluksos = metadata.get_Fluksos()

print "Adding sensors to tmpo"
for f in fluksos:
    for s in f.sensors:
        tmpos.add(s.sensor_id,s.token)

print "Sync tmpo..."
tmpos.sync()

print "RESULTS:"
for f in fluksos:
    print f.flukso_id,f.address
    for s in f.sensors:
        print "\t",s.sensor_id,s.sensortype, s.function
        try:
            ts = tmpos.series(s.sensor_id)
        except IndexError,e:
            print "\t\t",e
            continue
        
        try:
            print "\t\t",ts.index[0]," - ",ts.index[-1]
        except IndexError:
            print "\t\t Empty dataset from TMPO"
    print '\n'