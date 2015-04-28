print "imports",

import os, inspect, sys, time
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import config, metadata, queryhelper
c = config.Config()

sys.path.append(c.get('tmpo','folder'))
import tmpo

# Get database settings
server = c.get('sql','server')
user = c.get('sql','user')
password = c.get('sql','password')
database = c.get('sql','database')
port = c.get('sql','port')

#set up database connection, fetch metadata, set up data connection
print ", server connection",
QH = queryhelper.QueryHelper(server,user,password,database,port)
print ", metadata",
fluksos = metadata.get_Fluksos()
print ", tmpo data\n"
tmpos = tmpo.Session()

#add sensors and sync
for f in fluksos:
    for s in f.sensors:
        tmpos.add(s.sensor_id,s.token)
tmpos.sync()


hours = 24 #interval in which to search for new data
head = pd.Timestamp(time.time() - 60*60*hours, unit='s')

for f in fluksos:
    print f.flukso_id,len(f.sensors),'sensors',
    f.filter_empty_sensors(tmpos)
    print len(f.sensors),'after filter'
    if len(f.sensors) == 0: continue
        
    #Decide wether to initialize a new table or update existing
    if not QH.flukso_table_exists(f) or QH.flukso_has_new_sensors(f):
        print 'Init',
        QH.init_table(f)
        print 'data fetch'
        df = f.fetch_tmpo(tmpos)
        print df.size,'datapoints'
        QH.set_values(f,df)
    else:
        print 'Update'
        f.filter_empty_sensors(tmpos,head=head)
        print len(f.sensors),'sensors after filter, data fetch'
        df = f.fetch_tmpo(tmpos,head=head)
        if df is None:
            print "no data"
            continue
        print df.size,'datapoints, pushing...'
        QH.upsert_values(f,df)
    print "commit\n"
    QH.commit()