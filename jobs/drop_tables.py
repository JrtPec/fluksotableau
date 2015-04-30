print "imports",

import os, inspect, sys, time
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(os.path.join(script_dir, os.pardir, os.pardir))

from fluksotableau.library import config, metadata, queryhelper
c = config.Config()

# Get database settings
server = c.get('sql','server')
user = c.get('sql','user')
password = c.get('sql','password')
database = c.get('sql','database')
port = c.get('sql','port')

#set up database connection, fetch metadata, set up data connection
print ", server connection",
QH = queryhelper.QueryHelper(server,user,password,database,port)
print ", metadata"
fluksos = metadata.get_Fluksos()

for f in fluksos:
	print "dropping",f.address
	QH.flukso_drop_table(f)
	QH.commit()