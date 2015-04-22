import pymssql

import config
c = config.Config()

server = c.get('sql','server')
user = c.get('sql','user')
password = c.get('sql','password')
database = c.get('sql','database')
port = c.get('sql','port')

conn = pymssql.connect(server,user,password,database,port=port)
cursor = conn.cursor()

cursor.execute("""
IF OBJECT_ID('persons', 'U') IS NOT NULL
    DROP TABLE persons
CREATE TABLE persons (
    id INT NOT NULL,
    name VARCHAR(100),
    salesrep VARCHAR(100),
    PRIMARY KEY(id)
)
""")

cursor.executemany(
    "INSERT INTO persons VALUES (%d, %s, %s)",
    [(1, 'John Smith', 'John Doe'),
     (2, 'Jane Doe', 'Joe Dog'),
     (3, 'Mike T.', 'Sarah H.')])
# you must call commit() to persist your data if you don't set autocommit to True
conn.commit()