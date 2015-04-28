import pymssql

class QueryHelper(object):
    """
        Class to set up database connection and do typical operations for fluksotableau
    """
    def __init__(self,server,user,password,database,port):
        """
            Init connection

            Parameters
            ----------
            server: string
            user: string
            password: string
            database: string
            port: int
        """
        self.conn = pymssql.connect(server,user,password,database,port=port)
        self.cursor = self.conn.cursor()
        
    def commit(self):
        """
            Commit changes to database
        """
        self.conn.commit()
        
    def init_table(self,flukso):
        """
            Drop table and create it again

            Parameters
            ----------
            flukso: Flukso
        """
        self.drop_table(self._get_tablename(flukso))
        
        #Make part of the SQL-query, a column for each sensor
        sensors = ""
        for s in flukso.sensors:
            columnname = self._get_columnname(s)
            sensors += '"'+columnname+'"'+' FLOAT,'
            
        query = """
            CREATE TABLE "{name:s}" (
                id DATETIME NOT NULL,
                {sensors:s}
                PRIMARY KEY(id)
            )
            """.format(name=self._get_tablename(flukso),sensors=sensors)
        
        self.cursor.execute(query)
        
    def table_exists(self,name):
        """
            Check if a table exists in the database

            Parameters
            ----------
            name: string

            Returns
            -------
            bool
        """
        query = """
            IF OBJECT_ID('{name:s}', 'U') IS NOT NULL
                SELECT 'Table Exists'
        """.format(name=name)
        
        self.cursor.execute(query)
        try:
            self.cursor.fetchone()
        except:
            return False
        else:
            return True

    def column_exists(self,column_name,table_name):
        """
            Check if a column exists in a certain table

            Parameters
            ----------
            column_name: string
            table_name: string

            Returns
            -------
            bool
        """
        query = """
            IF COL_LENGTH('{table_name:s}','{column_name:s}') IS NOT NULL
                SELECT 'Column Exists'
             """.format(table_name=table_name,column_name=column_name)
        
        self.cursor.execute(query)
        try:
            self.cursor.fetchone()
        except:
            return False
        else:
            return True

    def sensor_column_exists(self,sensor):
        """
            Check if a column for a certain sensor exists in the database

            Parameters
            ----------
            sensor: Sensor

            Returns
            -------
            bool
        """
        column_name = self._get_columnname(sensor)
        table_name = self._get_tablename(sensor.parent)
        return self.column_exists(column_name,table_name)
        
    def flukso_table_exists(self,flukso):
        """
            Check if the table for a certain fluksometer exists in the database

            Parameters
            ----------
            flukso: Flukso

            Returns
            -------
            bool
        """
        name = self._get_tablename(flukso)
        return self.table_exists(name)

    def flukso_has_new_sensors(self,flukso):
        """
            Determine if a fluksometer has a sensor that isn't present in the database yet

            Parameters
            ----------
            flukso: Flukso

            Returns
            -------
            bool
        """
        for sensor in flukso.sensors:
            if not self.sensor_column_exists(sensor):
                return True
        return False
        
    def drop_table(self,name):
        """
            Drops a table from the database

            Parameters
            ----------
            name: string
        """
        query = """
            IF OBJECT_ID('{table:s}', 'U') IS NOT NULL
                DROP TABLE "{table:s}"
                """.format(table=name)
        self.cursor.execute(query)
        
    def set_values(self,flukso,df):
        """
            Insert a dataframe into the database

            Parameters
            ----------
            flukso: Flukso
            df: Pandas DataFrame
        """
        values = [(str(i[0]),) + tuple(i[1]) for i in df.iterrows()]
        query = self._insert_query(flukso)
        print query
        self.cursor.executemany(query,values)

    def update_values(self,flukso,df):
        """
            Update values from a dataframe into the database

            Parameters
            ----------
            flukso: Flukso
            df: Pandas DataFrame
        """
        values = [tuple(i[1],)+(str(i[0])) for i in df.iterrows()]
        query = self._update_query(flukso)
        print query
        self.cursor.executemany(query,values)

    def upsert_values(self,flukso,df):
        """
            Insert new values and update existing entries in the database

            Parameters
            ----------
            flukso: Flukso
            df: Pandas DataFrame
        """
        values = [tuple(i[1],)+(str(i[0]),str(i[0]),)+tuple(i[1]) for i in df.iterrows()]
        query = self._upsert_query(flukso)
        print query
        self.cursor.executemany(query,values)
                  
    def _get_columnname(self,sensor):
        """
            Creates the column name for a given sensor

            Parameters
            ----------
            sensor: Sensor

            Returns
            -------
            string
        """
        return "{name:s}_{type:s}_{function:s}".format(name=sensor.parent.address,
                                                       type=sensor.sensortype,
                                                       function=sensor.function)
    
    def _get_tablename(self,flukso):
        """
            Creates the table name for a given fluksometer

            Parameters
            ----------
            flukso: Flukso

            Returns
            -------
            string
        """
        return flukso.address
    
    def _insert_query(self,flukso):
        """
            Creates the SQL-query to insert data for a given fluksometer

            Parameters
            ----------
            flukso: Flukso

            Returns
            -------
            string
        """
        columnnames = ""
        formatters = ""
        for s in flukso.sensors:
            columnnames += ',"'+self._get_columnname(s)+'"'
            formatters += ',%d'
            
        tablename = self._get_tablename(flukso)
            
        res = 'INSERT INTO "'+tablename+'"(id'+columnnames+') VALUES(%s'+formatters+')'
        return res

    def _update_query(self,flukso):
        """
            Creates the SQL-query to update values for a given fluksometer

            Parameters
            ----------
            flukso: Flukso

            Returns
            -------
            string
        """
        columnnames = ""
        for s in flukso.sensors:
            if columnnames != "":
                columnnames += ','
            columnnames += '"'+self._get_columnname(s)+'"=%d'
            
        tablename = self._get_tablename(flukso)
        res = 'UPDATE "'+tablename+'" SET '+columnnames+' WHERE id=%s'
        return res

    def _upsert_query(self,flukso):
        """
            Creates the SQL-query to insert values if there isn't an index present,
            and updates if there is

            Parameters
            ----------
            flukso: Flukso

            Returns
            -------
            string
        """
        res = self._update_query(flukso)
        res += '; IF @@ROWCOUNT = 0 BEGIN '
        res += self._insert_query(flukso)
        res += '; END'
        
        return res

