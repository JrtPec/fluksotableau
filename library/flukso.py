class Flukso(object):
    """
        Object to contain a Fluksometer, its metadata and all its sensors
    """
    def __init__(self,flukso_id):
        """
            Init

            Parameters
            ----------
            flukso_id: string
        """
        self.flukso_id = flukso_id
        self.sensors = []
        self.address = None
        
    def add_sensor(self,sensor):
        """
            Add sensor to self.sensors

            Parameters
            ----------
            sensor: Sensor
        """
        sensor.set_parent(self)
        self.sensors.append(sensor)

    def set_address(self,address):
        """
            Set self.address

            Parameters
            ----------
            address: string
        """
        self.address = address

    def filter_empty_sensors(self,tmpos,head=0):
        """
            Filter self.sensors to only contain sensors that have data.
            Default looks for all data, but a head from which to search can be set

            Parameters
            ----------
            tmpos: tmpo session
            head: Pandas Timestamp (default 0)
        """
        res = []
        for s in self.sensors:
            if s.has_data(tmpos,head):
                res.append(s)

        self.sensors = res

    def fetch_tmpo(self,tmpos,head=0):
        """
            Fetch data for this fluksometer

            Parameters
            ----------
            tmpos: tmpo session
            head: Pandas Timestamp

            Returns
            -------
            Pandas DataFrame
        """
        if len(self.sensors) == 0: return None

        sensor_ids = [s.sensor_id for s in self.sensors]

        df = tmpos.dataframe(sensor_ids,head=head)
        df = self.diff_interp(df)
        df.index.tz = None
        return df.dropna()

    def diff_interp(self,ts):
        """
            Resample, interpolate and derive a timeseries or timeframe to
            minute data

            Parameters
            ----------
            ts: Pandas Series or DataFrame

            Returns
            -------
            same type as ts
        """
        newindex = ts.resample('min').index
        ts = ts.reindex(ts.index + newindex)
        ts = ts.interpolate(method='time')
        ts = ts.reindex(newindex)
        ts = ts.diff()
        ts = ts*3600/60
        return ts