import pandas as pd

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

    def filter_empty_sensors(self,dl,head=0):
        """
            Filter self.sensors to only contain sensors that have data.
            Default looks for all data, but a head from which to search can be set

            Parameters
            ----------
            dl: datalayer
            head: Pandas Timestamp (default 0)
        """
        res = []
        for s in self.sensors:
            if dl.tmpo_has_data(s.sensor_id,head):
                res.append(s)

        self.sensors = res

    def fetch_tmpo(self,datalayer,head=0,localize=True,timezone='Europe/Brussels'):
        """
            Fetch data for this fluksometer

            Parameters
            ----------
            datalayer: datalayer object
            head: Pandas Timestamp
            localize: bool
            timezone: String

            Returns
            -------
            Pandas DataFrame
        """
        if len(self.sensors) == 0: return None

        sensor_ids = [s.sensor_id for s in self.sensors]

        return datalayer.tmpo_dataframe(sensor_ids,head,localize,timezone)