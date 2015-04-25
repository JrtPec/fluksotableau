import fluksoapi

class Sensor(object):
    """
        Class to contain a fluksosensor,
        its metadata, and methods to fetch its data
    """
    def __init__(self,sensor_id,token,sensortype,function):
        self.sensor_id = sensor_id
        self.token = token
        self.sensortype = sensortype
        self.function = function

    def pull_api(self,unit,resolution='minute',interval='day',start=None,end=None):
        '''
            Request Flukso.net for sensor data

            Returns
            -------
            Pandas DataSeries
        '''
        if start is not None and end is not None:
            r = fluksoapi.pull_api(sensor = self.sensor_id,
                                   token = self.token,
                                   unit = unit,
                                   resolution = resolution,
                                   start = start,
                                   end = end)
        else:
            r = fluksoapi.pull_api(sensor = self.sensor_id,
                                   token = self.token,
                                   unit = unit,
                                   resolution = resolution,
                                   interval = interval)

        temp = fluksoapi.parse(r)
        #temp = temp[temp != 'nan'].dropna()
        temp = temp.convert_objects(convert_numeric = True)
        temp.name = self.function

        return temp.dropna()