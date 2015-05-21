class Sensor(object):
    """
        Class to contain a fluksosensor,
        its metadata, and methods to fetch its data
    """
    def __init__(self,sensor_id,token,sensortype,function=None,parent=None):
      """
        Init Sensor

        Parameters
        ----------
        sensor_id: string
        token: string
        sensortype: string
        function: string
        parent: (default None) Flukso
      """
      self.parent = parent
      self.sensor_id = sensor_id
      self.token = token
      self.sensortype = sensortype
      self.function = function

    def set_parent(self,flukso):
      """
        Set self.parent to a fluksoobject

        Parameters
        ----------
        flukso: Flukso
      """
      self.parent = flukso