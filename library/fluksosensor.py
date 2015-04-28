import fluksoapi

class Sensor(object):
    """
        Class to contain a fluksosensor,
        its metadata, and methods to fetch its data
    """
    def __init__(self,sensor_id,token,sensortype,function,parent=None):
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

    def has_data(self,tmpos,head=0):
      """
        Check if this sensor returns valid data from tmpo

        Parameters
        ----------
        tmpos: tmpo session
        head: Pandas Timestamp

        Returns
        -------
        bool
      """
      if tmpos.series(self.sensor_id,head=head).dropna().empty:
        return False
      else:
        return True