class Flukso(object):
    """
        Object to contain a Fluksometer, its metadata and all its sensors
    """
    def __init__(self,flukso_id):
        self.flukso_id = flukso_id
        self.sensors = []
        self.address = None
        
    def add_sensor(self,sensor):
        self.sensors.append(sensor)

    def set_address(self,address):
        self.address = address