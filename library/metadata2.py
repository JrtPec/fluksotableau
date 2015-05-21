import config
import gspread
import json
from oauth2client.client import SignedJwtAssertionCredentials
from flukso import Flukso
from fluksosensor import Sensor

c = config.Config()

class Metadata(object):
    """
        Singleton object that fetches metadata from google and parses flukso objects and sensors
    """
    def __init__(self):
        self.fluksos = []
        self.parse_sheet()
        
    def add_flukso(self,new_flukso):
        """
            Add a new Flukso to the list, but only if it doesn't exist yet
            (probably more efficient with a set)

            Parameters
            ----------
            new_flukso: Flukso
        """
        if self.get_flukso_by_id(new_flukso.flukso_id) is None:
            self.fluksos.append(new_flukso)
            
    def get_flukso_by_id(self, flukso_id):
        """
            Return Flukso object corresponding to id

            Parameters
            ----------
            flukso_id: String

            Returns
            -------
            Flukso or None
        """
        for flukso in self.fluksos:
            if flukso.flukso_id == flukso_id:
                return flukso
        return None

    def parse_sheet(self):
        #fetch Google credentials
        gfile = c.get('metadata','file2')
        gjson = c.get('metadata','json')
        json_key = json.load(open(gjson))
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)

        #login to Google and fetch spreadsheet
        gc = gspread.authorize(credentials)
        gc.login()
        sheet = gc.open(gfile).sheet1
        cellvalues = sheet.get_all_values()

        #determine column numbers
        flukso_col = sheet.find('FluksoID').col
        address_col = sheet.find('BuildingName').col
        sensorid_col = sheet.find('SensorID').col
        sensortoken_col = sheet.find('SensorToken').col
        sensortype_col = sheet.find('SensorType').col

        #get column values
        flukso_ids = sheet.col_values(flukso_col)[1:]
        addresses = sheet.col_values(address_col)[1:]
        sensor_ids = sheet.col_values(sensorid_col)[1:]
        tokens = sheet.col_values(sensortoken_col)[1:]
        types = sheet.col_values(sensortype_col)[1:]

        #create and add Fluksometers
        for flukso_id,address in zip(flukso_ids,addresses):
            new_flukso = Flukso(flukso_id)
            new_flukso.set_address(address)
            self.add_flukso(new_flukso)

        #create and add sensors
        for sensor_id, token, type, flukso_id in zip(sensor_ids, tokens, types, flukso_ids):
            new_sensor = Sensor(sensor_id = sensor_id,
                token = token,
                sensortype = type)
            flukso = self.get_flukso_by_id(flukso_id)
    
            flukso.add_sensor(new_sensor)
