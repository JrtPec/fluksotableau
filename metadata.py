import config
import gspread
from flukso import Flukso
from fluksosensor import Sensor

c = config.Config()
sensoramount = 6

def get_Fluksos():
    """
        Download the google spreadsheet with metadata,
        parse the contents into flukso and sensor objects.

        Returns
        -------
        array of Flukso objects
    """
    #fetch Google credentials
    guser = c.get('metadata','user')
    gpassword = c.get('metadata','password')
    gfile = c.get('metadata','file')

    #login to Google and fetch spreadsheet
    gc = gspread.login(guser, gpassword)
    sheet = gc.open(gfile).sheet1
    cellvalues = sheet.get_all_values()

    #Determine column numbers
    flukso_col = sheet.find('flukso_id').col
    sensorcols=[]
    for i in range(1,sensoramount+1):
        sensorcols.append(sheet.find("sensor "+ str(i)).col)
    address_col = sheet.find('address').col

    flukso_ids = sheet.col_values(flukso_col)[1:]
    Fluksos = []
    for row, flukso_id in enumerate(flukso_ids):
        #create a new flukso object for each row
        newFlukso = Flukso(flukso_id)
        newFlukso.set_address(cellvalues[row+1][address_col-1])
        
        for col in sensorcols:
            #parse sensors and create sensor objects
            sensordic = get_sensor(cellvalues = cellvalues,
                int_row = row+2, #+2 to offset header row and start from 0
                int_col = col)
            if sensordic is None: continue
            newSensor = Sensor(sensor_id = sensordic['Sensor'],
                               token = sensordic['Token'],
                               sensortype = sensordic['Type'],
                               function = sensordic['Function'])
            newFlukso.add_sensor(newSensor)
        
        Fluksos.append(newFlukso)

    return Fluksos

def get_sensor(cellvalues, int_row, int_col):
    """
    Return dictionary with all sensor specifications for sensor y in row x.
        
    Parameters
    ----------
        
    int_row: row number (integer)
    int_col: column number (integer)
        
    """
        
    cont = cellvalues[int_row-1][int_col-1]
    keys = ['Sensor', 'Token', 'Type', 'Function']

    try:
        res = dict(zip(keys, cont.split()))
    except:
        # there is no (complete) sensor data
        res = None
        
    if res == {}:
        res = None
        
    return res