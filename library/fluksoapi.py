import requests
import datetime as dt
import pandas as pd

def pull_api(sensor, token, unit, interval='day', resolution = 'minute', start=None, end=None):
   
    """     
   
    Function for downloading data from fluksometers
    
    Parameters
    ----------
    - inverval: string specifying the interval (day, month, ...)
    - sensor :  sensor name (from the flukso.net sensor tab)
    - token :  sensor token (from the flukso.net sensor tab)
    - resolution: time resolution (e.g. minute, 15min, hour, day, week, month, 
      year, decade, night)
    - unit: unit of measurements (e.g. watt, kwhperyear, lperday)
    - start & end: unix timestamps (optional), if supplied, they overrule interval

    Note
    ----
    The Flukso Server will automatically restrict the data to what's available
    
    
    Returns
    -------
    Resulf of the http request with the raw data.  
    Use the save2csv function to parse and save.
    
    """
    if start is not None and end is not None:
        payload = {'start'      :   start,
                   'end'        :   end,
                   'resolution' :   resolution,
                   'unit'       :   unit}
    else:
        payload = {'interval'   :   interval,
                   'resolution' :   resolution,
                   'unit'       :   unit}
               
    headers = {'Accept'     :   'application/json', 
               'X-Version'  :   '1.0', 
               'X-Token'    :   token}  
               
    url = 'https://api.flukso.net' + '/sensor/' + sensor 
    
    # Send Request
    try:    
        s = requests.Session()
        r = s.get(url, params = payload, headers = headers, verify=False)
    except:
        print "-------> Problem with HTTP request to Flukso <-------"
    
    # check result
    
    if not r.ok:
        print "The flukso api GET request did not succeed."
        print "Some details:"
        print "Request headers:"
        print r.request.headers
        print "Request url:"
        print r.request.url
        
        
    
    return r


def parse(r):
    """
    Parse and return a pandas TimeSeries object
    """
    
    
    # Create TimeSeries   
    try:
        d = {}
        for tup in r.json():
            d[dt.datetime.fromtimestamp(tup[0])] = tup[1]
        
    except:
        print "-------> Problem with Flukso data parsing <-------"
        raise
    
    #pdb.set_trace()
    Ts = pd.TimeSeries(data=d)
    # Convert the index to a pandas DateTimeIndex 
    Ts.index = pd.to_datetime(Ts.index)
    # this line gives an error.  Should be checked, but for now I keep the nan's        
    # Ts = Ts[Ts != 'nan']
    
    return Ts