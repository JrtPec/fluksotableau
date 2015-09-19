import pandas as pd
import math

class DataLayer(object):
    """
        Singleton object to contain all datasource connections
    """

    def __init__(self,tmpos):
        """
            Init all connections

            Parameters
            ----------
            tmpos: TMPO session
        """

        self.tmpos = tmpos

    def _start_date(self, sid):
        """
            Queries tempo for the date of the first blok for this sensor

            Parameters
            ----------
            sid: String
                sensor id

            Returns
            -------
            epoch
        """

        return int(self.tmpos.list(sid)[0][0][5])

    def _2epochs(self, time):
        """
            Converts pandas timestamp to epoch

            Parameters
            ----------
            time: timestamp

            Returns
            -------
            epoch
        """

        if isinstance(time, pd.tslib.Timestamp):
            return int(math.floor(time.value / 1e9))
        elif isinstance(time, int):
            return time
        else:
            raise NotImplementedError("Time format not supported. " +
                                      "Use epochs or a Pandas timestamp.")

    def tmpo_dataframe(self,sensor_ids,head=0, tail=2147483647,localize=True,timezone='Europe/Brussels'):
        """
            Fetch dataframe from tmpo

            Parameters
            ----------
            sensor_ids: array of strings, ids of sensors to fetch
            head: Pandas Timestamp, start of frame
            localize: bool, whether or not to change the timestamps to another zone
            timezone: String

            Returns
            -------
            Pandas DataFrame without timezone info
        """

        df = self.tmpos.dataframe(sensor_ids,head=head,tail=tail)
        if df.dropna().empty: return None
        df = self.diff_interp(df)
        if df.dropna().empty: return None
        if localize is True:
            df = self.localize(df,timezone)
        else:
            df.index.tz = None
        return df.dropna()

    def tmpo_series(self,sensor_id,head=0, tail=2147483647, localize=True,timezone='Europe/Brussels'):
        """
            Fetch series from tmpo

            Parameters
            ----------
            sensor_id: string
            head: Pandas Timestamp, start of frame
            localize: bool, whether or not to change the timestamps to another zone
            timezone: String

            Returns
            -------
            Pandas series without timezone info
        """

        start_date = self._start_date(sensor_id)

        if self._2epochs(head) < start_date and self._2epochs(tail) < start_date:
            return None
        elif self._2epochs(head) < start_date:
            head = start_date

        ts = self.tmpos.series(sensor_id,head=head, tail=tail)
        if ts.dropna().empty: return None
        ts = self.diff_interp(ts)
        if ts.dropna().empty: return None
        if localize is True:
            ts = self.localize(ts,timezone)
        else:
            ts.index.tz = None
        return ts.dropna()

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

    def localize(self, ts, timezone):
        """
            Localize a DataFrame or Series to another timezone
            and delete timezone info

            Parameters
            ----------
            ts: Pandas Series or DataFrame
            timezone: String

            Returns
            -------
            same type as ts
        """

        temp = ts.tz_convert(timezone)
        temp.index = pd.DatetimeIndex([i.replace(tzinfo=None) for i in temp.index])
        return temp

    def tmpo_has_data(self, sensor_id, head=0):
        """
            Check if sensor has valid data in tmpo

            Parameters
            ----------
            sensor_id: String
            head: Pandas Timestamp

            Returns
            -------
            bool
        """

        if self.tmpos.series(sensor_id,head=head).dropna().empty:
            return False
        else:
            return True
