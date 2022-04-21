from os.path import join, dirname
import requests
from urllib import parse, request
from ftplib import FTP
from ftplib import Error
import json
from datetime import timedelta, datetime
import pandas as pd

from buoy_tracking.utils import xldate_to_datetime, decimaldoy_to_datetime
from buoy_tracking.credentials import API_KEY

BUOYS_FILE=join(dirname(dirname(__file__)), "active_buoys.json")
FORMATS_FILE=join(dirname(__file__), "static", "buoy_staticvars.json")


def fetch_data_http(source, line_length, num_lines=5):
    """
    Downloads raw text data from the source url
    :param source: url of buoy data file
    :param line_length: Approximate bytes per line of the csv
    :param num_lines: Number of lines to read
    :return string: raw buoy data
    """
    # In requests.head and requests.get, set verify=False if encountering SSL errors
    headers = {}
    if num_lines:
        # Create a request header for a partial file.
        # Byte to start reading from, max to prevent negative bytes
        with requests.head(source) as r:
            max_byte = int(r.headers['Content-Length'])
            offset = max(max_byte - (line_length * num_lines), 0)
            # Requests a block of bytes at the end the file, corresponding
            # to number of lines needed
            headers = {'Range': 'bytes=%s-%s' % (offset, max_byte)}
    with requests.get(source, headers=headers) as req:
        return req.text


def fetch_data_pgapi(source, buoy_id, start_date='03/01/2021', provider='osu'):
    """
    Downloads raw text data from the pacific gyre https api.
    Much of this is hard coded and would need to be adjusted to generalize to other PG buoys.
    :param source: url of buoy data file
    :param buoy_id: IMEI of the buoy
    :param start_date: Date from which to download data record
    :param provider: API_KEY name associated with this request (must be stored in credentials.py)
    :return string: raw buoy data
    """
    if not provider in API_KEY:
        raise KeyError('API KEY not found')

    payload = {'apiKey': API_KEY[provider],
               'commIDs': buoy_id,
               'fieldList': ['DeviceDateTime', 'Latitude', 'Longitude'],
               'dateFormat': 'yyyy-MM-dd:HH:mm:ss'}
    if start_date:
        payload['startDate'] = start_date

    with requests.get(source, params=payload) as req:
        return req.text


def fetch_data_ftp(source, line_length, num_lines=5):
    """
    Downloads the data file from an FTP source.
    Saves the entire dataset to a local temporary .dat file.
    :param source: url of buoy data file
    :param line_length: Approximate bytes per line of the csv
    :param num_lines: Number of lines to read
    :param local_filename: name of temp file to create
    :return string: raw buoy data
    """
    url = parse.urlsplit(source)
    data = []

    def handle_ftp_content(buff):
        data.append(buff)

    try:
        ftp = FTP(url.hostname)
        ftp.login()
        res = ftp.retrlines("RETR {}".format(url.path[1:]), callback=handle_ftp_content)
        ftp.quit()
    except Error as e:
        raise e
        logger.exception(e)

    # The last entry is an EOF marker, so exclude it and add an extra line.
    # The first entry is skipped by parse_data, so add another extra line
    if num_lines:
        data = data[-(num_lines+2):-1]
    else:
        data = data[:-1]

    # collapse to string
    data = "\n".join(data) + "\n"
    return data


def parse_data(text, t_col, lat_col, lon_col, date_format,
               deliminator=None, reverse=False, y_col=None):
    """
    Parses the downloaded data to retrieve (time,lat,lon) series
    :param text: data from fetch_data()
    :param t_col: Column of the timestamp
    :param lat_col: Column of latitude
    :param lon_col: Column of longitude
    :param date_format: {xl, ddoy} date format used in text
    :param deliminator: within line of text
    :param reverse: whether the list is in reversed order (newest pt at list[0])
    :param y_col: Column of the year. Required only for date_format=ddoy
    :return drift_track, a list of (time, lat, lon) tuples for n last known points
    """
    drift_track = []

    # Convert the raw text to an iterator split by line breaks
    lines = iter(text.splitlines())
    # The first entry is a partial line because the number of bytes requested is approximate
    #   Therefore, we skip it
    next(lines)

    t_previous = None

    # Parse the data to get desired variables
    for line in lines:
        data = line.split(deliminator)
        if len(data) < 3:
            continue
        if date_format == 'xl':
            t = xldate_to_datetime(float(data[t_col]), tz="UTC")
        elif date_format == 'ddoy':
            t = decimaldoy_to_datetime(float(data[t_col]), int(data[y_col]))
        else:
            t = datetime.fromisoformat(data[t_col])
        
        # Round to 5 decimal places, approx. centimeter scale accuracy level
        lat = float(data[lat_col])
        lon = float(data[lon_col])

        # Skip repeated position updates
        if t != t_previous:
            drift_track.append((t, lat, lon))
            t_previous = t

    if reverse is True:
        drift_track = list(reversed(drift_track))

    return drift_track


def fetch_by_buoyid(buoy_id, n_pos=5):
    """
    Fetches the latest positions from a single buoy by name
    :param buoy_id: The ID of the buoy to track
    :param n_pos: The number of positions to read (1 for just the latest)
    :return: list of [(datetime, lat, lon), ...]
    """

    # In order to correctly download and parse the data, we need a bunch of
    # static variables specific to each buoy type. These are stored in a
    # local database.
    # buoy = utils.read_log(utils.BUOYS_FILE, None).get(buoy_id)
    with open(BUOYS_FILE, 'r') as fhandle:
        buoy = json.load(fhandle).get(buoy_id)
    if buoy is None:
        # This will only happen if a buoy has been removed...
        return []
    buoy_type = buoy["type"]
    data_url = buoy["source"]
    provider = buoy["provider"]
    update_freq = buoy["update"] # in minutes, how frequently the buoy updates
    
    if update_freq is None:
        update_freq = 60

    # buoy_format = utils.read_log(utils.FORMATS_FILE, None)[buoy_type]
    with open(FORMATS_FILE, 'r') as fhandle:
        buoy_format = json.load(fhandle)[buoy_type]

    if buoy_format["delim"] == 'None':
        delim = None
    else:
        delim = buoy_format["delim"]

    # Download the raw data from the source
    if buoy_type == 'PacificGyre':
        # Not passing n_pos will default start_date to 03/1/2021
        if n_pos is None:
            data = fetch_data_pgapi(data_url, buoy_id, provider=provider)      # This is returned in reverse order...
        else:
            n_hours = int(n_pos / (60 / update_freq))  # 144 pts per hour @10 min update
            if n_hours < 2:
                n_hours = 2
            start_date = datetime.utcnow() - timedelta(hours=n_hours)
            start_date = start_date.strftime("%m-%d-%Y %H:%M")
            data = fetch_data_pgapi(data_url, buoy_id, start_date=start_date, provider=provider)
        reverse = True
    elif data_url.split(':')[0] == 'ftp':
       data = fetch_data_ftp(data_url, buoy_format["line_len"], num_lines=n_pos)
       reverse = False
    else:  # data_url.split(':')[0] == 'http' or data_url.split(':')[0] == 'https'
        data = fetch_data_http(data_url, buoy_format["line_len"],
                               num_lines=n_pos)
        reverse = False

    # Parse the data into a usable drift track
    return parse_data(data, buoy_format["t_col"], buoy_format["lat_col"], buoy_format["lon_col"],
                      buoy_format["date_fmt"], delim, reverse=reverse, y_col=buoy_format.get("y_col"))


def get_buoy_ids():
    """
    :return: Complete list of active buoy ids
    """
    with open(BUOYS_FILE, 'r') as fhandle:
        buoy_ids = json.load(fhandle).keys()
    return buoy_ids


def get_buoy_df(buoy_id):
    """
    Converts a buoy data into a pandas dataframe, indexed by time
    WARNING: This reads the full buoy file
    :param buoy_id: Buoy ID to query
    :return: Pandas collection containing contents of buoy dat file, indexed by datetime
    """

    # Use n_pos=None to read whole file
    buoy_dat = fetch_by_buoyid(buoy_id, n_pos=None)
    buoy_df = pd.DataFrame(buoy_dat, columns=["timestamp", "Lat", "Lon"])
    buoy_df = buoy_df.set_index("timestamp")
    buoy_df.sort_index(inplace=True)
    buoy_df = buoy_df[~buoy_df.index.duplicated(keep='first')]
    return buoy_df


def get_buoy_before_after(date, buoy_id=None, buoy_dat=None):
    """
     Gets the buoy records immediately before and after the provided date
    :param date: Datetime to query
    :param buoy_id: buoy id; used to fetch buoy_dat if not providet
    :param buoy_dat: Pandas collection containing contents of buoy dat file, indexed by datetime
    :return:
    """

    if buoy_dat is None and buoy_id is not None:
        buoy_dat = get_buoy_df(buoy_id)

    if buoy_dat is not None:
        try:
            buoy_before = buoy_dat.iloc[buoy_dat.index.get_loc(date, method='ffill')]
        except KeyError:
            buoy_before = None

        try:
            buoy_after = buoy_dat.iloc[buoy_dat.index.get_loc(date, method='bfill')]
        except KeyError:
            buoy_after = None

        return [buoy_before, buoy_after]

    return [None, None]


def get_buoy_pos_at_time(time_at_acq, buoy_dat):
    """
    Gets the interpolated position of a buoy at a given time, based on the most the data available immediately before
    and after that time
    :param time_at_acq: Time to get the position for
    :param buoy_dat: Pandas collection containing contents of buoy dat file, indexed by datetime
    :return: Interpolated (lon, lat) of the buoy
    """

    buoy_before, buoy_after = get_buoy_before_after(time_at_acq, buoy_dat=buoy_dat)

    lon1, lat1 = buoy_before[['Lon', 'Lat']]
    lon2, lat2 = buoy_after[['Lon', 'Lat']]
    time1 = buoy_before.name
    time2 = buoy_after.name

    delta = (time2 - time1).total_seconds()
    delta_mid = (time_at_acq - time1).total_seconds()
    weight = delta_mid/delta

    lon = lon1 + (lon2 - lon1) * weight
    lat = lat1 + (lat2 - lat1) * weight

    return (lat, lon)

def poll_active_buoys():
    """
    Polls all buoys in the active list and prints the age since last report
    """
    all_buoys = get_buoy_ids()

    for buoy_id in all_buoys:
        buoy_dat = fetch_by_buoyid(buoy_id, n_pos=1)
        if len(buoy_dat) == 0:
            print("{}: No Data Found".format(buoy_id))
        else:
            t, lat, lon = buoy_dat[-1]
            print("{}: {}".format(buoy_id, datetime.utcnow() - t))