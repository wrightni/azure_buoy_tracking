import requests
import json
from datetime import timedelta, datetime

from utils import xldate_to_datetime, decimaldoy_to_datetime
from credintials import apiKey


def fetch_data_http(source, line_length, num_lines=5):
    """
    Downloads raw text data from the source url
    :param source: url of buoy data file
    :param line_length: Approximate bytes per line of the csv
    :param num_lines: Number of lines to read
    :return string: raw buoy data
    """

    headers = {}
    if num_lines:
        # Create a request header for a partial file.
        # Byte to start reading from, max to prevent negative bytes
        with requests.head(source, verify=False) as r:
            max_byte = int(r.headers['Content-Length'])
            offset = max(max_byte - (line_length * num_lines), 0)
            # Requests a block of bytes at the end the file, corresponding
            # to number of lines needed
            headers = {'Range': 'bytes=%s-%s' % (offset, max_byte)}
    with requests.get(source, headers=headers, verify=False) as req:
        return req.text


def fetch_data_pgapi(source, buoy_id, start_date='03/01/2021'):
    """
    Downloads raw text data from the pacific gyre https api.
    Much of this is hard coded and would need to be adjusted to generalize to other PG buoys.
    :param source: url of buoy data file
    :param buoy_id: IMEI of the buoy
    :param start_date: Date from which to download data record
    :return string: raw buoy data
    """
    payload = {'apiKey': apiKey['osu'],
               'commIDs': buoy_id,
               'fieldList': ['DeviceDateTime', 'Latitude', 'Longitude'],
               'dateFormat': 'yyyy-MM-dd:HH:mm:ss'}
    if start_date:
        payload['startDate'] = start_date

    with requests.get(source, params=payload) as req:
        return req.text


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
            t = xldate_to_datetime(float(data[t_col]))
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
    with open("static/active_buoys.json", 'r') as fhandle:
        buoy = json.load(fhandle).get(buoy_id)
    if buoy is None:
        # This will only happen if a buoy has been removed...
        return []
    buoy_type = buoy["type"]
    data_url = buoy["source"]
    # buoy_format = utils.read_log(utils.FORMATS_FILE, None)[buoy_type]
    with open("static/buoy_staticvars.json", 'r') as fhandle:
        buoy_format = json.load(fhandle)[buoy_type]

    if buoy_format["delim"] == 'None':
        delim = None
    else:
        delim = buoy_format["delim"]

    # Download the raw data from the source
    if buoy_type == 'PacificGyre':
        # Not passing n_pos will default start_date to 03/1/2021
        if n_pos is None:
            data = fetch_data_pgapi(data_url, buoy_id)      # This is returned in reverse order...
        else:
            n_hours = int(n_pos / 144)  # 144 pts per hour @10 min update
            if n_hours < 2:
                n_hours = 2
            start_date = datetime.utcnow() - timedelta(hours=n_hours)
            start_date = start_date.strftime("%m/%d/%Y")
            data = fetch_data_pgapi(data_url, buoy_id, start_date=start_date)
        reverse = True
    # elif data_url.split(':')[0] == 'ftp':
    #    data = fetch_data_ftp(data_url, buoy_format["line_len"], num_lines=n_pos)
    #    reverse = False
    else:  # data_url.split(':')[0] == 'http' or data_url.split(':')[0] == 'https'
        data = fetch_data_http(data_url, buoy_format["line_len"],
                               num_lines=n_pos)
        reverse = False

    # Parse the data into a usable drift track
    return parse_data(data, buoy_format["t_col"], buoy_format["lat_col"], buoy_format["lon_col"],
                      buoy_format["date_fmt"], delim, reverse=reverse, y_col=buoy_format.get("y_col"))