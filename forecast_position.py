import os
import sys
from datetime import datetime, timedelta

import numpy as np
from scipy import interpolate
import netCDF4 as ncdf
import pyproj

import geo_tools


def simple_forecast(target_time, drift_track, full_forecast=False):
    """
    Forecasts the position of the buoy at the given target time using
    a simple extrapolation based on previous drift speed and rotation.
    TO ADD: filtering is done to prevent wild forecasts due to inaccurate GPS coordinates.
    :param target_time: Time in UTC for the forecasted position
    :param drift_track: Drift track to use for extrapolation, array of (datetime, lat, lon) tuples.
    :param full_forecast: True; Return the position at every forecast timestep
                          False; Return only the target_time position
    :return: (latitude, longitude) forecast
    """

    # Initialize the record of motion ([speed, angular_vel])
    motion_record = np.zeros((len(drift_track)-2, 2))

    for i in range(1, len(drift_track)-1):
        pos_next = drift_track[i+1]
        pos_curr = drift_track[i]
        pos_last = drift_track[i-1]

        # dt[i-1] = pos_next[1:]  # ONLY FOR PLOTTING

        # Calculate d_pos/dt, which requires 2 positions
        distance = geo_tools.calc_distance(pos_curr[1:], pos_next[1:])
        time_delta = pos_next[0] - pos_curr[0]
        speed = distance / time_delta.total_seconds()  # in km/s

        # Calculate d_theta/dt, which requires 2 vectors and 3 positions
        bearing_curr = geo_tools.calc_bearing(pos_curr[1:], pos_next[1:])
        bearing_last = geo_tools.calc_bearing(pos_last[1:], pos_curr[1:])
        time_delta = pos_next[0] - pos_last[0]
        angular_velocity = (geo_tools.bearing_diff(bearing_curr, bearing_last)
                            / time_delta.total_seconds())  # in degrees/s

        # Record the speed and direction data
        motion_record[i-1] = [speed, angular_velocity]

    initial_time = drift_track[-1][0]
    lead_time = target_time - initial_time

    # We will forecast the position iteratively
    #   (this makes it easier to ignore the difference between arc length and a straight line)
    step_length = timedelta(minutes=15).total_seconds()
    # This rounding means the forecast lead time is only accurate to the step size
    num_steps = int(lead_time.total_seconds() / step_length)

    forecast_drift = [(0,0,0) for _ in range(int(num_steps/4))]

    # Determine the distance and direction traveled for each step
    d_per_step = np.average(motion_record[:, 0]) * step_length
    w_per_step = np.average(motion_record[:, 1]) * step_length

    # Initialize the forecast position to the last known location
    forecast_position = [drift_track[-1][1], drift_track[-1][2]]
    # Initialize the bearing to the last known bearing
    # bearing_curr will hold the last position data from the for loop
    forecast_bearing = bearing_curr

    # Iteratively calculate the final forecast
    forecast_time = initial_time
    for i in range(num_steps):
        # Add the average angular velocity to the current trajectory (bearing)
        forecast_bearing += w_per_step
        forecast_position = geo_tools.update_position(forecast_position[0], forecast_position[1],
                                                      forecast_bearing, d_per_step)
        forecast_time += timedelta(seconds=step_length)
        if i % 4 == 3:
            forecast_drift[int(i/4)] = (forecast_time, forecast_position[0], forecast_position[1])

    if full_forecast:
        return forecast_drift
    else:
        return forecast_drift[-1]


def advanced_forecast(buoy_position, target_time, topaz_start, topaz_end, full_forecast=False):

    # To track the time since last topaz download
    #    return true if the data was refreshed, false otherwise
    topaz_update = False
    # Reconstruct the topaz filename from the stored start and end time
    topaz_filename = '{}/topaz/{}_{}_velocityfield.nc'.format(sys.path[0],
                                                               datetime.strftime(topaz_start, "%Y-%m-%d"), 
                                                               datetime.strftime(topaz_end, "%Y-%m-%d"))

    # Returns True if the needed time window falls entirely within the existing topaz data range
    if not check_existing_topaz(topaz_start, topaz_end, buoy_position[0], target_time):
        # Delete the existing data if it exists
        try:
            os.remove(topaz_filename)
        except FileNotFoundError:
            pass
        topaz_filename, topaz_start, topaz_end = fetch_topaz_forecast(buoy_position, target_time, "{}/topaz".format(sys.path[0]))
        topaz_update = True

    # Load the variables from the file
    topaz_vars = load_topaz_vars(topaz_filename)

    # Run the actual forecast
    forecast_drift = _advanced_forecast(buoy_position, target_time, topaz_vars, full_forecast=full_forecast)

    # Return the start and end time of the topaz file that was (maybe) downloaded, and also the forecast result
    return topaz_update, topaz_start, topaz_end, forecast_drift


def _advanced_forecast(buoy_position, target_time, topaz_vars, full_forecast=False):
    """
    Forecasts the position of the buoy at the given target time using
    TOPAZ Sea ice velocity field forecasts.
    :param buoy_id: The name of the buoy to forecast
    :param target_time: Time in UTC for the forecasted position
    :param topaz_vars: Data from the topaz forecast as [t, lats, lons, u, v]
        These variables are returned from load_topaz_vars()
    :param buoy_position: (datetime, lat, lon) of the buoy position
    :return: (latitude, longitude) forecast
    """

    t, lats, lons, u, v = topaz_vars
    # Topaz time axis is in units of hours since 01/01/1950
    time_units_origin = datetime(1950, 1, 1)

    buoy_time = buoy_position[0]
    buoy_lat = buoy_position[1]
    buoy_lon = buoy_position[2]

    # Find the hours since 1950 for the buoys latest position
    #   Round to the nearest whole hour
    time_diff = buoy_time - time_units_origin
    buoy_time_topazfmt = np.around(time_diff.total_seconds() / 3600)

    lead_time = target_time - buoy_time
    lead_time_hours = int(np.around(lead_time.total_seconds() / 3600))
    # remainder_time = lead_time - timedelta(hours=lead_time_hours)

    # Find the index for the buoy time in the topaz array
    t0 = (np.abs(t - buoy_time_topazfmt)).argmin()

    # Find the index of the grid cell with the smallest difference from the buoy lat/lon
    ll_idx = np.unravel_index((np.abs(lons - buoy_lon) + np.abs(lats - buoy_lat)).argmin(),
                              np.shape(lons))

    # Trim the u/v grids to a local window around the buoy location
    # Local grid size (in each direction from center)
    lgs = 6
    
    # print(lats)
    # print(lons)

    # trim u and v
    u = u[t0:t0 + lead_time_hours + 4, ll_idx[0] - lgs:ll_idx[0] + lgs, ll_idx[1] - lgs:ll_idx[1] + lgs]
    v = v[t0:t0 + lead_time_hours + 4, ll_idx[0] - lgs:ll_idx[0] + lgs, ll_idx[1] - lgs:ll_idx[1] + lgs]
    lats = lats[ll_idx[0] - lgs:ll_idx[0] + lgs, ll_idx[1] - lgs:ll_idx[1] + lgs]
    lons = lons[ll_idx[0] - lgs:ll_idx[0] + lgs, ll_idx[1] - lgs:ll_idx[1] + lgs]

    # Reproject the lat/lon grid to one with units of meters
    # Create the projections for wgs84 (4326) and polar stereo (3413)
    proj_ll = pyproj.Proj('epsg:4326')
    proj_ps = pyproj.Proj('epsg:3413')

    #  x/y grid
    grid_x = np.zeros(np.shape(lons))
    grid_y = np.zeros(np.shape(lats))
    grid_t = np.linspace(0, lead_time_hours+3, lead_time_hours+4)     # 1 hourly temporal grid

    # For each grid cell find x/y values from lon/lat
    for i in range(np.shape(grid_y)[0]):
        for j in range(np.shape(grid_y)[1]):
            grid_x[i, j],  grid_y[i, j] = pyproj.transform(proj_ll, proj_ps, lons[i, j], lats[i, j], always_xy=True)

    # Trim and round the reprojected units to create a regular grid
    grid_x = np.around(np.average(grid_x, axis=0), 0)
    grid_y = np.around(np.average(grid_y, axis=1), 0)

    # Project the buoy coordinates onto the xy-grid
    x, y = pyproj.transform(proj_ll, proj_ps, buoy_lon, buoy_lat, always_xy=True)

    # U and V are in units of m/s
    # Convert to m/hr
    scale_factor = 3600
    u = np.multiply(u, scale_factor)
    v = np.multiply(v, scale_factor)

    # Run the forecast using a rk4 solver over the velocity fields
    forecast_drift_xy = rk4(u, v, 0, x, y, .5, lead_time_hours, grid_x, grid_y, grid_t, full_forecast=full_forecast)

    # Project the solution back to lon/lat
    # len(forecast_drift)==1 if full_forecast==False
    forecast_drift = []
    for tn, x, y in forecast_drift_xy:
        new_lon, new_lat = pyproj.transform(proj_ps, proj_ll, x, y, always_xy=True)
        forecast_drift.append((buoy_time+timedelta(hours=tn), new_lat, new_lon))

    return forecast_drift


def rk4(u, v, t0, x0, y0, step_size, final_step, grid_x, grid_y, grid_t, full_forecast=False):
    """
    Runge-Kutta 4th order solver to determine final position from known velocity fields
    :param u: Velocity field grid (in x direction)
    :param v: Velocity field grid (in y direction)
    :param t0: Index of initial time
    :param x0: Initial x position (in raw meters, not an index)
    :param y0: Initial y position (in raw meters, not an index)
    :param step_size: Step size to take for each iteration (units of t0)
    :param final_step: Time where solver stops
    :param grid_x: 2d grid with x coordinates
    :param grid_y: 2d grid with y coordinates
    :param grid_t: 1d grid with time coordinates
    :return: (latitude, longitude) forecast
    """
    # Initialize variables with initial time/position
    h = step_size
    x, y = x0, y0
    tn = t0

    if full_forecast:
        forecast_drift = []

    # Interpolate u and v values over the x/y/t grid so that we can
    #   determine velocities at fractional grid cells (x and y are true meters, not indices)
    u_interp = interpolate.RegularGridInterpolator((grid_t, grid_x, grid_y), u, bounds_error=True)
    v_interp = interpolate.RegularGridInterpolator((grid_t, grid_x, grid_y), v, bounds_error=True)

    # Find final position iteratively based on step size
    while tn <= final_step:

        # Evaluate the function at f(tn, yn)
        k1 = [u_interp([tn, x, y])[0],
              v_interp([tn, x, y])[0]]
        k2 = [u_interp([tn + (h/2), x + ((h*k1[0])/2), y + ((h*k1[0])/2)])[0],
              v_interp([tn + (h/2), x + ((h*k1[1])/2), y + ((h*k1[1])/2)])[0]]
        k3 = [u_interp([tn + (h/2), x + ((h*k2[0])/2), y + ((h*k2[0])/2)])[0],
              v_interp([tn + (h/2), x + ((h*k2[1])/2), y + ((h*k2[1])/2)])[0]]
        k4 = [u_interp([tn + h, x + h*k3[0], y + h*k3[0]])[0],
              v_interp([tn + h, x + h*k3[1], y + h*k3[1]])[0]]

        # Update for the next iteration
        tn += h
        x = x + (1/6)*h*(k1[0] + 2*k2[0] + 2*k3[0] + k4[0])
        y = y + (1/6)*h*(k1[1] + 2*k2[1] + 2*k3[1] + k4[1])

        if full_forecast:
            forecast_drift.append((tn, x, y))

    if full_forecast:
        return forecast_drift
    else:
        forecast_drift = [(tn, x, y)]
        return forecast_drift


def load_topaz_vars(topaz_filename):
    # The advanced forecast needs to download sea ice velocity field forecasts
    topaz = ncdf.Dataset(topaz_filename, mode='r')

    t = topaz.variables['time'][:]
    lats = topaz.variables['latitude'][:]
    lons = topaz.variables['longitude'][:]
    u = topaz.variables['uice'][:, :, :]
    v = topaz.variables['vice'][:, :, :]
    u, v = np.nan_to_num(u), np.nan_to_num(v)

    return t, lats, lons, u, v


def fetch_topaz_forecast(buoy_position, target_time, output_dir):
    """
    :param buoy_position: (datetime, lat, lon) of the buoy position
    :param target_time: Time in UTC for the forecasted position
    """

    # Determine the range of data we need based on target forecast time
    # End date is the target time:
    #       Add a small buffer of 4 hours
    date_end = target_time + timedelta(hours=4)
    # Start time is the buoys last know position:
    date_start = buoy_position[0]

    forecast_hours = (date_end - date_start).total_seconds() / 3600

    # Calculates an appoximate region of interest based on the length of forecast and 1km/h drift
    lat_min, lat_max, lon_min, lon_max = calc_latlon_window(buoy_position[1], buoy_position[2], forecast_hours)

    # If no existing file was found, download a new one.
    # Select the date range as the needed window, plus a buffer.
    #   This makes it more likely that future forecasts will be able to reuse this data file
    date_min = date_start - timedelta(hours=4)
    # Don't want to add too much time here, because the forecast skill drops off with increased
    #   lead times. I.e. we want to be redownloading this file every day or two.
    date_max = date_end + timedelta(hours=25)
    output_filename = '{}_{}_velocityfield.nc'.format(datetime.strftime(date_min, "%Y-%m-%d"), 
                                                      datetime.strftime(date_max, "%Y-%m-%d"))
    username = 'nwright'
    password = '1qaz2wsx#EDC$RFV'

    cmd = ("python3 -m motuclient --motu http://nrt.cmems-du.eu/motu-web/Motu "
           "--service-id ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a-TDS "
           "--product-id dataset-topaz4-arc-1hr-myoceanv2-be "
           "--longitude-min {} --longitude-max {} --latitude-min {} --latitude-max {} "
           "--date-min '{}' --date-max '{}' "
           "--variable latitude --variable longitude --variable uice --variable vice "
           "--out-dir {} --out-name {} "
           "--user '{}' --pwd '{}'").format(lon_min, lon_max, lat_min, lat_max,
                                            date_min, date_max, output_dir,
                                            output_filename, username, password)
    # print(cmd)
    os.system(cmd)
    return os.path.join(output_dir, output_filename), date_min, date_max


def check_existing_topaz(topaz_start, topaz_end, date_start, date_end):
    """
    Check if the existing forecast file matchs the time window needs
    :param topaz_filename: Existing Topaz filename; convention is "YYYY-MM-DD_YYYY-MM-DD_velocityfield.nc"
    :param date_start: datetime; beginning of forecast window
    :param date_end: datetime; end of forecast window
    return true if existing topaz is usable, false if a new one must be acquired
    """

    # If our needed window falls within the range of this file,
    #   we do not need to download a new set.
    if topaz_start <= date_start and topaz_end >= date_end:
        return True
    else:
        return False


def calc_latlon_window(lat, lon, forecast_hours):

    # Assume maximum average drift of 1km/hr
    max_distance_km = float(forecast_hours * 4)
    max_distance_deglat = max_distance_km / 111.0     # 1 degree lat is about 111km at 70N
    max_distance_deglon = max_distance_km / 36.0      # 1 degree lon is about 36km at 70.7N

    lat_min = round(lat - max_distance_deglat, 3)
    lat_max = round(lat + max_distance_deglat, 3)
    lon_min = round(lon - max_distance_deglat, 3)
    lon_max = round(lon + max_distance_deglat, 3)

    # Lat can't go below the approx coast position
    if lat_min < 70:
        lat_min = 70

    print(lat, lon, lat_min, lat_max, lon_min, lon_max)

    return lat_min, lat_max, lon_min, lon_max