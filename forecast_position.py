import numpy as np
from datetime import datetime, timedelta
import geo_tools
# import matplotlib.pyplot as plt
import os
import sys
#import netCDF4 as ncdf
from scipy import interpolate


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