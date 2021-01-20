import numpy as np
import math
from sklearn.metrics.pairwise import haversine_distances


def calc_bearing(pos, target):
    """
        Calculates the bearing between the current point and the target point (in (lat,lon) tuple)
    Formula from:
    https://www.igismap.com/formula-to-find-bearing-or-heading-angle-between-two-points-latitude-longitude/
    :param pos:
    :param target:
    :return: bearing, in degrees, clockwise from north
    """

    # Difference in longitude, converted to radians
    dlon = np.deg2rad(target[1] - pos[1])
    # Convert latitude degrees to radians
    plr = np.deg2rad(pos[0])
    tlr = np.deg2rad(target[0])

    x = np.cos(tlr) * np.sin(dlon)

    y = np.cos(plr) * np.sin(tlr) - np.sin(plr) * np.cos(tlr) * np.cos(dlon)

    bearing = np.arctan2(x, y)

    bearing = np.rad2deg(bearing)

    if bearing < 0:
        bearing = 360 - np.abs(bearing)

    return bearing


def bearing_diff(b1, b2):
    r = (b1 - b2) % 360.0
    if r >= 180.0:
        r -= 360.0
    return r


def update_position(lat, lon, bearing, distance):
    # Determines new position from current lat/lon, bearing, and distance travelled
    r = 6371
    lat = np.deg2rad(lat)
    lon = np.deg2rad(lon)
    b = np.deg2rad(bearing)
    new_lat = np.math.asin(np.math.sin(lat) * np.math.cos(distance / r) +
                           np.math.cos(lat) * np.math.sin(distance / r) * np.math.cos(b))
    new_lon = lon + np.math.atan2(np.math.sin(b) * np.math.sin(distance / r) * np.math.cos(lat),
                                  np.math.cos(distance / r) - np.math.sin(lat) * np.math.sin(new_lat))

    return np.rad2deg(new_lat), np.rad2deg(new_lon)


def calc_distance(a, b):

    # Convert positions a and b from degrees to radians
    a_radians = [math.radians(_) for _ in a]
    b_radians = [math.radians(_) for _ in b]

    # Calculate the distance between a and b with the haversine formula
    distance = haversine_distances([a_radians, b_radians])
    distance *= 6371  # multiply by Earth radius to get kilometers

    return distance[0, 1]