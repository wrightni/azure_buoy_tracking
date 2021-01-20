from datetime import timedelta, datetime


def xldate_to_datetime(xldate, tz="EST"):
    temp = datetime(1899, 12, 30)
    delta = timedelta(days=xldate)
    if tz == "EST":
        utc_offset = timedelta(hours=5)
    else:
        utc_offset = timedelta(hours=0)
    return temp+delta+utc_offset


def decimaldoy_to_datetime(decimaldoy, year=2020):
    temp = datetime(year-1, 12, 31)
    delta = timedelta(days=decimaldoy)
    return temp + delta


def format_timedelta(tdelta):
    s = tdelta.total_seconds()
    hours, remainder = divmod(s, 3600)
    minutes, seconds = divmod(remainder, 60)

    return "{:02}h {:02}m {:02}s".format(int(hours), int(minutes), int(seconds))