
from __future__ import absolute_import, print_function, division

from datetime import datetime

import pytz


def timezone_from_latlon(latitude, longitude):
    '''
    Guess timezone name from latitude and longitude

    Parameters
    ----------
    latitude : float
        geographical latitude, degrees
    longitude : float
        geographical longitude, degrees

    Returns
    -------
    Timezone name

    Notes
    -----
    antes se llamaba guess_timezone y devolvia timezone, tz_hours y
    tz_str. Ahora solo devuelve timezone. Para calcular los otros dos
    parametros, usa timezone_utcoffset y timezone_as_string, respectivamente
    '''

    from timezonefinder import TimezoneFinder

    try:
        tf = TimezoneFinder()
        timezone = tf.certain_timezone_at(lng=longitude, lat=latitude)
    except Exception:
        timezone = None
    return timezone


def timezone_utcoffset(timezone):
    '''
    Timezone offset

    Parameters
    ----------
    timezone : str
        timezone name

    Returns
    -------
    Timezone offset in hours
    '''

    tz = pytz.timezone(timezone)
    return tz.utcoffset(datetime.now()).total_seconds() / 3600.


def timezone_as_string(timezone):
    '''
    Timezone offset as string

    Parameters
    ----------
    timezone : str
        timezone name

    Returns
    -------
    Timezone offset as string
    '''

    tz = pytz.timezone(timezone)
    return '{}'.format(tz.localize(datetime.now()).strftime('%Z (UTC%z)'))
