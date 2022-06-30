
from __future__ import absolute_import, print_function, division

import warnings
from datetime import datetime, timedelta

import numpy as np


def date2num(d):
    if np.iterable(d):
        return np.array([date2num(t) for t in d], dtype=np.float128)
    else:
        return d.toordinal() \
               + (d.hour + (d.minute + d.second / 60.) / 60.) / 24.


def num2date(x):
    if np.iterable(x):
        return np.array([num2date(v) for v in x], dtype='O')
    else:
        seconds_per_day = 24. * 60. * 60.
        i_day, f_day = divmod(x, 1)
        d = datetime.fromordinal(int(i_day))
        i_seconds = int(round(f_day * seconds_per_day))
        return d + timedelta(seconds=i_seconds)


def warningfilter(action, category=RuntimeWarning):
    def warning_deco(func):
        def func_wrapper(*args, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter(action, category)
                return func(*args, **kwargs)
        return func_wrapper
    return warning_deco