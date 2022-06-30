
from __future__ import absolute_import, print_function, division

import collections
from functools import reduce

import numpy as np
import pandas as pd
from pandas.tseries.frequencies import to_offset

from termcolor import colored as tc_colored


def is_sequence(obj):
    if isinstance(obj, str):
        return False
    return isinstance(obj, collections.abc.Sequence)


def guess_time_resolution(df_or_series):  # , enable_warnings=True):
    """
    The returned value is to cast the dataframe or series to the correct
    frequency as follows:

        inferred_freq = guess_time_resolution(x)
        if inferred_freq is None:
            raise ...
        x = x.asfreq(inferred_freq)

    The method asfreq set the frequency to the passed value and fills
    the missings according to the input options (by default, fills with nans)
    """

    index = df_or_series.index

    if (inferred_freq := pd.infer_freq(index)) is not None:
        return pd.to_timedelta(to_offset(inferred_freq))

    # for most cases, the following should work. If there are many missings
    # in the series, little can be done...
    step = pd.to_timedelta(np.diff(index.to_numpy()).min())
    reconstructed_index = pd.date_range(index[0], index[-1], freq=step)
    if index.isin(reconstructed_index).all():
        return step

    return None


class TextStyle:

    @staticmethod
    def format(text, style=None):
        attributes = (
            'bold', 'dark', 'underline', 'blink', 'reverse', 'concealed')

        color, on_color, attrs = [None]*3
        if style is None:
            return tc_colored(text, color, on_color, attrs)

        opts = [opt.strip() for opt in style.split()]

        if opt := list(filter(lambda opt: opt.startswith('on_'), opts)):
            on_color = opt[0]

        opts = list(filter(lambda opt: not opt.startswith('on_'), opts))

        if opt := list(filter(lambda opt: opt in attributes, opts)):
            attrs = list(opt)

        if opt := list(filter(lambda opt: opt not in attributes, opts)):
            color = opt.pop(0)

        return tc_colored(text, color, on_color, attrs)

    @staticmethod
    def join(*texts):
        return reduce(lambda a, b: a + b, texts)


styler = TextStyle()


def time_interpolation(data, new_index):
    # time interpolation
    extended_index = data.index.append(new_index).sort_values()
    new_data = data.reindex(extended_index).interpolate(method='time', limit=1)
    # drop duplicated indices
    new_data = new_data.reset_index()
    index_name = new_data.index.name or 'index'
    new_data = new_data.drop_duplicates(subset=index_name, keep='first')
    new_data.index = new_data[index_name]
    new_data.drop(columns=index_name, inplace=True)
    new_data = new_data.reindex(new_index)
    return new_data
