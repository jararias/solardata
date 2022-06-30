
from __future__ import absolute_import, print_function, division

import os
import io
import re
import time
import gzip
import copy
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pylab as pl
import pandas as pd

from loguru import logger

from . import config
from . import resources
from .util import num2date, warningfilter


logger.disable(__name__)


@warningfilter('ignore')
def quiet_mean(*args, **kwargs):
    return np.nanmean(*args, **kwargs)


def sites(force=False):
    '''
    Available AERONET' sites
    '''
    return sorted(sites_metadata(None, force))


def sites_metadata(site=None, force=False, include_availability=False):
    '''
    Retrieves metadata information of the AERONET stations

    Parameters:
    -----------
    site: string
      AERONET' site acronym
    force: bool
      True to force the update of the local copy of metadata from the remote
      site
    include_availability: bool
      If True, the number of months with data is computed for each site.
      Since this computation requires some extra time, set it to False to
      save time

    Returns:
    --------
    Dictionary with keys the acronym of each site and values a metadata
    dictionary.
    '''

    MAX_ELAPSED_DAYS_FROM_LAST_UPDATE = 10

    aero_config = config.load()
    localdir = aero_config['localdir']
    server = aero_config['server']
    
    base_fn = 'aeronet_locations_v3.txt'
    local_fn = localdir.joinpath(base_fn)
    remote_fn = base_fn

    if (not local_fn.exists()) or (force is True):
        logger.info(f'fetching file {local_fn}')
        resources.download(server + remote_fn, local_fn, with_progress_bar=False)

    # if last modification is beyond the updating period, update the file
    elapsed_time_days = (os.path.getmtime(local_fn) - time.time()) / (3600.*24.)
    if elapsed_time_days > MAX_ELAPSED_DAYS_FROM_LAST_UPDATE:
        logger.info(f'fetching file {local_fn}')
        resources.download(server + remote_fn, local_fn, with_progress_bar=False)

    if not local_fn.exists():
        logger.warning(f'missing file of AERONET locations {local_fn}')
        return

    # leer local_fn
    with local_fn.open('r') as f:
        lines = [line.strip() for line in f.readlines()]

    sites = {}
    for line in lines[2:]:
        values = line.split(',')
        site_name = values[0]
        sites[site_name] = {
            'longitude': float(values[1]),
            'latitude': float(values[2]),
            'elevation': float(values[3]),
            'data_download_url': (
                server + 'cgi-bin/data_display_aod_v3?site={site_name}'
            ),
            'site_info_url': (
                server + 'new_web/photo_db_v3/{site_name}.html'
            )
        }

    if include_availability:
        avail = availability(force=force)
        for key in sites.keys():
            n_months = sum([sum(v.values()) for v in avail[key].values()])
            sites[key]['n_months'] = n_months

    if site is None:
        return sites

    sites_lower = [aero_site.lower() for aero_site in sorted(sites)]
    if site not in sites:
        if site.lower() in sites_lower:
            k = sites_lower.index(site.lower())
            logger.warning(f'{site} is missing in AERONET but {sorted(sites)[k]} exists')
        return None
    return sites[site]


def availability(site=None, year=None, month=None, force=False,
                 quality_level='20', year_start=1993):
    '''
    Returns a dictionary with the data availability for each AERONET
    site, in monthly blocks

    Parameters:
    -----------
    site: string
      AERONET' site acronym
    year: integer >= 1993
        Year of the availability to return. If not provided, all years
        from year_start are returned.
    month: integer (1...12)
        Month of the availability to return. If not provided, all months
        are returned.
    force: bool
        True to force the update of the local copy of metadata from the remote site
    quality_level: string, '10', '15' or '20'
        AERONET's data quality level
    year_start: integer >= 1993
        Year from which provide availability if year is not provided
    '''

    MAX_ELAPSED_DAYS_FROM_LAST_UPDATE = 10
    assert quality_level in ('10', '15', '20')

    sites = defaultdict(dict)

    years = (year,)
    if year is None:
        years = range(year_start, datetime.now().year + 1)

    aero_config = config.load()
    localdir = aero_config['localdir']
    server = aero_config['server']

    for this_year in years:
        base_fn = f'aeronet_locations_v3_{this_year:d}_lev{quality_level}.txt'
        local_fn = localdir.joinpath(base_fn)
        remote_fn = f'Site_Lists_V3/{base_fn}'

        if (not local_fn.exists()) or (force is True):
            logger.info(f'fetching file {local_fn}')
            resources.download(server + remote_fn, local_fn, with_progress_bar=False)

        elapsed_time_days = (os.path.getmtime(local_fn) - time.time()) / (3600.*24.)
        if elapsed_time_days > MAX_ELAPSED_DAYS_FROM_LAST_UPDATE:
            logger.info(f'fetching file {local_fn}')
            resources.download(server + remote_fn, local_fn, with_progress_bar=False)

        if not local_fn.exists():
            logger.warning(f'missing file of AERONET locations {local_fn}')
            return None

        # read the local_fn for this_year and quality level
        with local_fn.open('r') as f:
            lines = [line.strip() for line in f.readlines()]

        for line in lines[2:]:
            values = line.split(',')
            data_availability = dict(zip(range(1, 13), map(bool, values[4:])))
            sites[values[0]].update({this_year: data_availability})

    if site is None:

        if year is None:

            if month is None:
                return sites

            return {st: {yr: {month: sites[st][yr][month]}
                         for yr in sites[st].keys()}
                    for st in sites.keys()}

        if month is None:
            return {st: {year: sites[st][year]}
                    for st in sites.keys()}

        return {st: {year: {month: sites[st][year][month]}}
                for st in sites.keys()}

    if year is None:

        if month is None:
            return sites[site]

        return {yr: {month: sites[site][yr][month]}
                for yr in sites[site].keys()}

    if month is None:
        return {year: sites[site][year]}

    return {year: {month: sites[site][year][month]}}


# def populate_database(aenet_file, data_type='aod20', overwrite=False,
#                       ym_ini=None, ym_end=None):

#     # if the input filename follows the standard pattern from AERONET:
#     # <first_day>_<last_day>_<site_name>.lev20, take the period extension
#     # from the file name; otherwise, take it from the data in the file.
#     # If there is no data for a month within the period extension, the
#     # routine understand that there are no measurements for that month and,
#     # consequently, it creates an entry in the void-index file associated
#     # to this site and this data type

#     def parse_yearmonth(data_line):
#         try:
#             day = datetime.strptime(data_line.split(',')[0], '%d:%m:%Y')
#         except Exception:
#             return None
#         return day.year, day.month

#     def iterate_yearmonth(ym_ini, ym_end):
#         day = datetime(ym_ini[0], ym_ini[1], 15)
#         while (day.year, day.month) <= ym_end:
#             yield (day.year, day.month)
#             day = (day + timedelta(30)).replace(day=15)

#     aero_config = config.load()
#     localdir = aero_config['localdir']

#     if data_type not in ('aod10', 'aod15', 'aod20'):
#         raise AssertionError(f'data type {data_type} not supported')

#     aenet_file = Path(aenet_file)
#     if not aenet_file.exists():
#         logger.error(f'missing file {aenet_file}')

#     with aenet_file.open('rt') as inp_f:
#         lines = inp_f.readlines()

#     site_name = lines[1].strip()

#     header = [f'AERONET data populated from file {aenet_file.name}']
#     header.append(lines[0].strip())
#     header.append(lines[2].strip())
#     header.append(lines[3].strip())
#     header.append(lines[4].strip())
#     header.append('AERONET_Site,' + lines[6].strip())

#     # daily or all_points file??
#     daily = True
#     noon = datetime.strptime('12:00:00', '%H:%M:%S')
#     for line in lines[7:]:
#         the_time = datetime.strptime(line.split(',')[1], '%H:%M:%S')
#         if the_time != noon:
#             daily = False
#             break

#     # digitize the data set by (year, month) entries in a dictionary
#     data = defaultdict(list)
#     for line in lines[7:]:
#         year_and_month = parse_yearmonth(line)
#         data[year_and_month].append(site_name + ',' + line.strip())

#     # evaluate the data period...

#     # if the data file name include the start and end of the data period,
#     # use them to generate the (year, month)s
#     existing_yearmonth = list(data.keys())  # only the existing months
#     try:
#         day_ini, day_end = aenet_file.name.split('_')[:2]
#         day_ini = datetime.strptime(day_ini, '%Y%m%d')
#         ym_s = (day_ini.year, day_ini.month)
#         day_end = datetime.strptime(day_end, '%Y%m%d')
#         ym_e = (day_end.year, day_end.month)
#     except Exception:
#         ym_s = existing_yearmonth[0]
#         ym_e = existing_yearmonth[-1]

#     range_yearmonth = list(
#         iterate_yearmonth(
#             ym_s if ym_ini is None else ym_ini,
#             ym_e if ym_end is None else ym_end
#         )
#     )

#     time_step = 'daily' if daily is True else 'all_points'
#     logger.info(f'populating {time_step} data in site {site_name} '
#                 f'from file {aenet_file.name}')

#     void_files = []  # list of months with no data

#     for year, month in range_yearmonth:
#         base_fn = resources.aeronet_filename(
#             site_name, year, month, data_type, daily)
#         local_fn = localdir.joinpath(site_name).joinpath(base_fn)
#         relpath = os.path.relpath(local_fn, localdir)

#         if local_fn.exists() and (overwrite is False):
#             logger.debug(
#                 f'existing file <AERONET local database>/{relpath}. Skipping')
#             continue

#         if local_fn.exists():
#             logger.info(f'overwriting file <AERONET local database>/{relpath}')
#         else:
#             logger.debug(f'creating file <AERONET local database>/{relpath}')

#         if not local_fn.parent.exists():
#             local_fn.parent.mkdir(parents=True)

#         if (year, month) in data:
#             data_text = '\n'.join(header + data[(year, month)])
#             with gzip.open(local_fn, 'w') as gz_f:
#                 gz_f.write(data_text.encode('utf-8'))
#         else:
#             void_files.append(local_fn.name)

#     # update index of void files...
#     resources.update_void_index(site_name, data_type, daily, void_files)


def load_data(site, year, month, data_type='AOD20', daily=False,
              inv_product=None, force_download=False, dry_run=False):
    '''
    Returns the requested Version 3 AERONET data and metadata. If they
    are not in the local database, it tries to download them throw the
    Version 3 REST interface. The data is returned in monthly blocks.

    Parameters:
    -----------
    site: string
        AERONET site name. Mandatory.
    year: integer >= 1993
        Year of the data to return. Mandatory.
    month: integer (1...12)
        Month of the data to return. Mandatory.
    data_type: string
        Version 3 AERONET data type. Default: AOD20
        See https://aeronet.gsfc.nasa.gov/print_web_data_help_v3.html and
        https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv.html for
        the AOD/SDA and inversion data types, respectively.
    daily: boolean
        True to return daily values
    inv_product: string
        Inversion product type. Required only for inversion data types.
        See https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv.html
    force_download: boolean
        Download the data and stores it in the local database even if they
        are already in the local database
    dry_run: boolean
        If the data is not in the local database, downloads it, stores it, but
        does not load it and return None

    Returns:
    --------
    A dictionary with various metadata and the data, as a Pandas DataFrame,
    under the dictionary's key 'values'
    '''

    aero_config = config.load()
    base_fn = resources.aeronet_filename(site, year, month, data_type, daily)
    local_fn = aero_config['localdir'].joinpath(site).joinpath(base_fn)

    # IN THIS BLOCK THERE IS COLLISION RISK OF PROCESSES ACCESSING
    # CONCURRENTLY TO THE VOID-INDEX FILE WHEN THE DOWNLOAD IS PERFORMED
    # IN PARALLEL USING MULTIPROCESSING.
    # I AM GOING TO TEST IT WITHOUT IMPLEMENTING ANY LOCKING. IF COLLISION
    # PROBLEMS ARISE, THEN I SHOULD IMPLEMENT THE LOCKING SCHEME IN THE
    # ROUTINES resources.read_void_index and resources.update_void_index

    metadata = {}
    data = pd.DataFrame()

    if not local_fn.exists():

        void_files = resources.read_void_index(site, data_type, daily)

        if local_fn.name in void_files:
            return data, metadata

        else:
            rpath = os.path.relpath(local_fn, aero_config['localdir'])
            logger.info(f'downloading file <AERONET local database>/{rpath}')

            from_datetime = datetime(year, month, 1, 0)
            to_datetime = (from_datetime + timedelta(35)).replace(day=1)
            resources.download_aeronet_v3(
                site=site, from_datetime=from_datetime,
                to_datetime=to_datetime, data_type=data_type,
                inv_product=inv_product, daily=daily)

            if not local_fn.exists():
                return data, metadata

    else:

        if force_download is True:
            rpath = os.path.relpath(local_fn, aero_config['localdir'])
            logger.info(f'downloading file <AERONET local database>/{rpath}')

            from_datetime = datetime(year, month, 1, 0)
            to_datetime = (from_datetime + timedelta(35)).replace(day=1)
            resources.download_aeronet_v3(
                site=site, from_datetime=from_datetime,
                to_datetime=to_datetime, data_type=data_type,
                inv_product=inv_product, daily=daily)

            if not local_fn.exists():
                return data, metadata

    if dry_run is True:
        return data, metadata

    relpath = os.path.relpath(local_fn, aero_config['localdir'])
    logger.info(f'reading file <AERONET local database>/{relpath}')

    init_time = time.time()

    def readline(stream_handler):
        return stream_handler.readline().decode('utf-8').strip()

    def parse_day(day_s, fmt):
        return datetime.strptime(day_s, fmt).toordinal()

    def parse_time(time_s, fmt):
        t = datetime.strptime(time_s, fmt)
        return (t.hour + (t.minute + t.second / 60.) / 60.) / 24.

    with gzip.open(local_fn, 'r') as f:

        skip_n_lines = None
        column_names_line = None

        if data_type in resources.AOD_DATA_TYPES:
            skip_n_lines = 4
            column_names_line = 6

        if data_type in resources.INV_DATA_TYPES:
            skip_n_lines = 5
            column_names_line = 7

        try:
            header = [readline(f) for _ in range(skip_n_lines)]
        except StopIteration:
            logger.warning('missing data in file {local_fn.name}. The file might be empty')
            return data, metadata

        metadata['header'] = header

        m = re.search('PI=(.*); PI Email=(.*)', readline(f))
        pi_name, pi_email = '', ''
        if m is None:
            logger.debug('could not extract the PI information')
        else:
            pi_name, pi_email = m.groups()
        metadata['principal_investigator_name'] = pi_name
        metadata['principal_investigator_email'] = pi_email

        INP_COLUMN_NAMES = readline(f)

        if INP_COLUMN_NAMES == '':
            logger.warning(f'missing data in file {local_fn.name}. The file might be empty')
            return data, metadata

        INP_COLUMN_NAMES = INP_COLUMN_NAMES.split(',')

        if 'AOD' in data_type:
            date_column_name = 'Date(dd:mm:yyyy)'
            time_column_name = 'Time(hh:mm:ss)'

            OUT_COLUMN_NAMES = [
                date_column_name, time_column_name, 'Precipitable_Water(cm)']

            if daily is not True:
                OUT_COLUMN_NAMES.extend([
                    'Solar_Zenith_Angle(Degrees)',
                    'Ozone(Dobson)', 'NO2(Dobson)'])

            for column_name in INP_COLUMN_NAMES:
                regex = re.compile(r'^AOD_\d+nm$')
                if regex.match(column_name):
                    OUT_COLUMN_NAMES.append(column_name)
                regex = re.compile(r'^\d+-\d+_Angstrom_Exponent$')
                if regex.match(column_name):
                    OUT_COLUMN_NAMES.append(column_name)

        else:
            date_column_name = '_UNKNOWN_DATE_COLUMN_NAME_'
            for date_column_name in ('Date_(dd:mm:yyyy)', 'Date(dd:mm:yyyy)'):
                if date_column_name in INP_COLUMN_NAMES:
                    break

            time_column_name = '_UNKNOWN_TIME_COLUMN_NAME_'
            for time_column_name in ('Time_(hh:mm:ss)', 'Time(hh:mm:ss)'):
                if time_column_name in INP_COLUMN_NAMES:
                    break

            # remove unconditionally unnecessary columns
            OUT_COLUMN_NAMES = copy.copy(INP_COLUMN_NAMES)
            for column_name in (
                    'AERONET_Site', 'AERONET_Site_Name', 'Data_Quality_Level',
                    'Inversion_Data_Quality_Level'):
                if column_name in OUT_COLUMN_NAMES:
                    OUT_COLUMN_NAMES.pop(OUT_COLUMN_NAMES.index(column_name))

        try:
            date_column_index = INP_COLUMN_NAMES.index(date_column_name)
        except ValueError:
            raise ValueError(f'missing required column `{date_column_name}`')

        try:
            time_column_index = INP_COLUMN_NAMES.index(time_column_name)
        except ValueError:
            raise ValueError(f'missing required column `{time_column_name}`')

        converters = {
            date_column_index:
                lambda s: parse_day(s.decode('utf-8'), '%d:%m:%Y'),
            time_column_index:
                lambda s: parse_time(s.decode('utf-8'), '%H:%M:%S')
        }

        if 'Last_Processing_Date' in OUT_COLUMN_NAMES:
            index = INP_COLUMN_NAMES.index('Last_Processing_Date')
            converters[index] = \
                lambda s: pl.strpdate2num('%d:%m:%Y')(s.decode('utf-8'))

        if 'Last_Processing_Date(dd:mm:yyyy)' in OUT_COLUMN_NAMES:
            index = INP_COLUMN_NAMES.index('Last_Processing_Date(dd:mm:yyyy)')
            converters[index] = \
                lambda s: pl.strpdate2num('%d:%m:%Y')(s.decode('utf-8'))

        if 'Last_Date_Processed' in OUT_COLUMN_NAMES:
            index = INP_COLUMN_NAMES.index('Last_Date_Processed')
            converters[index] = \
                lambda s: pl.strpdate2num('%d:%m:%Y')(s.decode('utf-8'))

        if 'Last_Processing_Time(hh:mm:ss)' in OUT_COLUMN_NAMES:
            index = INP_COLUMN_NAMES.index('Last_Processing_Time(hh:mm:ss)')
            converters[index] = \
                lambda s: pl.strpdate2num('%H:%M:%S')(s.decode('utf-8'))

        if 'Measurement_Type(solar or lunar)' in INP_COLUMN_NAMES:
            index = INP_COLUMN_NAMES.index('Measurement_Type(solar or lunar)')
            converters[index] = lambda s: 1 if s == 'solar' else 0

        if 'Measurement_Type(solar or lunar)' in OUT_COLUMN_NAMES:
            index = OUT_COLUMN_NAMES.index('Measurement_Type(solar or lunar)')
            OUT_COLUMN_NAMES[index] = 'Measurement_Type(solar=1, lunar=0)'

        # back to the beginning of the file to read lat, lon
        # and elevation but first skip the header lines
        f.seek(0)
        for _ in range(column_names_line):
            readline(f)
        line = readline(f).split(',')

        # read latitude and remove column from the output
        latitude_column_names = ('Site_Latitude(Degrees)', 'Latitude(Degrees)')
        for latitude_column_name in latitude_column_names:
            if latitude_column_name in INP_COLUMN_NAMES:
                index = INP_COLUMN_NAMES.index(latitude_column_name)
                metadata['latitude'] = float(line[index])
                if latitude_column_name in OUT_COLUMN_NAMES:
                    index = OUT_COLUMN_NAMES.index(latitude_column_name)
                    OUT_COLUMN_NAMES.pop(index)
                break
        else:
            raise ValueError('missing required latitude column')

        # read longitude and remove column from the output
        longitude_column_names = ('Site_Longitude(Degrees)', 'Longitude(Degrees)')
        for longitude_column_name in longitude_column_names:
            if longitude_column_name in INP_COLUMN_NAMES:
                index = INP_COLUMN_NAMES.index(longitude_column_name)
                metadata['longitude'] = float(line[index])
                if longitude_column_name in OUT_COLUMN_NAMES:
                    index = OUT_COLUMN_NAMES.index(longitude_column_name)
                    OUT_COLUMN_NAMES.pop(index)
                break
        else:
            raise ValueError('missing required longitude column')

        # read elevation and remove column from output
        elevation_column_names = ('Site_Elevation(m)', 'Elevation(m)')
        for elevation_column_name in elevation_column_names:
            if elevation_column_name in INP_COLUMN_NAMES:
                index = INP_COLUMN_NAMES.index(elevation_column_name)
                metadata['elevation'] = float(line[index])
                if elevation_column_name in OUT_COLUMN_NAMES:
                    index = OUT_COLUMN_NAMES.index(elevation_column_name)
                    OUT_COLUMN_NAMES.pop(index)
                break
        else:
            raise ValueError('missing required elevation column')

        if 'Retrieval_Measurement_Scan_Type' in INP_COLUMN_NAMES:
            index = INP_COLUMN_NAMES.index('Retrieval_Measurement_Scan_Type')
            metadata['retrieval_measurement_scan_type'] = line[index]
            if 'Retrieval_Measurement_Scan_Type' in OUT_COLUMN_NAMES:
                index = OUT_COLUMN_NAMES.index(
                    'Retrieval_Measurement_Scan_Type')
                OUT_COLUMN_NAMES.pop(index)

        column_indices = [INP_COLUMN_NAMES.index(column_name)
                          for column_name in OUT_COLUMN_NAMES]

    DROP_COLUMN_NAMES = [
        'Day_of_Year', 'Day_of_Year(fraction)', 'Latitude(Degrees)',
        'Site_Latitude(Degrees)', 'Longitude(Degrees)', 'Site_Longitude(Degrees)',
        'Elevation(m)', 'Site_Elevation(Degrees)', 'AERONET_Site',
        'AERONET_AERONET_Site'
    ]

    data = pd.read_csv(local_fn, skiprows=column_names_line-1,
                       parse_dates=[[date_column_name, time_column_name]])
    times_utc = pd.to_datetime(data[f'{date_column_name}_{time_column_name}'],
                                format='%d:%m:%Y %H:%M:%S')
    data = data.set_index(keys=f'{date_column_name}_{time_column_name}', drop=True)
    data.index.name = 'times_utc'
    data[data == -999] = float('nan')

    data_columns = data.columns.str.casefold()
    for k in range(len(DROP_COLUMN_NAMES)):
        column = DROP_COLUMN_NAMES[k].casefold()
        if column in data_columns:
            DROP_COLUMN_NAMES[k] = data.columns[column == data_columns][0]
    data = data.drop(columns=DROP_COLUMN_NAMES, errors='ignore')

    # calculate beta (only if alpha is already in the dataset)
    if '440-870_Angstrom_Exponent' in data.columns:
        angexp = data['440-870_Angstrom_Exponent']
        data['alpha'] = angexp

        channel_regex = re.compile(r'^AOD_(\w*)nm$')

        channel_wvls = []
        for column_name in data.columns:
            match = re.search(channel_regex, column_name)
            if not match:
                continue

            channel_wvl = float(match.groups()[0])
            if (channel_wvl > 871) or (channel_wvl < 439):
                continue

            channel_wvls.append(channel_wvl)

        if len(channel_wvls) > 0:
            # mean of all available channels. This is to maximize the number
            # of observations. It might occur that throughout time the
            # observations are gathered from alternate channels (Yes, I know;
            # this sounds unlikely, but it is possible...)
            beta = []
            for wvl in channel_wvls:
                wvl_um = wvl*1e-3
                aod = data[f'AOD_{wvl:.0f}nm']
                beta.append(aod*(wvl_um**angexp))
            data['beta'] = quiet_mean(np.stack(beta, axis=1), axis=1)

    elapsed_time = time.time() - init_time
    logger.debug(f'elapsed time: {elapsed_time} seconds')

    return data, metadata
