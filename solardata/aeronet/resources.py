
from __future__ import absolute_import, print_function, division

import os
import io
import sys
import time
import gzip
import glob
from datetime import datetime, timedelta

import urllib.request as urllib
try:
    from tqdm import tqdm
    PROGRESS_BAR_AVAILABLE = True
except ImportError:
    PROGRESS_BAR_AVAILABLE = False

from loguru import logger

from . import config


logger.disable(__name__)


AOD_DATA_TYPES = ('AOD10', 'AOD15', 'AOD20',
                  'SDA10', 'SDA15', 'SDA20',
                  'TOT10', 'TOT15', 'TOT20')

INV_DATA_TYPES = ('ALM15', 'ALM20', 'HYB15', 'HYB20')

INV_PRODUCTS = ('SIZ', 'RIN', 'CAD', 'VOL', 'TAB', 'AOD',
                'SSA', 'ASY', 'FRC', 'LID', 'FLX', 'ALL')


def aeronet_filename(site, year, month, data_type, is_daily):
    '''
    Returns the aeronet file name in the local machine

    Parameters:
    -----------
    site: string
        AERONET site
    year: integer
        Data year
    month: integer
        Data month
    data_type: string
        AERONET data type
        See https://aeronet.gsfc.nasa.gov/print_web_data_help_v3.html and
        https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv.html for
        the inversion data types
    is_daily: boolean
        True if the file contains daily data. False otherwise
    '''

    avg = 'daily' if is_daily is True else 'all_points'
    return f'{site}_{year:d}{month:02d}_{avg}_{data_type.lower()}.dat.gz'


def void_index_filename(site, data_type, is_daily):
    '''
    Returns the file name of the index of empty months in the local machine

    Parameters:
    -----------
    site: string
        AERONET site
    data_type: string
        AERONET data type
        See https://aeronet.gsfc.nasa.gov/print_web_data_help_v3.html and
        https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv.html for
        the inversion data types
    is_daily: boolean
        True if the file contains daily data. False otherwise
    '''

    avg='daily' if is_daily is True else 'all_points'
    return f'.void_index_{site}_{avg}_{data_type.lower()}.gz'


def download(url, out_fn, max_retries=3, retry_delay_seconds=5,
             blocksize_bytes=1024, with_progress_bar=True):
    '''
    Downloads a url file

    Parameters:
    -----------
    url: string
        File url address
    out_fn: string
        File name in the local machine
    max_retries: integer
        Maximum number of retries on fail
    retry_delay_seconds: integer
        Seconds between consecutive retries
    blocksize_bytes: integer
        Download the file in blocks of this size, in bytes
    with_progress_bar: boolean
        Show download progress bar
    '''

    # download loop up to max_retries
    n_retries = 0
    while n_retries < max_retries:
        try:
            try:
                remote_f = urllib.urlopen(url)

                if PROGRESS_BAR_AVAILABLE and with_progress_bar:
                    progress_bar = tqdm(total=None, unit='kB')

                with open(out_fn, 'wb') as local_f:
                    data_block = remote_f.read(blocksize_bytes)

                    while data_block:
                        local_f.write(data_block)
                        if PROGRESS_BAR_AVAILABLE and with_progress_bar:
                            progress_bar.update(blocksize_bytes / 1024.)
                        data_block = remote_f.read(blocksize_bytes)

            except urllib.HTTPError as exc:
                raise ValueError('(HTTPError) %s' % exc.reason)
            n_retries = max_retries
        except Exception as exc:
            n_retries += 1
            if max_retries > 1:
                logger.warning(
                    f'Retry {n_retries}/{max_retries} {exc.args[0]}: {url}')
            if n_retries < max_retries:
                time.sleep(retry_delay_seconds)
        finally:
            if PROGRESS_BAR_AVAILABLE and with_progress_bar:
                progress_bar.close()


def read_void_index(site, data_type, daily):
    aero_config = config.load()
    localdir = aero_config['localdir']

    void_fn = void_index_filename(site, data_type, daily)
    local_fn = localdir.joinpath(site).joinpath(void_fn)

    if local_fn.exists():
        with gzip.open(local_fn, 'r') as inp_f:
            void_files = [
                line.decode('utf-8').strip() for line in inp_f.readlines()]
        return void_files
    return []


def update_void_index(site, data_type, daily, void_files=[]):
    aero_config = config.load()
    localdir = aero_config['localdir']

    if isinstance(void_files, str):
        void_files = [void_files]

    void_fn = void_index_filename(site, data_type, daily)
    local_fn = localdir.joinpath(site).joinpath(void_fn)

    if not localdir.exists():
        localdir.mkdir(parents=True)

    for void_file in [localdir.joinpath(fn) for fn in void_files]:
        if void_file.exists():
            void_file.unlink()

    known_void_files = read_void_index(site, data_type, daily)

    # merge the void files listed in the index and the new void files
    all_void_files = set(known_void_files).union(void_files)

    # remove from the list of void files those files that exist in the dir.
    all_files = [fn.name for fn in localdir.joinpath(site).glob('*.dat.gz')]
    all_void_files = all_void_files.difference(all_files)

    logger.debug(f'updating void-index file {void_fn} with '
                 f'void_files {sorted(all_void_files)}')

    with gzip.open(local_fn, 'w') as gz_f:
        gz_f.write('\n'.join(sorted(all_void_files)).encode('utf-8'))


def download_aeronet_v3(site, from_datetime,  data_type, inv_product=None,
                        daily=False, to_datetime=None,
                        as_html=False, bbox=None):
    '''
    Downloads Version 3 AERONET data through the REST interface and
    archives the data into the AERONET's local database

    Parameters:
    -----------
    site: string
        AERONET site name. Mandatory.
    from_datetime: datetime
        Request data starting at this datetime. Mandatory.
    data_type: string
        AERONET data type. Mandatory.
        See https://aeronet.gsfc.nasa.gov/print_web_data_help_v3.html and
        https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv.html for
        the inversion data types
    inv_product: string
        Inversion product type. Required only for inversion data types.
        See https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv.html
    daily: boolean
        True to return daily values
    to_datetime: datetime
        Request data ending at this datetime. If not provided, only one
        day, starting at from_datetime, is downloaded
    as_html: boolean
        Download the data as html code, as provided by the REST server
    bbox: sequence of 4 floats
        Bounding box coordinates enclosing the AERONET sites that will
        be returned. The coordinates are: (lower-left latitude,
        lower-left longitude, upper-right latitude, upper-right longitude)
    '''

    # main AERONET server
    if data_type in AOD_DATA_TYPES:
        SERVER = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?'
    elif data_type in INV_DATA_TYPES:
        SERVER = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_inv_v3?'
        if inv_product not in INV_PRODUCTS:
            raise ValueError(f'unknown inversion product {inv_product}')
    else:
        raise ValueError(f'unknown data type {data_type}')

    arguments = []

    # if site is 'all', the argument site is not added to
    # the url and all sites are returned
    if site != 'all':
        # site list: https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt
        # check this list before retrieving data. I can keep a local copy
        # and choose when to update it. Perhaps I can force automatic updating
        # when the copy becomes older than a few days/weeks
        arguments.append(f'site={site}')

    # from_datetime: it is mandatory
    if from_datetime.year < 1993:
        raise ValueError('from_datetime.year cannot be older than 1992')

    arguments.append(f'year={from_datetime.year}')
    arguments.append(f'month={from_datetime.month}')
    arguments.append(f'day={from_datetime.day}')
    arguments.append(f'hour={from_datetime.hour}')

    # to_datetime: it is optional
    if to_datetime is None:
        to_datetime = datetime.now() + timedelta(1)
        to_datetime = to_datetime.replace(hour=0, second=0)

    arguments.append(f'year2={to_datetime.year}')
    arguments.append(f'month2={to_datetime.month}')
    arguments.append(f'day2={to_datetime.day}')
    arguments.append(f'hour2={to_datetime.hour}')

    # data_type (table 2): it is mandatory
    arguments.append(f'{data_type}=1')

    # avg: all points or daily averages: it is mandatory
    arguments.append('AVG=20' if daily is True else 'AVG=10')

    if data_type in INV_DATA_TYPES:
        arguments.append(f'product={inv_product}')

    # bounding box: optional
    if bbox is not None:
        ll_lat, ll_lon, ur_lat, ur_lon = bbox
        if (ll_lat < -90.) or (ll_lat > 90.):
            raise ValueError(
                f'lower left latitude ({ll_lat:.4f}) out of bounds')
        if (ur_lat < -90.) or (ur_lat > 90.):
            raise ValueError(
                'upper right latitude ({ur_lat:.4f}) out of bounds')
        if (ll_lon < -180.) or (ll_lon > 180.):
            raise ValueError(
                'lower left longitude ({ll_lon:.4f}) out of bounds')
        if (ur_lon < -180.) or (ur_lon > 180.):
            raise ValueError(
                'upper right longitude ({ur_lon:.4f}) out of bounds')
        arguments.append(f'lat1={ll_lat:.4f}')
        arguments.append(f'lon1={ll_lon:.4f}')
        arguments.append(f'lat2={ur_lat:.4f}')
        arguments.append(f'lon2={ur_lon:.4f}')

    # if_no_html: optional
    arguments.append('if_no_html=0' if as_html is True else 'if_no_html=1')

    url = SERVER + '&'.join(arguments)

    aero_config = config.load()
    localdir = aero_config['localdir']
    base_fn = aeronet_filename(
        site, from_datetime.year, from_datetime.month, data_type, daily)

    out_dir = localdir.joinpath(site)
    if not out_dir.exists():
        out_dir.mkdir(parents=True)

    logger.debug(f'data url: {url}')
    out_fn = out_dir.joinpath(base_fn)  # ending with dat.gz
    dat_fn = out_fn.with_suffix('')  # ending with dat
    download(url, dat_fn)

    with dat_fn.open('r') as dat_f:
        data = dat_f.read().encode('utf-8')

    if len(data.splitlines()) < 7:
        update_void_index(site, data_type, daily, out_fn.name)
    else:
        with gzip.open(out_fn, 'w') as gz_f:
            gz_f.write(data)

    dat_fn.unlink()
