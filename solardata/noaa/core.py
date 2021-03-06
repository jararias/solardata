
from __future__ import absolute_import, print_function, division

import os
import time
import copy
import zipfile
import multiprocessing as mp
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import yaml
import numpy as np
import pandas as pd
from loguru import logger

from . import config, resources


logger.disable(__name__)

inventory = resources.inventory()


def sites():
    return sorted(inventory.keys())


def sites_metadata(site=None):
    '''
    Retrieves metadata information of the NOAA stations

    Inputs:
    -------
    site: string
      Site acronym

    Returns:
    --------
    Dictionary with keys the acronym of each site and values a metadata
    dictionary.
    '''

    if site is None:
        return inventory

    return inventory.get(site, None)


# def availability(site=None, year=None, month=None, force=False, year_start=1976):
    
#     noaa_config = config.load()
#     localdir = noaa_config.get('localdir')
#     remote_files_fn = localdir.joinpath('noaa_remote_files.yml')

#     if not remote_files_fn.exists() or (force is True):
#         resources.update_list_of_remote_files(force=True)

#     # load the sites from the local sites file
#     logger.debug(f'loading files availability from {remote_files_fn}')
#     with remote_files_fn.open('r') as f:
#         remote_files_list = yaml.load(f.read(), Loader=yaml.FullLoader)

#     years = (year,)
#     if year is None:
#         years = list(range(year_start, datetime.now().year + 1))

#     sites = defaultdict(dict)
#     for this_site in remote_files_list.keys():
#         file_list = list(map(
#             os.path.basename, remote_files_list[this_site]['files']))
#         all_years = {
#             yr: {mn: True
#                  for mn in range(1, 13)
#                  if resources.noaa_filename(this_site, yr, mn) in file_list}
#             for yr in years}
#         sites[this_site].update({yr: values
#                                  for yr, values in all_years.items()
#                                  if values})

#     if site is None:

#         if year is None:

#             if month is None:
#                 return sites

#             return {st: {yr: {month: sites[st][yr][month]}
#                          for yr in sites[st].keys()}
#                     for st in sites.keys()}

#         if month is None:
#             return {st: {year: sites[st][year]}
#                     for st in sites.keys()}

#         return {st: {year: {month: sites[st][year][month]}}
#                 for st in sites.keys()}

#     if year is None:

#         if month is None:
#             return sites[site]

#         return {yr: {month: sites[site][yr][month]}
#                 for yr in sites[site].keys()}

#     if month is None:
#         return {year: sites[site][year]}

#     return {year: {month: sites[site][year][month]}}



def download_database(sites='all', start_year=1998, end_year=None):
    # on and after 1 Jan 1998 the data is reported in 1-min steps

    from datetime import datetime

    end_year = end_year or datetime.now().year
    sites_to_download = sites() if sites == 'all' else [sites]

    files_to_download = []
    for site in sites_to_download:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                files_to_download.append((site, year, month))

    print(files_to_download)

    PROGRESS_BAR_AVAILABLE = True
    try:
        from tqdm import tqdm
    except ImportError:
        PROGRESS_BAR_AVAILABLE = False

    try:
        workers = mp.Pool(2)

        if PROGRESS_BAR_AVAILABLE:
            n_files = len(files_to_download)
            pbar = tqdm(total=n_files, unit='files', unit_scale=1)

            download = workers.starmap_async(
                resources.noaa_download, files_to_download, chunksize=1)

            while not download.ready():
                tasks_completed = n_files - download._number_left
                pbar.update(int(tasks_completed - pbar.n))

            if pbar.n < n_files:
                pbar.update(int(n_files - pbar.n))

        else:
            download = workers.starmap_async(
                resources.noaa_download, files_to_download, chunksize=1)

            while not download.ready():
                pass

    except Exception as exc:
        raise exc

    finally:
        if PROGRESS_BAR_AVAILABLE:
            pbar.close()
        workers.close()
        workers.join()


def load_data(site, years, months, timeout=None, force=False, n_threads=2):
    # timestamps are referred to the center of the interval
    # n_threads is for the http downloader

    import itertools as it
    import multiprocessing as mp

    # check the files in the local database and download if missing
    
    periods = list(it.product(sorted(years), sorted(months)))

    try:
        workers = mp.Pool(2)  # mp.cpu_count())

        download_args = [
            (site, year, month, timeout, force, n_threads)
            for year, month in periods
        ]
        
        download = workers.starmap_async(
            resources.noaa_download, download_args, chunksize=1)

    except Exception as exc:
        raise exc

    finally:
        workers.close()
        workers.join()

    noaa_config = config.load()
    localdir = noaa_config.get('localdir')

    site_dir = localdir.joinpath(site)
    if not site_dir.exists():
        site_dir.mkdir(parents=True)

    metadata = {}
    if site in inventory:
        metadata.update(sites_metadata(site))
    else:
        logger.warning(f'site {site} is missing in the '
                       'inventory. Please add it!')

    all_data = []

    for year, month in periods:
        logger.debug(f'loading data for site {site} and period {year}-{month:02d}')

        zip_fname = site_dir.joinpath(
            resources.noaa_filename(site, year, month, ext='zip')
        )

        if zip_fname.exists():
            logger.info(f'reading file {zip_fname.name}')
            data = pd.read_csv(zip_fname, sep='\s+', skiprows=4,
                            header=None, parse_dates=[[0, 1, 2, 3, 4]])
            data['times'] = pd.to_datetime(data['0_1_2_3_4'], format='%Y %m %d %H %M')
            data = data[[5, 6, 7]].rename(columns={5: 'dni', 6: 'dif', 7: 'ghi'})
            data.index.name = 'times_utc'
            data[data == -999.] = float('nan')
            
            all_data.append(data)
            continue

        daily_files = [
            site_dir.joinpath(str(year)).joinpath(fn)
            for fn in resources.noaa_filename(site, year, month, ext='dat')
        ]

        if metadata['network'].casefold() == 'baseline':
            column_names = [
 	            'jday', 'dt', 'zen', 'dw_solar', 'qc_dwsolar', 'uw_solar', 'qc_uwsolar',
                'direct_n', 'qc_direct_n', 'diffuse', 'qc_diffuse', 'dw_ir', 'qc_dwir',
                'dw_casetemp', 'qc_dwcasetemp', 'dw_dometemp', 'qc_dwdometemp', 'uw_ir',
                'qc_uwir', 'uw_casetemp', 'qc_uwcasetemp', 'uw_dometemp', 'qc_uwdometemp',
                'uvb', 'qc_uvb', 'par', 'qc_par', 'netsolar', 'qc_netsolar', 'netir',
                'qc_netir', 'totalnet', 'qc_totalnet', 'temp', 'qc_temp', 'rh', 'qc_rh',
                'windspd', 'qc_windspd', 'winddir', 'qc_winddir', 'pressure', 'qc_pressure'
            ]
            alt_column_names = column_names
            columns_to_drop = ['jday', 'dt']
       
        if metadata['network'].casefold() == 'solrad':
            column_names = [
                'jday', 'dt', 'zen', 'dw_psp', 'qc_dwpsp', 'direct', 'qc_direct',
                'diffuse', 'qc_diffuse', 'uvb', 'qc_uvb' ,'uvb_temp', 'qc_uvb_temp',
                'std_dw_psp', 'std_direct', 'std_diffuse', 'std_uvb'
            ]
            alt_column_names = [  # for Madison after 18 June 2009
                'jday', 'dt', 'zen', 'dw_psp', 'qc_dwpsp', 'direct', 'qc_direct',
                'diffuse', 'qc_diffuse', 'uvb', 'qc_uvb' ,'uvb_temp', 'qc_uvb_temp',
                'dpir', 'qc_dpir', 'dpirc', 'qc_dpirc', 'dpird', 'qc_dpird',
                'std_dw_psp', 'std_direct', 'std_diffuse', 'std_uvb', 'std_uvb',
                'std_dpir', 'std_dpirc', 'std_dpird'
            ]
            columns_to_drop = ['jday', 'dt']

        def file_reader(fname):
            if not fname.exists():
                logger.warning(f'missing file {fname}')
                return None

            try:
                kwargs = dict(header=None, parse_dates=[[0, 2, 3, 4, 5]])
                data = pd.read_csv(fname, sep='\s+', skiprows=2, **kwargs)
                data['times'] = pd.to_datetime(data['0_2_3_4_5'], format='%Y %m %d %H %M')
                data = data.set_index(keys='times', drop=True).drop(columns=['0_2_3_4_5'])
                try:
                    data.columns = column_names
                except Exception as exc:
                    data.columns = alt_column_names
                data = data.drop(columns=columns_to_drop, errors='ignore')
                data[data == -9999.9] = float('nan')
                data.index.name = 'times_utc'
            except Exception as exc:
                logger.error(f'exception raised while reading {fname}: {exc.args}. Skipping file!!')
                data = None

            return data
        
        # exclude missing days... (i.e., when file_reader gets None)
        data = list(filter(lambda e: e is not None, [file_reader(fn) for fn in sorted(daily_files)]))
        if not data:
            continue

        all_data.append(pd.concat(data, axis=0))
        # return data, metadata
    
    if not all_data:
        return None, metadata

    all_data = pd.concat(all_data, axis=0)
    return all_data, metadata