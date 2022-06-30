
import os
import re
import time
import ftplib
import importlib_resources

import yaml
import pandas as pd
from loguru import logger

from . import config
from .exception import NOAADownloadError


logger.disable(__name__)


def inventory():
    root_dir = importlib_resources.files('solardata')
    inv_fn = root_dir.joinpath('noaa/site_inventory.yml')

    if not inv_fn.exists():
        raise ValueError('missing NOAA inventory file %s' % inv_fn)

    with inv_fn.open('r') as f:
        inventory = yaml.load(f.read(), Loader=yaml.FullLoader)

    return inventory


def noaa_filename(site, year, month, ext='zip'):
    if ext == 'zip':
        return f'{site}_{year:d}_{month:02d}.{ext}'
    
    if ext == 'dat':
        first_day = pd.to_datetime(f'{year}{month:02d}01')
        last_day = first_day + pd.Timedelta(first_day.days_in_month - 1, 'D')
        doy_first_day = first_day.day_of_year
        doy_last_day = last_day.day_of_year
        return [f'{site}{str(year)[-2:]}{doy:03d}.dat'
                for doy in range(doy_first_day, doy_last_day + 1)]
    
    return None


def noaa_download(site, year, month, timeout=None, force=False):
    noaa_config = config.load()
    server = noaa_config.get('server')
    localdir = noaa_config.get('localdir')
    remotedir = noaa_config.get('remotedir')

    site_dir = localdir.joinpath(site)
    if not site_dir.exists():
        site_dir.mkdir(parents=True)

    zip_fname = site_dir.joinpath(noaa_filename(site, year, month, ext='zip'))
    if zip_fname.exists() and (not force):
        return

    dat_fnames = [
        site_dir.joinpath(f'{year}/{fn}')
        for fn in noaa_filename(site, year, month, ext='dat')
    ]
    missing_dat_files = [fn for fn in dat_fnames if not fn.exists()]
    if (not missing_dat_files) and (not force):
        return

    try:
        logger.debug(f'Accessing to server {server}, remotedir {remotedir}')
        ftp = ftplib.FTP(server, timeout=timeout)
        ftp.login()
        ftp.cwd(remotedir)
    except Exception as exc:
        ftp.close()
        logger.error(f'loging error: {exc.args[0]}')
        return

    try:
        ftp.cwd(site)
    except Exception as exc:
        ftp.close()
        logger.error(f'missing directory {site}: {exc.args[0]}')
        return

    regex = re.compile(site + r'_(\d{4})_(\d{2}).zip')
    remote_files = filter(regex.match, ftp.nlst())

    # data is in monthly zip files...    
    if (fn := noaa_filename(site, year, month, ext='zip')) in remote_files:
        logger.info(f'Downloading file {fn} from NOAA server')
        try:
            site_fn = site_dir.joinpath(fn)
            with site_fn.open('wb') as f:
                ftp.retrbinary('RETR {0}'.format(fn), f.write)
            logger.debug(f'file {fn} added to the local data base')
        except Exception as exc:
            ftp.close()
            logger.error(f'download error: {exc.args[0]}')
            return
    
    else:  # data is in daily dat files...
        try:
            ftp.cwd(str(year))
        except Exception as exc:
            ftp.close()
            logger.error(f'missing directory {site}/{year}: {exc.args[0]}')
            return

        site_dir = site_dir.joinpath(str(year))
        if not site_dir.exists():
            site_dir.mkdir(parents=True)

        regex = re.compile(site + r'\d{5}.dat')
        remote_files = list(filter(regex.match, ftp.nlst()))
        daily_files = noaa_filename(site, year, month, ext='dat')

        for fn in filter(lambda fn: fn in remote_files, daily_files):
            if (site_fn := site_dir.joinpath(fn)).exists():
                continue
            logger.info(f'Downloading file {fn} from NOAA server')
            try:
                site_fn = site_dir.joinpath(fn)
                with site_fn.open('wb') as f:
                    ftp.retrbinary('RETR {0}'.format(fn), f.write)
                logger.debug(f'file {fn} added to the local data base')
            except Exception as exc:
                logger.error(f'download error in file {fn}: {exc.args[0]}')
        ftp.close()


# def update_list_of_remote_files(force=False):

#     MAX_ELAPSED_DAYS_FROM_LAST_UPDATE = 10

#     noaa_config = config.load()
#     server = noaa_config.get('server')
#     remotedir = noaa_config.get('remotedir')

#     localdir = noaa_config.get('localdir')
#     if not localdir.exists():
#         localdir.mkdir(parents=True)

#     out_fn = localdir.joinpath('noaa_remote_files.yml')

#     if out_fn.exists() and (force is False):
#         elapsed_time_secs = os.path.getmtime(out_fn) - time.time()
#         elapsed_time_days = elapsed_time_secs / (3600. * 24.)
#         if elapsed_time_days < MAX_ELAPSED_DAYS_FROM_LAST_UPDATE:
#             return

#     logger.info('updating local list of NOAA files from remote server')
#     try:
#         ftp = ftplib.FTP(server)
#         ftp.login()
#         ftp.cwd(remotedir)
#     except Exception as exc:
#         ftp.close()
#         raise NOAADownloadError(
#             'loging error: {0}'.format(exc.args[0])
#         ) from exc

#     available_sites = sorted(filter(lambda x: len(x) == 3, ftp.nlst()))

#     sites = {}
#     try:
#         for site in available_sites:
#             logger.debug(f'checking site {site}')
#             regex = re.compile('{0}/{0}'.format(site) + r'_(\d{4})_(\d{2}).zip')
#             files = list(filter(regex.match, ftp.nlst(site)))
#             logger.debug(f'cheking site {site}: {len(files)} files found')
#             sites[site] = {'files': files}

#     except Exception as exc:
#         raise NOAADownloadError(
#             'listing files for site {0}: {1}'.format(site, exc.args[0])
#         ) from exc

#     finally:
#         logger.debug('closing FTP connection')
#         ftp.close()

#     logger.debug(f'dumping sites to {out_fn}')
#     with out_fn.open('w') as f:
#         f.write(yaml.dump(sites))
