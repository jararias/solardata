
from __future__ import (absolute_import, print_function,
                        division, unicode_literals)

import os
import re
import time
import ftplib
from netrc import netrc
from datetime import datetime

import yaml
import requests
from loguru import logger
from bs4 import BeautifulSoup

from . import config
from .exception import BSRNDownloadError


logger.disable(__name__)


def bsrn_filename(site, year, month, ext='dat.gz'):
    return f'{site}{month:02d}{str(year)[-2:]}.{ext}'


def bsrn_download(site, year, month, user=None, password=None, server=None, timeout=None):
    bsrn_config = config.load()
    
    if server is None:
        logger.debug('Server not provided. Using default server from config file')
        server = bsrn_config['server']
        logger.debug(f'server: {server}')

    if user is None:
        logger.debug('User not provided. Using user fron netrc file')
        try:
            user, _, _ = netrc().authenticators(server)
        except Exception as exc:
            raise exc

    if password is None:
        logger.debug('Password not provided. Using password fron netrc file')
        try:
            _, _, password = netrc().authenticators(server)
        except Exception as exc:
            raise exc

    try:
        ftp = ftplib.FTP(server, user, password, timeout=timeout)
        ftp.login()
    except Exception as exc:
        ftp.close()
        raise BSRNDownloadError(
            f'loging error: {exc.args[0]}'
        ) from exc

    sites = filter(lambda x: len(x) == 3, ftp.nlst())
    if site not in sites:
        ftp.close()
        raise BSRNDownloadError(f'missing site {site}')

    ftp.cwd(site)
    files = filter(lambda x: x.endswith('dat.gz'), ftp.nlst())
    if (requested_fn := bsrn_filename(site, year, month)) not in files:
        ftp.close()
        raise BSRNDownloadError(f'missing file {requested_fn}')

    localdir = bsrn_config['localdir']

    site_dir = localdir.joinpath(site)
    if not site_dir.exists():
        site_dir.mkdir(parents=True)

    site_fn = site_dir.joinpath(requested_fn)
    logger.info(f'downloading file {requested_fn} from BSRN server')

    try:
        with site_fn.open('wb') as f:
            ftp.retrbinary(f'RETR {requested_fn}', f.write)
    except Exception as exc:
        raise BSRNDownloadError(
            f'download error: {exc.args[0]}'
        ) from exc
    finally:
        ftp.close()

    logger.success(f'{requested_fn} added to {site_fn.parent}')


def update_list_of_remote_files(user=None, password=None, server=None, force=False):
    bsrn_config = config.load()

    MAX_ELAPSED_DAYS_FROM_LAST_UPDATE = 10

    localdir = bsrn_config.get('localdir')
    if not localdir.exists():
        localdir.mkdir(parents=True)

    out_fn = localdir.joinpath('bsrn_remote_files.yml')

    if out_fn.exists() and (force is False):
        elapsed_time_secs = os.path.getmtime(out_fn) - time.time()
        elapsed_time_days = elapsed_time_secs / (3600. * 24.)
        if elapsed_time_days < MAX_ELAPSED_DAYS_FROM_LAST_UPDATE:
            return

    if server is None:
        logger.debug('Server not provided. Using default server from config file')
        server = bsrn_config['server']
        logger.debug(f'server: {server}')

    if user is None:
        logger.debug('User not provided. Using user fron netrc file')
        try:
            user, _, _ = netrc().authenticators(server)
        except Exception as exc:
            raise exc

    if password is None:
        logger.debug('Password not provided. Using password fron netrc file')
        try:
            _, _, password = netrc().authenticators(server)
        except Exception as exc:
            raise exc

    logger.debug('updating local copy of BSRN remote files')
    try:
        ftp = ftplib.FTP(server, user, password)
        ftp.login()
    except Exception as exc:
        ftp.close()
        raise BSRNDownloadError(
            f'loging error: {exc.args[0]}'
        ) from exc

    sites = {}
    try:
        for site in sorted(filter(lambda x: len(x) == 3, ftp.nlst())):
            logger.debug(f'checking site {site}')
            regex = re.compile(r'{0}/{0}\d\d\d\d.dat.gz'.format(site))
            files = list(filter(regex.match, ftp.nlst(site)))
            logger.debug(f'cheking site {site}: {len(files)} files found')
            sites[site] = {'files': files}
    except Exception as exc:
        raise BSRNDownloadError(
            f'listing files for site {site}: {exc.args[0]}'
        ) from exc
    finally:
        logger.debug('closing FTP connection')
        ftp.close()

    logger.debug(f'serializing sites to {out_fn}')
    with out_fn.open('w') as f:
        f.write(yaml.dump(sites))


def update_site_metadata_from_bsrn_web_site(force=False):
    bsrn_config = config.load()

    MAX_ELAPSED_DAYS_FROM_LAST_UPDATE = 10

    localdir = bsrn_config.get('localdir')
    if not localdir.exists():
        localdir.mkdir(parents=True)

    out_fn = localdir.joinpath('bsrn_site_metadata.yml')

    if out_fn.exists() and (force is False):
        elapsed_time_secs = os.path.getmtime(out_fn) - time.time()
        elapsed_time_days = elapsed_time_secs / (3600. * 24.)
        if elapsed_time_days < MAX_ELAPSED_DAYS_FROM_LAST_UPDATE:
            return

    logger.info('updating local copy of BSRN sites')
    # get the table of BSRN sites from the BSRN web site
    url = ('https://www.pangaea.de/ddi?request=bsrn/BSRNEvent&format'
           '=html&title=BSRN+Stations')
    logger.info(f'url: {url}')
    r = requests.get(url)

    soup = BeautifulSoup(r.text, features='html.parser')

    table = soup.findAll('table')[0]
    rows = table.findAll('tr')

    listing = {}
    for row in rows[1:]:
        logger.debug(f'Parsing row: {row}')
        entries = [entry.getText().lstrip('&nbsp;')
                   for entry in row.findAll('td')]
        acronym = entries[1].lower()
        listing[acronym] = {
            'station': entries[0],
            'location': entries[2],
            'info': entries[3],
            'latitude': float(entries[4]) if entries[4] != '' else -999.,
            'longitude': float(entries[5]) if entries[5] != '' else -999.,
            'elevation': float(entries[6]) if entries[6] != '' else -999.,
            'datetime_start': (
                datetime.strptime(entries[7], '%Y-%m-%d')
                if entries[7] != '' else ''),
            'datetime_end': (
                datetime.strptime(entries[8], '%Y-%m-%d')
                if entries[8] != '' else ''),
            'remarks': '; '.join(entries[9:-1])
        }

    with out_fn.open('w') as f:
        f.write(yaml.dump(listing, default_flow_style=False))


def read_site_metadata_file(force=False):
    bsrn_config = config.load()

    sites_fn = bsrn_config['localdir'].joinpath('bsrn_site_metadata.yml')

    logger.debug(f'retrieving sites metadata from file {sites_fn}')

    update_site_metadata_from_bsrn_web_site(force)

    logger.debug(
        'loading sites metadata from <BSRN local database>/%s',
        os.path.relpath(sites_fn, bsrn_config['localdir']))
    with sites_fn.open('r', encoding='utf-8') as f:
        bsrn_sites_list = yaml.load(f.read(), Loader=yaml.FullLoader)

    return bsrn_sites_list
