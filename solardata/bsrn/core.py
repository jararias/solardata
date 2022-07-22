
from __future__ import absolute_import, print_function, division

import os
import re
import time
import gzip
import calendar
import itertools
import multiprocessing as mp
from datetime import datetime, timedelta
from collections import defaultdict

import yaml
from loguru import logger
from termcolor import colored

import numpy as np
import pandas as pd
import pylab as pl
from matplotlib.dates import MonthLocator, DateFormatter

from . import config, resources, parser
from .exception import BSRNDownloadError
from .util import (
    time_interpolation,
    is_sequence,
    styler,
    guess_time_resolution
)
from .description_tables import (
    TableA4,
    TableA5,
    LogicalRecordDescription
)
from .timezone import (
    timezone_from_latlon,
    timezone_utcoffset,
    timezone_as_string
)


logger.disable(__name__)


def sites(force=False):
    '''
    Available sites in the BSRN

    Inputs:
    -------
    force: bool
      If False, the local list of available sites in the remote server
      is used. If True, the remote server is inspected and the local
      list of available files remotely is updated

    Returns:
    --------
    List of available sites
    '''

    return sorted(sites_metadata(None, force))


def sites_metadata(site=None, force=False, include_availability=False):
    '''
    Metadata of BSRN sites

    Inputs:
    -------
    site: string
      Site acronym (e.g., `car` for Carpentras)
    force: bool
      If False, the local list of available sites in the remote server
      is used. If True, the remote server is inspected and the local
      list of available files remotely is updated
    include_availability: bool
      If True, the number of months with data is computed for each site

    Returns:
    --------
    Dictionary
    '''

    logger.debug(
        f'reading site metadata at {"all sites" if site is None else site}')
    bsrn_sites_list = resources.read_site_metadata_file(force)

    for key in bsrn_sites_list.keys():
        maxdate = datetime(datetime.now().year + 1, 1, 1).strftime('%Y-%m-%d')
        url = (
            'https://www.pangaea.de/?q=project%3Alabel%3ABSRN+%2Bevent%3A'
            'label%3A{0}+-guidelines&mindate=1992-01-01T00%3A00%3A00&'
            'maxdate={1}T23%3A59%3A59'.format(key.upper(), maxdate))
        bsrn_sites_list[key]['url'] = url

    if include_availability:
        avail = availability(force=force)
        for key in bsrn_sites_list.keys():
            n_months = sum([sum(v.values()) for v in avail[key].values()])
            bsrn_sites_list[key]['n_months'] = n_months

    if site is None:
        return bsrn_sites_list

    if site not in bsrn_sites_list:
        logger.warning(f'missing site {site}')
        return None

    return bsrn_sites_list[site]


def availability(site=None, year=None, month=None, print_table=False,
                 force=False, year_start=1992):
    '''
    Data availability in the BSRN sites

    Inputs:
    -------
    site: str or None
      Site acronym (e.g., `car` for Carpentras). If None, all sites are
      considered
    year: int
      Year to explore. If None, all years since `year_start` are considered
    month: int or None
      Month to explore. If None, all months are considered
    print_table: bool
      print to standard output a table with availability information
    force: bool
      If False, the local list of available sites in the remote server
      is used. If True, the remote server is inspected and the local
      list of available files remotely is updated
    year_start: int
      first year to be explored if `year` is None

    Returns:
    --------
    Dictionary
    '''

    bsrn_config = config.load()
    localdir = bsrn_config['localdir']
    remote_files_fn = localdir.joinpath('bsrn_remote_files.yml')

    resources.update_list_of_remote_files(force)

    # load the sites from the local sites file
    with remote_files_fn.open('r') as f:
        remote_files_list = yaml.load(f.read(), Loader=yaml.FullLoader)

    years = (year,)
    if year is None:
        years = list(range(year_start, datetime.now().year + 1))

    sites = defaultdict(dict)
    for this_site in remote_files_list.keys():
        remote_files = remote_files_list[this_site]['files']
        file_list = list(map(os.path.basename, remote_files))
        all_years = {
            yr: {mn: True for mn in range(1, 13)
                 if resources.bsrn_filename(this_site, yr, mn) in file_list}
            for yr in years
        }
        sites[this_site].update(
            {yr: values for yr, values in all_years.items() if values}
        )

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

            if print_table is True:
                month_names = [calendar.month_abbr[i] for i in range(1, 13)]
                print(colored(f'{site:4s}', 'red', 'on_white'),
                      colored(' '.join(month_names), 'green'))
                for yr in sites[site]:
                    line = colored(f'{yr:4d}', 'green')
                    for mo in range(1, 13):
                        if mo in sites[site][yr]:
                            line += '  X '
                        else:
                            line += '    '
                    print(line + colored(f' {yr}', 'green'))

            return sites[site]

        return {yr: {month: sites[site][yr][month]}
                for yr in sites[site].keys()}

    if month is None:
        return {year: sites[site][year]}

    return {year: {month: sites[site][year][month]}}


def availability_map(force=False, figsize=(20, 10), dpi=None):

    def timeline_boundaries(files):
        regex = re.compile(r'(.{3})/(.{3})(\d\d)(\d\d).dat.gz')
        timeline = []
        for file in files:
            month, year = map(int, regex.match(file).groups()[-2:])
            year = 2000 + year if year < 50 else 1900 + year
            timeline.append((year, month))
        timeline = sorted(timeline)
        datetime_start = datetime(timeline[0][0], timeline[0][1], 1)
        datetime_end = datetime(timeline[-1][0], timeline[-1][1], 1)
        return datetime_start, datetime_end

    def next_month(adatetime):
        the_next_month = adatetime + timedelta(35)
        return the_next_month.replace(day=1)

    def monthly_range(from_datetime, to_datetime):
        datetime_range = [from_datetime]
        while datetime_range[-1] < to_datetime:
            datetime_range.append(next_month(datetime_range[-1]))
        return datetime_range

    bsrn_config = config.load()
    localdir = bsrn_config['localdir']

    remote_files_fn = localdir.joinpath('bsrn_remote_files.yml')

    resources.update_list_of_remote_files(force)

    # load the sites from the local sites file
    logger.debug(
        'loading files availability from <BSRN local database>/%s',
        os.path.relpath(remote_files_fn, localdir))
    with open(remote_files_fn, 'r') as f:
        sites = yaml.load(f.read(), Loader=yaml.FullLoader)

    # search for the start and end of the availability timeline
    for site in sites:

        sites[site]['timeline_start'] = None
        sites[site]['timeline_end'] = None

        if not (files := sites[site]['files']):
            logger.warning('there are no data files for site %s', site)
            continue

        datetime_start, datetime_end = timeline_boundaries(files)
        sites[site]['timeline_start'] = datetime_start
        sites[site]['timeline_end'] = datetime_end

    # search for the start and end of the global timeline
    timeline_start = datetime(2050, 1, 1)
    timeline_end = datetime(1900, 1, 1)
    for site in sites:
        if (sites[site]['timeline_start'] is None) or \
                (sites[site]['timeline_end'] is None):
            continue
        timeline_start = min(timeline_start, sites[site]['timeline_start'])
        timeline_end = max(timeline_end, sites[site]['timeline_end'])

    timeline_start = timeline_start.replace(month=1)
    timeline_end = timeline_end.replace(
        year=timeline_end.year + 1, month=1, day=1)

    # construct the availability calendar map
    timeline = monthly_range(timeline_start, timeline_end)
    availability = np.ones((len(sites) + 1, len(timeline)), dtype='i')

    txt_calmap = '      '
    for year in range(timeline_start.year, timeline_end.year):
        txt_calmap += '    {0}    |'.format(year)
    txt_calmap += '\n'

    site_acronyms = sorted(sites.keys())
    for n_site, site in enumerate(site_acronyms):
        txt_calmap += '{0} > '.format(site)
        for n_timeline_step, timeline_step in enumerate(timeline):
            if (n_timeline_step > 0) and (timeline_step.month == 1):
                txt_calmap += '|'
            fn = resources.bsrn_filename(
                site, timeline_step.year, timeline_step.month)
            if os.path.join(site, fn) not in sites[site]['files']:
                availability[n_site, n_timeline_step] = 0
                txt_calmap += ' '
            else:
                txt_calmap += 'o'
        txt_calmap += '\n'

    n_fig = 1 if not pl.get_fignums() else max(pl.get_fignums())
    fig = pl.figure(n_fig, figsize=figsize, dpi=dpi)
    ax = fig.add_subplot(111)
    n_sites = len(site_acronyms)
    calmap = np.ma.masked_equal(availability, 0)
    y = np.arange(n_sites + 1)
    ax.pcolormesh(timeline, y, calmap, vmin=0, vmax=1)
    ax.xaxis.axis_date()
    ax.xaxis.set_major_locator(MonthLocator(bymonth=(6, 12)))
    ax.xaxis.set_minor_locator(MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(DateFormatter('%m/%Y'))

    ax.yaxis.set_ticks(0.5 + np.arange(n_sites), minor=False)
    ax.yaxis.set_ticklabels(site_acronyms)
    ax.yaxis.set_ticks(np.arange(n_sites + 1), minor=True)

    pl.tick_params(axis='y', which='major', length=0)
    pl.tick_params(axis='y', which='minor', direction='inout', length=7)
    pl.tick_params(labelbottom='on', labeltop='on', labelleft='on',
                   labelright='on')
    pl.tick_params(axis='x', which='major', direction='inout', length=7)
    pl.tick_params(axis='x', which='minor', direction='in', length=0)
    ax.grid(which='minor', axis='x', ls='-', color='0.', lw=0.2)
    ax.grid(which='major', axis='x', ls='-', color='0.', lw=0.4)
    ax.grid(which='minor', axis='y', ls='-', color='0.', lw=0.2)
    ax.grid(which='major', axis='y', ls='')

    pl.setp(pl.getp(ax, 'xticklabels'), rotation=35)
    pl.setp(pl.getp(ax, 'yticklabels'), fontsize=8)
    ax.set_ylim(0, n_sites)
    pl.tight_layout()

    return txt_calmap, fig


def download_database(sites='all', dry_run=True, force=False):
    '''
    Download all available files for the requested BSRN sites

    Inputs:
    -------
    sites: list or `all`
      list of sites to download. If `all`, all sites are downloaded
    dry_run: bool
      check the files to download, but they are not downloaded
    force: bool
      If False, the local list of available sites in the remote server
      is used. If True, the remote server is inspected and the local
      list of available files remotely is updated

    Returns:
    --------
    Nothing. It just download the requested files
    '''

    bsrn_config = config.load()

    def local_file_exists(site, year, month):
        fn = resources.bsrn_filename(site, year, month)
        return bsrn_config['localdir'].joinpath(fn).exists()

    # first, update the list of existing remote files and sites metadata
    resources.update_list_of_remote_files(force)
    resources.update_site_metadata_from_bsrn_web_site(force)

    PROGRESS_BAR_AVAILABLE = True
    try:
        from tqdm import tqdm
    except ImportError:
        PROGRESS_BAR_AVAILABLE = False

    available_sites = availability()
    if sites != 'all':
        available_sites = {
            k: v for k, v in available_sites.items() if k in sites
        }

    files_to_download = []
    for site in available_sites:
        for year in available_sites[site]:
            for month in available_sites[site][year]:
                if not local_file_exists(site, year, month):
                    files_to_download.append((site, year, month))

    if dry_run is True:
        for args in files_to_download:
            logger.info('(DRY RUN) Downloading file {}'.format(
                resources.bsrn_filename(*args)))
        logger.info('(DRY RUN) {} files downloaded'
                    .format(len(files_to_download)))
        return

    try:
        workers = mp.Pool(mp.cpu_count())

        if PROGRESS_BAR_AVAILABLE:

            n_files = len(files_to_download)

            pbar = tqdm(total=n_files, unit='files', unit_scale=1)

            download = workers.starmap_async(
                resources.bsrn_download, files_to_download, chunksize=1)

            while not download.ready():
                tasks_completed = n_files - download._number_left
                pbar.update(int(tasks_completed - pbar.n))

            if pbar.n < n_files:
                pbar.update(int(n_files - pbar.n))

        else:
            download = workers.starmap_async(
                resources.bsrn_download, files_to_download, chunksize=1)

            while not download.ready():
                pass

    except Exception as exc:
        raise exc

    finally:
        if PROGRESS_BAR_AVAILABLE:
            pbar.close()
        workers.close()
        workers.join()


def load_data(site, years, months=range(1, 13), full_output=False,
              timeout=None, enable_time_centering=True,
              check_remote_server_on_missing_file=True):
    '''
    Load the data available at a BSRN' site for a list of years and months

    THE DATA TIMESTAMP IS CENTERED!!!
    '''

    bsrn_config = config.load()

    if is_sequence(years) or is_sequence(months):

        loy = [years] if isinstance(years, (int, float)) else years
        lom = [months] if isinstance(months, (int, float)) else months
        list_of_years_and_months = sorted(itertools.product(loy, lom))

        if full_output is True:
            logger.warning(
                'full_output can only be True on retrievals of a single '
                'month. It is set to False for this retrieval'
            )

        try:
            tasks = []
            for year, month in list_of_years_and_months:
                tasks.append(
                    (
                        site, year, month,
                        False, timeout, enable_time_centering,
                        check_remote_server_on_missing_file
                    )
                )

            workers = mp.Pool(mp.cpu_count())
            run = workers.starmap_async(load_data, tasks, chunksize=1)

            while not run.ready():
                pass

        except Exception as exc:
            raise exc

        finally:
            workers.close()
            workers.join()

        retrieval = run.get()  # potentially unordered data!!

        # remove erroneous or missing retrievals
        if len(list_of_years_and_months) > 1:
            logger.info('appending monthly files...')

        retrieval = [retr for retr in retrieval if retr is not None]
        retrieval = [retr for retr in retrieval if 'values' in retr]
        if not retrieval:
            logger.warning('missing data')
            return

        # sort the retrievals chronologically
        retrieval = sorted(
            retrieval, key=lambda retr: (retr['year'], retr['month']))
        # and concatenate them...
        data = retrieval[0]
        data.pop('year')
        data.pop('month')

        def check_item(item):
            value = data.setdefault(item, None)
            if value is None:
                # if value is None, search all the retrievals and
                # take the first value that is not None
                values = list(
                    filter(
                        None, [retr.get(item, None) for retr in retrieval]
                    )
                )  # use filter to remove None's
                if values:
                    data[item] = values[0]

        check_item('latitude')
        check_item('longitude')
        check_item('location')
        check_item('country')
        check_item('elevation')
        check_item('horizon_azimuth')
        check_item('horizon_elevation')
        check_item('surface_type')
        check_item('topography_type')
        check_item('network')

        data['values'] = pd.concat([retr['values'] for retr in retrieval])

        # guess time series resolution...
        try:
            # seconds
            data['resolution'] = guess_time_resolution(data['values']).seconds
        except Exception as exc:
            pass

        metadata = data.copy()
        data = metadata.pop('values')
        return data, metadata  # output port!!

    else:

        year = years
        month = months

        localdir = bsrn_config['localdir']

        fn = resources.bsrn_filename(site, year, month, 'dat.gz')
        local_fn = localdir.joinpath(site).joinpath(fn)
        relpath = os.path.relpath(local_fn.as_posix(), localdir)
        dirname = os.path.dirname(relpath)
        basename = os.path.basename(relpath)

        if (not local_fn.exists()) and check_remote_server_on_missing_file:
            try:
                resources.bsrn_download(site, year, month, timeout)
            except BSRNDownloadError:
                pass

        if not local_fn.exists():
            logger.info(
                styler.join(
                    f'missing file {local_fn.parent}{os.path.sep}',
                    styler.format(local_fn.name, 'yellow')
                )
            )
            return

        logger.info(
            styler.join(
                f'reading file {local_fn.parent}{os.path.sep}',
                styler.format(local_fn.name, 'green')
            )
        )

        with gzip.open(local_fn, 'rb') as gz:
            txt_data = [line.rstrip().decode('utf-8')
                        for line in gz.readlines()]

        logical_records = parser.find_logical_record_bounds(txt_data)
        # print(logical_records)

        contents = {}
        init_time = time.time()

        # READ METADATA...
        # All metadata must be read before reading the data records

        contents['metadata'] = {}

        for lrid in sorted(logical_records):

            if lrid not in parser.metadata_parsers:
                continue

            lrdesc = logical_records[lrid]
            first_line = lrdesc['first_line']
            last_line = lrdesc['last_line']

            lrcontents = {}

            lrparser = parser.metadata_parsers[lrid]
            logger.debug(
                colored(f'parsing logical record {lrid} for metadata', 'blue'))
            try:
                lrcontents = lrparser(txt_data[first_line:last_line + 1])
                for attr_name in lrcontents:
                    logger.debug(f'  - attribute `{attr_name}` retrieved')
            except Exception as exc:
                logger.debug(
                    'error parsing metadata logical record '
                    f'{lrid}: {exc.args[0]} [Skipping]')
            finally:
                contents['metadata'].update(lrcontents)

        # promote important metadata values (such as, longitude, latitude
        # and elevation) to the first level of the dictionary for easy access

        bsrn_sites_list = None
        if bsrn_sites_list is None:
            bsrn_sites_list = resources.read_site_metadata_file(force=False)

        if site not in bsrn_sites_list:
            bsrn_sites_list = resources.read_site_metadata_file(force=True)

        contents['site'] = site
        contents['year'] = year
        contents['month'] = month
        contents['location'] = bsrn_sites_list[site]['station']
        contents['country'] = bsrn_sites_list[site]['location']
        contents['latitude'] = bsrn_sites_list[site]['latitude']
        contents['longitude'] = bsrn_sites_list[site]['longitude']
        contents['elevation'] = bsrn_sites_list[site]['elevation']

        contents['horizon_azimuth'] = \
            np.array(contents['metadata'].get('horizon_azimuth', None))
        contents['horizon_elevation'] = \
            np.array(contents['metadata'].get('horizon_elevation', None))

        contents['surface_type'] = TableA4.get(
            contents['metadata'].get('surface_type', None), None)

        contents['topography_type'] = contents['metadata'].get(
            'topography_type',
            contents['metadata'].get('topograpy_type', None))
        contents['topography_type'] = TableA5.get(
            contents['topography_type'], contents['topography_type'])

        contents['network'] = 'BSRN'

        logger.debug(f'Site latitude: {contents["latitude"]:+.4f}N')
        logger.debug(f'Site longitude: {contents["longitude"]:+.4f}E')
        logger.debug(f'Site altitude: {contents["elevation"]:+.1f} m.a.s.l.')

        # READ DATA...

        contents['logical_records'] = {}

        for lrid in sorted(logical_records):

            if lrid not in parser.data_parsers:
                if int(lrid) > 99:
                    descr = LogicalRecordDescription.get(lrid, None)
                    msg = (f'Unavailable data parser for logical record '
                           f'with id {lrid}')
                    if descr is not None:
                        msg += f': {descr}'
                    logger.debug(msg)
                continue

            try:
                lrdesc = logical_records[lrid]
                first_line = lrdesc['first_line']
                last_line = lrdesc['last_line']

                # I add an entry for each logical record (lrid) because
                # the data for each logical record has independent times
                lrparser = parser.data_parsers[lrid]
                logger.debug(
                    colored(f'parsing logical record {lrid} for data', 'blue'))
                lrcontents = lrparser(txt_data[first_line:last_line + 1])

                lrcontents = {key: value for key, value in lrcontents.items()
                              if not np.all(np.isnan(value))}
                lrcontents['description'] = \
                    LogicalRecordDescription.get(lrid, 'unavailable')

                day = lrcontents.pop('day')
                hour = lrcontents.pop('hour')
                minute = lrcontents.pop('minute')
                kwargs = dict(second=0, microsecond=0, tzinfo=None)
                utc_times = [
                    datetime(year, month, day[k], hour[k], minute[k], **kwargs)
                    for k in range(len(day))]
                lrcontents['utc_times'] = np.array(utc_times)

                for attr_name in lrcontents:
                    logger.debug(f'  - attribute `{attr_name}` retrieved')

                contents['logical_records'][lrid] = {}
                contents['logical_records'][lrid].update(lrcontents)
            except ValueError as exc:
                # ERROR: CAM, 2008-05 (cam0508.dat.gz)
                #    there was a problem parsing logical record 0100
                #    hour greater than 23 at: (day=19, hour=24, minute=0)
                # It is exactly the same problem (and solution) than
                # CAM 2013-09 (see below)
                #
                # ERROR: CAM, 2008-10 (cam1008.dat.gz)
                #    there was a problem parsing logical record 0100
                #    hour greater than 23 at: (day=20, hour=24, minute=0)
                # It is exactly the same problem (and solution) than
                # CAM 2013-09 (see below)
                #
                # ERROR: CAM, 2009-08 (cam0809.dat.gz)
                #    there was a problem parsing logical record 0100
                #    hour greater than 23 at: (day=10, hour=24, minute=0)
                # It is exactly the same problem (and solution) than
                # CAM 2013-09 (see below)
                #
                # ERROR: CAM, 2009-11 (cam1109.dat.gz)
                #    there was a problem parsing logical record 0100
                #    hour greater than 23 at: (day=25, hour=24, minute=0)
                # It is exactly the same problem (and solution) than
                # CAM 2013-09 (see below)
                #
                # ERROR: CAM, 2013-07 (cam0713.dat.gz)
                # There are two errors in the time stamps in record C0100
                # for the 2nd day of the month. First, the 2nd minute is
                # missing:
                #   2    0      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    346 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   2    2      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    351 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                # I simply solve it by inserting a record of missings:
                #   2    0      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    346 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   2    1   -999 -99.9 -999 -999   -999 -99.9 -999 -999
                #            -999 -99.9 -999 -999   -999 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   2    2      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    351 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                # Second, the 2nd day ends with minute 1440, but the maximum
                # allowed minute is 1439, and the 3rd day starts with minute
                # 1, but the first minute should be 0. That is:
                #   2 1439      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    336 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   2 1440   -999 -99.9 -999 -999   -999 -99.9 -999 -999
                #            -999 -99.9 -999 -999   -999 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   3    1      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    337 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                # I simply solve it moving the record with day=2 and
                # minute=1440 to day=3 and minute=0. That is:
                #   2 1439      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    336 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   3    0   -999 -99.9 -999 -999   -999 -99.9 -999 -999
                #            -999 -99.9 -999 -999   -999 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #   3    1      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    337 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #
                # ERROR: CAM, 2013-09 (cam0913.dat.gz)
                # The 11th day ends with minute 1440, but the maximum
                # allowed is 1439, and the 12th day starts with minute 1,
                # but the first minute should be 0. That is:
                #  11 1439      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    388 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #  11 1440   -999 -99.9 -999 -999   -999 -99.9 -999 -999
                #            -999 -99.9 -999 -999   -999 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #  12    1      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    388 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                # I simply solve it moving the record with day=11 and
                # minute=1440 to day=12 and minute=0. That is:
                #  11 1439      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    388 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #  12    0   -999 -99.9 -999 -999   -999 -99.9 -999 -999
                #            -999 -99.9 -999 -999   -999 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #  12    1      0 -99.9 -999 -999      0 -99.9 -999 -999
                #               0 -99.9 -999 -999    388 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                #
                # ERROR: SMS, 2016-04 (sms0416.dat.gz)
                # From day 24 all days have an extra record "1440". That is:
                #  24 1440   -999 -99.9 -999 -999   -999 -99.9 -999 -999
                #            -999 -99.9 -999 -999   -999 -99.9 -999 -999\
                #    -99.9 -99.9 -999
                # I simply removed them from the file
                #
                # ERROR: SMS, 2016-05 (sms0516.dat.gz)
                # As previous error, but for day 1. I simply removed the
                # record from the file
                #
                # ERROR: SMS, 2016-10 (sms1016.dat.gz)
                # As previous error, but for days 3 and 4. I simply removed
                # the records from the file
                #
                # ERROR: SMS, 2016-11 (sms1116.dat.gz)
                # As previous error, but for day 9. I simply removed the
                # record from the file
                #
                # and many more...

                # The logical record 0100 is essential because it holds the
                # solar radiation data. Errors parsing it are not allowed!!
                if lrid in ('0100',):
                    logger.error(
                        'there was a problem parsing logical record 0100')
                    if max(hour) > 23:
                        msg = 'hour greater than 23 at: '
                        msg += ', '.join(
                            [f'(year={year}, month={month}, day={day[k]}, '
                             f'hour={hour[k]}, minute={minute[k]})'
                             for k in np.argwhere(hour > 23)[0]])
                        logger.error(msg)
                    raise exc
                logger.warning('there was a problem parsing logical '
                               f'record {lrid}. Skipping')

        for lrid in sorted(logical_records):
            if ((lrid not in parser.metadata_parsers) and
               (lrid not in parser.data_parsers)):
                logger.debug(f'missing parser for logical record {lrid}')

        # from IPython import embed; embed()

        # get rid of "stats" columns in logical record 0100 and pile up the
        # rest in a DataFrame structure
        if '0100' in contents['logical_records']:

            logger.debug(
                colored('parsing data in logical record 0100', 'blue'))

            def translate(varname):
                mapping = {
                    'global_horizontal': 'ghi',
                    'direct_normal': 'dni',
                    'diffuse_horizontal': 'dif',
                    'downward_longwave': 'dlw',
                    'air_temperature': 'tair',
                    'relative_humidity': 'rh',
                    'atmospheric_pressure': 'pressure'
                }
                for long_name, short_name in mapping.items():
                    if long_name in varname:
                        return varname.replace(long_name, short_name)
                return varname

            series = {}
            times_utc = contents['logical_records']['0100']['utc_times']

            for variable in contents['logical_records']['0100']:
                if variable.endswith('_min') or variable.endswith('_max'):
                    continue

                if variable in ('utc_times', 'description'):
                    continue

                logger.debug(
                    f'  - getting variable `{variable}`: '
                    f'{repr(contents["logical_records"]["0100"][variable])}')
                series[translate(variable)] = pd.Series(
                    data=contents['logical_records']['0100'][variable],
                    index=times_utc)

            if not series:
                logger.debug(colored('empty logical record 0100', 'red'))
                return None

            try:
                contents['values'] = pd.concat(series, axis=1)
            except Exception as exc:
                logger.debug(
                    'an exception has occurred while retrieving '
                    f'the logical record 0100: {exc}')
                return None

        elapsed_time = time.time() - init_time
        logger.debug(f'total elapsed time: {elapsed_time} seconds')

        if 'values' not in contents:
            return None

        metadata = contents.pop('metadata')
        logical_records = contents.pop('logical_records')

        with np.printoptions(threshold=5):
            logger.debug('Retrieved data:')
            for k, v in contents.items():
                if k == 'values':
                    logger.debug(f'  - {k}: \n{repr(v)}')
                    continue
                logger.debug(f'  - {k}: {repr(v)}')

        #####################################################################
        # THE TIMESTAMP OF THE DATAPOINT REPRESENTS THE STARTING POINT OF   #
        # THE 1-MIN AVERAGE (page 1493, Driemel et al., 2018,               #
        # doi: www.earth-syst-sci-data.net/10/1491/2018/                    #
        #####################################################################
        contents['timestamp_reference'] = 'start'

        if enable_time_centering is True:

            contents['values'].index = (
                contents['values'].index + pd.Timedelta(30, 'seconds')
            )

            # interpolation to be sure that the time grid is as it should
            t_s = contents['values'].index[0]
            t_e = contents['values'].index[-1]
            times_1min = pd.date_range(
                f'{t_s.year}-{t_s.month:02d}-01T00:00:30',
                f'{t_e.year}-{t_e.month:02d}-{t_e.daysinmonth}T23:59:30',
                freq=pd.Timedelta(60, 'seconds'))
            contents['values'] = time_interpolation(
                contents['values'], times_1min)

            contents['timestamp_reference'] = 'center'

        if 'timezone' not in contents:
            contents['timezone_region'] = (
                timezone := timezone_from_latlon(
                    contents['latitude'], contents['longitude'])
            )
            contents['timezone'] = timezone_utcoffset(timezone)
            contents['timezone_specs'] = timezone_as_string(timezone)

        if full_output is True:
            return contents, metadata, logical_records
        return contents
