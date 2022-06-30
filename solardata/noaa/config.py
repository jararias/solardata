
from __future__ import absolute_import, print_function, division

from pathlib import Path

import yaml
import importlib_resources
from loguru import logger

from .exception import IOConfigError


logger.disable(__name__)


def read():
    root_dir = importlib_resources.files('solardata')
    config_fn = root_dir.joinpath('noaa/noaa.yml')

    if not config_fn.exists():
        raise IOConfigError(
            f'missing configuration file {config_fn.as_posix()}')

    with config_fn.open('r') as f:
        noaa_conf = yaml.load(f.read(), yaml.FullLoader)

    return noaa_conf, config_fn


def load(conf=None):
    noaa_conf, config_fn = read() if conf is None else (conf, None)

    if config_fn is not None:
        logger.info(f'Configuration file: {config_fn}')

    if noaa_conf.get('localdir', None) is None:
        raise IOConfigError('missing option `localdir` in config file')

    noaa_conf['localdir'] = Path(noaa_conf.get('localdir'))
    if not noaa_conf.get('localdir').exists():
        noaa_conf.get('localdir').mkdir(parents=True)

    noaa_conf.setdefault('server', 'ftp.bsrn.awi.de')

    return noaa_conf


def set_localdir(localdir):
    noaa_conf, config_fn = read()

    noaa_conf['localdir'] = localdir

    with config_fn.open('w') as f:
        f.write(yaml.dump(noaa_conf))