
import os
from pathlib import Path

import yaml
import importlib_resources
from loguru import logger

from .exception import IOConfigError


logger.disable(__name__)


yaml.SafeLoader.add_constructor(
    u'!environ', lambda loader, node: loader.construct_scalar(node).format(**os.environ)
)


def read():
    root_dir = importlib_resources.files('solardata')
    config_fn = root_dir.joinpath('aeronet/aeronet.yml')

    if not config_fn.exists():
        raise IOConfigError(
            f'missing configuration file {config_fn.as_posix()}')

    with config_fn.open('r') as f:
        aero_conf = yaml.safe_load(f.read())

    return aero_conf, config_fn


def load(conf=None):
    aero_conf, config_fn = read() if conf is None else (conf, None)

    if config_fn is not None:
        logger.info(f'Configuration file: {config_fn}')

    if aero_conf.get('localdir', None) is None:
        raise IOConfigError('missing option `localdir` in config file')

    aero_conf['localdir'] = Path(aero_conf.get('localdir'))
    if not aero_conf.get('localdir').exists():
        aero_conf.get('localdir').mkdir(parents=True)

    return aero_conf


def set_localdir(localdir):
    aero_conf, config_fn = read()

    aero_conf['localdir'] = localdir

    with config_fn.open('w') as f:
        f.write(yaml.dump(aero_conf))