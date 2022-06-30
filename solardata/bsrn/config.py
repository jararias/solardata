
from __future__ import absolute_import, print_function, division

import yaml
import importlib_resources
from pathlib import Path
from loguru import logger

from .exception import IOConfigError


logger.disable(__name__)


def read():
    root_dir = importlib_resources.files('solardata')
    config_fn = root_dir.joinpath('bsrn/bsrn.yml')

    if not config_fn.exists():
        raise IOConfigError(
            f'missing configuration file {config_fn.as_posix()}')

    with config_fn.open('r') as f:
        bsrn_conf = yaml.load(f.read(), yaml.FullLoader)

    return bsrn_conf, config_fn


def load(conf=None):
    bsrn_conf, config_fn = read() if conf is None else (conf, None)

    if config_fn is not None:
        logger.info(f'Configuration file: {config_fn}')

    if bsrn_conf.get('localdir', None) is None:
        raise IOConfigError('missing option `localdir` in config file')

    bsrn_conf['localdir'] = Path(bsrn_conf.get('localdir'))
    if not bsrn_conf.get('localdir').exists():
        bsrn_conf.get('localdir').mkdir(parents=True)
        
    bsrn_conf.setdefault('server', 'ftp.bsrn.awi.de')
    # bsrn_conf.setdefault('init_year', 1992)

    return bsrn_conf


def set_localdir(localdir):
    bsrn_conf, config_fn = read()
    
    bsrn_conf['localdir'] = localdir
    
    with config_fn.open('w') as f:
        f.write(yaml.dump(bsrn_conf))