try:
    from .version import __version__  # noqa
except ImportError:
    pass

from . import bsrn  # noqa
