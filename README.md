# solardata

Data download for solar research applications

#### Installation

```
python3 -m pip install git+https://git@github.com/jararias/solardata#egg=solardata
```

#### Usage pattern for NOAA data

By default, the data is downloaded to

```bash
$HOME/.solardata/NOAA
```

It can be checked as follows:

```python
from solardata import noaa

print(noaa.config.load().get('localdir'))
```

To download the data to a different location, the default location can be overwritten with a symbolic link that points to the target location, or it can be changed as follows:

```python

noaa.config.set_localdir('/different/custom/location')
```

This change persists until the package is re-installed. Alternatively, one can set the target location editing the file _$INSTALL_DIR/solardata/noaa/noaa.yml_.

A common pattern of use might be:

```python
from solardata import noaa

# if logging is required...
from loguru import logger
logger.enable('solardata.noaa')

# to get a list of known sites...
known_sites = noaa.sites()

# to get metadata of the known sites...
metadata = noaa.sites_metadata()

# to retrieve data and metadata for a known site...
data, metadata = noaa.load_data(<site>, <list_of_years>, <list_of_months>)
```

If the data is not already in local, they are downloaded and archived locally, for later use.

From this data, it is straightforward to construct a SolarDataFrame (see [solarpandas](https://github.com/jararias/solarpandas) for further references):

```python
import solarpandas as sp
sdf = sp.SolarDataFrame(data=data, metadata=metadata)
```

#### Usage pattern for BSRN data

All as for NOAA data, with little modifications. For instance, the default download directory is

```bash
$HOME/.solardata/BSRN
```

and the package import is:

```python
from solardata import bsrn
```

Also, the retrieval of data from the BSRN server requires user and password, that must be provided via [.netrc](https://docs.python.org/3/library/netrc.html)


#### Usage pattern for AERONET:

As for NOAA and BSRN with

```python
from solardata import aeronet
```