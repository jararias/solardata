# solardata
### Data download for solar research applications

#### Installation

```
python3 -m pip install git+https://git@github.com/jararias/solardata#egg=solardata
```

#### Usage pattern for NOAA data:

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

If the data is already in local, they are downloaded.


With this retrieval is pretty easy to create a SolarDataFrame:

```python
# set the local datatabase in a custom location. This is only
# needed once. The change persists until a new set is invoked
# and while the library is not re-installed or updated
noaa.config.set_localdir('/home/jararias/NOAA')
```

```python
import solarpandas as sp
sdf = sp.SolarDataFrame(data=data, metadata=metadata)
```

To show the current local database location:

```python
noaa.config.load()
```

To simply download data to the local database:

```python
noaa.download_database(<site>, <start_year>)
```

#### Usage pattern for AERONET:

```python
from solardata import aeronet

# if logging is required...
from loguru import logger

logger.enable('solardata.aeronet')

# set the local datatabase in a custom location. This is only
# needed once. The change persists until a new set is invoked
# and while the library is not re-installed or updated
aeronet.config.set_localdir('/home/jararias/AERONET')

# to retrieve a month site...
data, metadata = aeronet.load_data(<site>, <year>, <month>, ...)
```
