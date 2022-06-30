### Usage pattern for BSRN and NOAA data:

```python
from solardata import noaa

# if logging is required...
from loguru import logger

logger.enable('solardata.noaa')

# set the local datatabase in a custom location. This is only
# needed once. The change persists until a new set is invoked
# and while the library is not re-installed or updated
noaa.config.set_localdir('/home/jararias/NOAA')

# to retrieve a month site...
data, metadata = noaa.load_data(<site>, <year>, <month>)
```

With this retrieval is pretty easy to create a SolarDataFrame:

```python
import solarpandas as sp
sdf = sp.SolarDataFrame(data=data, metadata=metadata)
```

To simply download data to the local database:

```python
noaa.download_database(<site>, <start_year>)
```

### Usage pattern for AERONET:

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