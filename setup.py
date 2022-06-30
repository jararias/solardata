import os  # noqa
from setuptools import setup

version = '0.1'

with open('solardata/_version.py', 'w') as f:
    f.write(f'__version__ = "{version}"\n')

setup(
    name='solardata',
    version=version,
    author='Jose A Ruiz-Arias',
    author_email='jararias@uma.es',
    url='',
    description='Retrieval tools of solar-related data',
    # long_description=read_content('README.md'),
    # long_description_content_type='text/markdown',
    keywords=["solar radiation", "observation data"],
    classifiers=[
        "Natural Language :: English",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.9",
        "Development Status :: 4 - Beta"
    ],
    packages=[
        'solardata',
        'solardata.bsrn',
        'solardata.noaa',
        'solardata.aeronet'
    ],
    package_data={
        'solardata': [
            'bsrn/bsrn.yml',
            'noaa/noaa.yml',
            'noaa/site_inventory.yml',
            'aeronet/aeronet.yml'
        ]
    },
    install_requires=[
        'pyyaml',
        'pytz',
        'loguru',
        'requests',
        'numpy',
        'pandas',
        'termcolor',
        'matplotlib',
        'openpyxl',
        'timezonefinder',
        'bs4',
        'tqdm',
    ],
    python_requires=">=3.6"
)

if os.path.exists('solardata/_version.py'):
    os.remove('solardata/_version.py')
