
LogicalRecordDescription = {
    '0001': 'basic information',
    '0002': 'scientist',
    '0003': 'messages',
    '0004': 'station location and horizon',
    '0005': 'radiosonde equipment',
    '0006': 'ozone measurement equipment',
    '0007': 'station history',
    '0008': 'radiation instruments',
    '0009': 'assignment of radiation quantities to instruments',
    '0100': 'basic measurements',
    '0200': 'expanded measurements',
    '0300': 'other measurements in minute intervals',
    '0400': 'special spectral measurements',
    '0500': 'ultra-violet measurements',
    '1000': 'surface SYNOP measurements',
    '1100': 'radiosonde measurements in launch intervals',
    '1200': 'ozone measurements in hour intervals',
    '1300': 'expanded measurements in hour intervals',
    '1500': 'other measurements in hout intervals',
}


# Table A3. Quantity measured
# Every radiation value is measured by exactly one radiation instrument.
# If a value in height is missing, the quantity is measured only once at
# standard height. The id. no. of instruments not measured at standard
# height consists of the id. no. measured at standard height followed by
# 6 numericals expressing the height of the instruments above ground in cm
TableA3 = {
    2: {'description': 'global 2 (pyranometer)', 'unit': 'Wm-2'},
    3: {'description': 'direct', 'unit': 'Wm-2'},
    4: {'description': 'diffuse sky', 'unit': 'Wm-2'},
    5: {'description': 'long-wave downward', 'unit': 'Wm-2'},
    21: {'description': 'air temperature', 'unit': 'degC'},
    22: {'description': 'relative humidity', 'unit': '%'},
    23: {'description': 'pressure', 'unit': 'hPa'},
    121: {'description': 'uv-a-global', 'unit': 'Wm-2'},
    122: {'description': 'uv-b-direct', 'unit': 'Wm-2'},
    123: {'description': 'uv-b-global', 'unit': 'Wm-2'},
    124: {'description': 'uv-b-diffuse', 'unit': 'Wm-2'},
    125: {'description': 'uv-b-reflected', 'unit': 'Wm-2'},
    131: {'description': 'short-wave reflected', 'unit': 'Wm-2'},
    132: {'description': 'long-wave upward', 'unit': 'Wm-2'},
    141: {'description': 'net radiation (net radiometer)', 'unit': 'Wm-2'},
    104: {'description': 'short-wave spectral band 1', 'unit': ''},
    112: {'description': 'short-wave spectral band 3', 'unit': ''},
    301: {'description': 'total cloud amount with instrument', 'unit': '%'},
    302: {'description': 'cloud base height with instrument', 'unit': 'm'},
    303: {'description': 'cloud liquid water', 'unit': 'mm'},
    # height is in cm
    2000700: {
        'description': 'global 2 (pyranometer)',
        'unit': 'Wm-2', 'heigh': 700},
    2001000: {
        'description': 'global 2 (pyranometer)',
        'unit': 'Wm-2', 'heigh': 1000},
    131000700: {
        'description': 'short-wave reflected',
        'unit': 'Wm-2', 'heigh': 700},
    131001000: {
        'description': 'short-wave reflected',
        'unit': 'Wm-2', 'heigh': 1000},
    132000700: {
        'description': 'long-wave upward', 'unit': 'Wm-2', 'heigh': 700},
    132001000: {
        'description': 'long-wave upward', 'unit': 'Wm-2', 'heigh': 1000},
    5000700: {
        'description': 'long-wave downward', 'unit': 'Wm-2', 'heigh': 700},
    5001000: {
        'description': 'long-wave downward', 'unit': 'Wm-2', 'heigh': 1000},
    21000700: {
        'description': 'air temperature', 'unit': 'degC', 'heigh': 700},
    21001000: {
        'description': 'air temperature', 'unit': 'degC', 'heigh': 1000},
    22000700: {
        'description': 'relative humidity', 'unit': '%', 'heigh': 700},
    22001000: {
        'description': 'relative humidity', 'unit': '%', 'heigh': 1000},
    131003000: {
        'description': 'short-wave reflected', 'unit': 'Wm-2', 'heigh': 3000},
    132003000: {
        'description': 'long-wave upward', 'unit': 'Wm-2', 'heigh': 3000},
}

# Table A4. Types of surface
TableA4 = {
    1: 'glacier, accumulation area',
    2: 'glacier, ablation area',
    3: 'iceshelf',
    4: 'sea ice',
    5: 'water, river',
    6: 'water, lake',
    7: 'water, ocean',
    8: 'desert, rock',
    9: 'desert, sand',
    10: 'desert, gravel',
    11: 'concrete',
    12: 'asphalt',
    13: 'cultivated',
    14: 'tundra',
    15: 'grass',
    16: 'shrub',
    17: 'forest, evergreen',
    18: 'forest, deciduous',
    19: 'forest, mixed',
    20: 'rock',
    21: 'sand'
}

# Table A5. Types of topography
TableA5 = {
    1: 'flat, urban',
    2: 'flat, rural',
    3: 'hilly, urban',
    4: 'hilly, rural',
    5: 'mountain top, urban',
    6: 'mountain top, rural',
    7: 'mountain valley, urban',
    8: 'mountain valley, rural'
}
