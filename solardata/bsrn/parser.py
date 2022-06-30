
from __future__ import absolute_import, print_function, division

import re

from loguru import logger
import numpy as np

from .exception import BSRNParsingError
from .description_tables import TableA3


logger.disable(__name__)


def parse_float(x):
    try:
        return float(x)
    except Exception:
        return float('nan')


class Translate(object):
    def __init__(self, missing):
        self._missing = missing

    def __call__(self, value):
        return None if value == self._missing else value


def find_logical_record_bounds(txt_data):
    regex = re.compile(r'^\s*\*[CU]\d\d\d\d\s*$')
    logical_records_list = []
    for n, line in enumerate(txt_data):
        if regex.match(line):
            stamp = line.strip()
            # if stamp not in [lr[0] for lr in logical_records_list]:
            logical_records_list.append((stamp, n))

    logical_records_limits = {}
    for logical_record_stamp, first_line in logical_records_list:
        logical_record_id = logical_record_stamp[2:]
        if logical_record_id in logical_records_limits:
            continue
        logical_records_limits[logical_record_id] = {
            'header': logical_record_stamp,
            'has_changed': logical_record_stamp[1] == 'C',
            'first_line': first_line
        }

    for logical_record_id in logical_records_limits:
        k = 0
        while logical_records_list[k][0][2:] != logical_record_id:
            k += 1
        if k < len(logical_records_list) - 1:
            last_line = logical_records_list[k + 1][1] - 1
        else:
            last_line = len(txt_data)
        logical_records_limits[logical_record_id]['last_line'] = last_line

    # from IPython import embed; embed()
    # import sys; sys.exit()

    return logical_records_limits


def parse(txt, pattern, logical_record_id, formatter=None, missing=None):
    regex = re.compile(pattern)
    m = regex.match(txt)
    if m is None:
        raise BSRNParsingError(
            'mismatch of regular expression {0} and line {1}'.format(
                pattern, '<empty line>' if txt == '' else txt))

    groups = map(lambda s: s.strip(), m.groups())

    if formatter is not None:
        groups = map(formatter, groups)

    if missing is not None:
        groups = map(Translate(missing), groups)

    return list(groups)


def parse_logical_record_0001(txt_data):
    contents = {}
    logical_record_id = '0001'

    # First data line
    pattern = ' (.{2}) (.{2}) (.{4}) (.{2})'
    m = parse(txt_data[1], pattern, logical_record_id, int)
    contents['station_id'] = m[0]
    contents['month'] = m[1]
    contents['year'] = m[2]
    contents['data_version'] = m[3]

    # Second and following data lines
    contents['quantity_measured'] = {}
    for line in txt_data[2:]:
        m = parse(line, ' (.{9})' * 8, logical_record_id, int)
        for qty_id in filter(lambda x: x != -1, m):
            contents['quantity_measured'][qty_id] = TableA3[qty_id]

    return contents


def parse_logical_record_0002(txt_data):
    contents = {}
    logical_record_id = '0002'

    # date when station scientist changed
    m = parse(txt_data[1], ' (.{2})' * 3, logical_record_id, int, -1)
    contents['scientist_changed_on'] = dict(zip(('day', 'hour', 'minute'), m))

    # name of station scientist
    m = parse(txt_data[2], '(.{38}) (.{20}) (.*)', logical_record_id)
    contents['scientist_name'] = m[0]
    contents['scientist_telephone'] = m[1]
    contents['scientist_fax'] = m[2]

    # station scientist tcp/ip and e-mail
    m = parse(txt_data[3], '(.{15}) (.*)', logical_record_id)
    contents['scientist_tcp/ip'] = m[0]
    contents['scientist_email'] = m[1]

    # station scientist address
    contents['scientist_address'] = txt_data[4].strip()

    # date when station deputy changed
    m = parse(txt_data[5], ' (.{2})' * 3, logical_record_id, int, -1)
    contents['deputy_changed_on'] = dict(zip(('day', 'hour', 'minute'), m))

    # name of station deputy
    m = parse(txt_data[6], '(.{38}) (.{20}) (.*)', logical_record_id)
    contents['deputy_name'] = m[0]
    contents['deputy_telephone'] = m[1]
    contents['deputy_fax'] = m[2]

    # tcp/ip e-mail
    m = parse(txt_data[7], '(.{15}) (.*)', logical_record_id)
    contents['deputy_tcp/ip'] = m[0]
    contents['deputy_email'] = m[1]

    contents['deputy_address'] = txt_data[8].strip()

    return contents


def parse_logical_record_0003(txt_data):
    # logical_record_id = '0003'
    return {'message': txt_data[1].strip()}


def parse_logical_record_0004(txt_data):
    contents = {}
    logical_record_id = '0004'

    # date when station description changed
    m = parse(txt_data[1], ' (.{2})' * 3, logical_record_id, int, -1)
    contents['description_changed_on'] = dict(
        zip(('day', 'hour', 'minute'), m))

    # surface and topography type
    m = parse(txt_data[2], ' (.{2}) (.{2})', logical_record_id, int)
    contents['surface_type'] = m[0]
    contents['topograpy_type'] = m[1]

    # station address
    contents['station_address'] = txt_data[3].strip()

    # station telephone and fax
    m = parse(txt_data[4], '(.{20}) (.*)', logical_record_id)
    contents['station_telephone'] = m[0]
    contents['station_fax'] = m[1]

    # station tcp/ip and email
    m = parse(txt_data[5], '(.{15}) (.*)', logical_record_id)
    contents['station_tcp/ip'] = m[0]
    contents['station_email'] = m[1]

    # geographical location
    m = parse(txt_data[6], ' (.{7}) (.{7}) (.{4}) (.*)', logical_record_id)
    contents['latitude'] = float(m[0]) - 90.
    contents['longitude'] = float(m[1]) - 180.
    contents['altitude'] = float(m[2])
    contents['synop_id'] = m[3].strip()

    # date when horizon changed
    m = parse(txt_data[7], ' (.{2})' * 3, logical_record_id, int, -1)
    contents['horizon_changed_on'] = dict(zip(('day', 'hour', 'minute'), m))

    # horizon numerical description
    horizon_azimuth = []
    horizon_elevation = []
    for line in txt_data[8:]:
        m = parse(line, ' (.{3}) (.{2})' * 11, logical_record_id, int)
        m = list(filter(lambda x: x != -1, m))
        horizon_azimuth.extend(m[0::2])
        horizon_elevation.extend(m[1::2])
    contents['horizon_azimuth'] = horizon_azimuth
    contents['horizon_elevation'] = horizon_elevation

    return contents


def parse_logical_record_0005(txt_data):
    contents = {}
    logical_record_id = '0005'

    if len(txt_data) != 3:
        raise BSRNParsingError(
            'expected 3 input lines. Got %d lines' % len(txt_data))

    # date when change occurred
    m = parse(txt_data[1], ' (.{2}) (.{2}) (.{2}) (.)', logical_record_id)
    contents['radiosonde_changed_on'] = dict(
        zip(('day', 'hour', 'minute'), map(Translate(-1), map(int, m[:3]))))
    contents['radiosonde_operating'] = True if m[3] == 'Y' else False

    # manufacturer, location, distance from site, launch times, radiosonde id
    pattern = '(.{30}) (.{25}) (.{3}) (.{2}) (.{2}) (.{2}) (.{2}) (.*)'
    m = parse(txt_data[2], pattern, logical_record_id)
    contents['radiosonde_manufacturer'] = m[0]
    contents['radiosonde_location'] = m[1]
    contents['radiosonde_distance_km'] = int(m[2])
    contents['radiosonde_hUTC_1st_launch'] = Translate(-1)(int(m[3]))
    contents['radiosonde_hUTC_2nd_launch'] = Translate(-1)(int(m[4]))
    contents['radiosonde_hUTC_3rd_launch'] = Translate(-1)(int(m[5]))
    contents['radiosonde_hUTC_4th_launch'] = Translate(-1)(int(m[6]))
    contents['radiosonde_id'] = m[7]

    # additional remarks
    try:
        contents['radiosonde_remarks'] = txt_data[3].strip()
    except IndexError:
        contents['radiosonde_remarks'] = ''

    return contents


def parse_logical_record_0006(txt_data):
    contents = {}
    logical_record_id = '0006'

    if len(txt_data) != 3:
        raise BSRNParsingError(
            'expected 3 input lines. Got %d lines' % len(txt_data))

    # date when change occurred
    m = parse(txt_data[1], ' (.{2}) (.{2}) (.{2}) (.)', logical_record_id)
    contents['ozone_changed_on'] = dict(
        zip(('day', 'hour', 'minute'), map(Translate(-1), map(int, m[:3]))))
    contents['ozone_operating'] = True if m[3] == 'Y' else False

    # manufacturer, location, distance from site and ozone id
    m = parse(txt_data[2], '(.{30}) (.{25}) (.{3}) (.*)', logical_record_id)
    contents['ozone_manufacturer'] = m[0]
    contents['ozone_location'] = m[1]
    contents['ozone_distance_km'] = int(m[2])
    contents['ozone_id'] = m[3]

    # additional remarks
    contents['ozone_remarks'] = txt_data[3].strip()

    return contents


def parse_logical_record_0007(txt_data):
    contents = {}
    logical_record_id = '0007'

    # date when change occurred
    m = parse(txt_data[1], ' (.{2}) (.{2}) (.{2})', logical_record_id, int, -1)
    contents['station_history_changed_on'] = dict(
        zip(('day', 'hour', 'minute'), m))

    # method est. cloud amount (digital proc.)
    contents['station_history_cloud_amount'] = txt_data[2].strip()

    # method est. cloud base height (with instrument)
    contents['station_history_cloud_base_height'] = txt_data[3].strip()

    # method est. cloud liquid water content
    contents['station_history_cloud_liquid_water_content'] = \
        txt_data[4].strip()

    # method est. cloud aerosol vertical distribution
    contents['station_history_aerosol_vertical_distribution'] = \
        txt_data[5].strip()

    # method est. water vapor press
    contents['station_history_water_vapor_pressure'] = txt_data[6].strip()

    # 6 flags indicating if the SYNOP and/or the corresponding
    # quantities of the expanded programme are measured
    m = parse(txt_data[7], ' '.join(['(.)'] * 6), logical_record_id,
              lambda flag: True if flag.lower() == 'y' else False)
    contents['station_history_synop_flags'] = m

    return contents


def parse_logical_record_0008(txt_data):
    contents = {}
    logical_record_id = '0008'

    nline = 1
    n_instrument = 1

    while nline < len(txt_data):
        radinstr = {}
        logger.debug('parsing radiation instrument %s', n_instrument)
        # date when change occurred
        pattern = ' (.{2}) (.{2}) (.{2}) (.)'
        m = parse(txt_data[nline], pattern, logical_record_id)
        radinstr['changed_on'] = dict(
            zip(('day', 'hour', 'minute'),
                map(Translate(-1), map(int, m[:3]))))
        radinstr['operating'] = True if m[3] == 'Y' else False

        # manufacturer, model, serial number, purchase date and WRMC id number
        nline += 1
        pattern = '(.{30}) (.{15}) (.{18}) (.{8}) (.*)'
        m = parse(txt_data[nline], pattern, logical_record_id)
        radinstr['manufacturer'] = m[0]
        radinstr['model'] = m[1]
        radinstr['serial_number'] = m[2]
        radinstr['purchase_date'] = m[3]
        radinstr['wrmc_id'] = int(m[4])

        # additional remarks
        nline += 1
        radinstr['remarks'] = txt_data[nline].strip()

        # instrument and calibration technical details
        nline += 1
        pattern = 2 * ' (.{2})' + 6 * ' (.{7})' + 2 * ' (.{2})'
        m = parse(txt_data[nline], pattern, logical_record_id)
        radinstr['pyrgeometer_body_compensation_code'] = \
            Translate(-1)(int(m[0]))
        radinstr['pyrgeometer_dome_compensation_code'] = \
            Translate(-1)(int(m[1]))
        radinstr['wavelength_of_band_1'] = Translate(-1.)(float(m[2]))
        radinstr['bandwidth_of_band_1'] = Translate(-1.)(float(m[3]))
        radinstr['wavelength_of_band_2'] = Translate(-1.)(float(m[4]))
        radinstr['bandwidth_of_band_2'] = Translate(-1.)(float(m[5]))
        radinstr['wavelength_of_band_3'] = Translate(-1.)(float(m[6]))
        radinstr['bandwidth_of_band_3'] = Translate(-1.)(float(m[7]))
        radinstr['max_xx_zenith_angle_direct_degrees'] = \
            Translate(-1)(int(m[8]))
        radinstr['min_xx_spectral_instrument'] = Translate(-1)(int(m[9]))

        nline += 1
        m = parse(txt_data[nline], '(.{30}) (.*)', logical_record_id)
        radinstr['location_of_calibration'] = m[0]
        radinstr['person_doing_calibration'] = m[1]

        nline += 1
        pattern = '(.{8}) (.{8}) (.{2}) (.{12}) (.{12})'
        m = parse(txt_data[nline], pattern, logical_record_id)
        radinstr['start_of_calibration_period_of_band_1'] = m[0]
        radinstr['end_of_calibration_period_of_band_1'] = m[1]
        radinstr['number_of_comparisons_of_band_1'] = \
            Translate(-1)(int(m[2]))
        radinstr['mean_calibration_coefficient_of_band_1'] = \
            Translate(-1.)(float(m[3]))
        radinstr['standard_error_of_calibration_coefficient_of_band_1'] = \
            Translate(-1.)(float(m[4]))

        nline += 1
        pattern = '(.{8}) (.{8}) (.{2}) (.{12}) (.{12})'
        m = parse(txt_data[nline], pattern, logical_record_id)
        radinstr['start_of_calibration_period_of_band_2'] = m[0]
        radinstr['end_of_calibration_period_of_band_2'] = m[1]
        radinstr['number_of_comparisons_of_band_2'] = \
            Translate(-1)(int(m[2]))
        radinstr['mean_calibration_coefficient_of_band_2'] = \
            Translate(-1.)(float(m[3]))
        radinstr['standard_error_of_calibration_coefficient_of_band_2'] = \
            Translate(-1.)(float(m[4]))

        nline += 1
        pattern = '(.{8}) (.{8}) (.{2}) (.{12}) (.{12})'
        m = parse(txt_data[nline], pattern, logical_record_id)
        radinstr['start_of_calibration_period_of_band_3'] = m[0]
        radinstr['end_of_calibration_period_of_band_3'] = m[1]
        radinstr['number_of_comparisons_of_band_3'] = Translate(-1)(int(m[2]))
        radinstr['mean_calibration_coefficient_of_band_3'] = \
            Translate(-1.)(float(m[3]))
        radinstr['standard_error_of_calibration_coefficient_of_band_3'] = \
            Translate(-1.)(float(m[4]))

        nline += 1
        radinstr['calibration_remarks'] = [txt_data[nline].strip()]

        nline += 1
        radinstr['calibration_remarks'].append(txt_data[nline].strip())

        radinstr_key = 'radiation_instrument_{0}'.format(radinstr['wrmc_id'])
        contents[radinstr_key] = {}
        contents[radinstr_key].update(radinstr)

        nline += 1
        n_instrument += 1

    return contents


def parse_logical_record_0009(txt_data):
    contents = {}
    logical_record_id = '0009'

    nline = 1

    contents['measurements'] = []

    attributes = ('day', 'hour', 'minute', 'id_qty', 'id_instr', 'spctr_band')
    pattern = ' (.{2}) (.{2}) (.{2}) (.{9}) (.{5}) (.{2})'
    while nline < len(txt_data):
        m = parse(txt_data[nline], pattern, logical_record_id, int, -1)
        contents['measurements'].append(
            dict(zip(attributes, m)))
        nline += 1

    return contents


def parse_logical_record_0100(txt_data):
    # logical_record_id = '0100'
    contents = {}

    nline = 0

    line_data_1 = []
    line_data_2 = []
    while nline < len(txt_data) - 1:
        try:
            nline += 1
            line_values_1 = [parse_float(e) for e in txt_data[nline].split()]
            if len(line_values_1) != 10:
                raise ValueError()

            nline += 1
            line_values_2 = [parse_float(e) for e in txt_data[nline].split()]
            if len(line_values_2) != 11:
                raise ValueError()

            line_data_1.append(line_values_1)
            line_data_2.append(line_values_2)
        except ValueError:
            logger.warning(
                'Parser of logical record 0100: at line number %d, '
                'skipping bad line \'%s\'', nline, txt_data[nline])

    variable_names = (
        'day', 'minute',
        'global_horizontal', 'global_horizontal_std', 'global_horizontal_min',
        'global_horizontal_max', 'direct_normal', 'direct_normal_std',
        'direct_normal_min', 'direct_normal_max')

    for varname, values in zip(variable_names, np.stack(line_data_1).T):
        contents[varname] = values

    contents['day'] = contents['day'].astype(int)
    contents['minute'] = contents['minute'].astype(int)

    variable_names = (
        'diffuse_horizontal', 'diffuse_horizontal_std',
        'diffuse_horizontal_min', 'diffuse_horizontal_max',
        'downward_longwave', 'downward_longwave_std',
        'downward_longwave_min', 'downward_longwave_max', 'air_temperature',
        'relative_humidity', 'atmospheric_pressure')

    for varname, values in zip(variable_names, np.stack(line_data_2).T):
        contents[varname] = values

    hour, minute = np.divmod(contents['minute'], 60)
    contents['hour'] = hour
    contents['minute'] = minute

    for variable in ('global_horizontal', 'global_horizontal_min',
                     'global_horizontal_max', 'direct_normal',
                     'direct_normal_min', 'direct_normal_max',
                     'diffuse_horizontal', 'diffuse_horizontal_min',
                     'diffuse_horizontal_max', 'downward_longwave',
                     'downward_longwave_min', 'downward_longwave_max',
                     'atmospheric_pressure'):
        contents[variable][contents[variable] == -999] = np.nan

    for variable in ('global_horizontal_std', 'direct_normal_std',
                     'diffuse_horizontal_std', 'downward_longwave_std',
                     'air_temperature', 'relative_humidity'):
        contents[variable][contents[variable] == -99.9] = np.nan

    return contents


def parse_logical_record_0300(txt_data):
    # logical_record_id = '0300'
    contents = {}

    nline = 0

    line_data = []
    while nline < len(txt_data) - 1:
        nline += 1
        try:
            line_values = [parse_float(e) for e in txt_data[nline].split()]
            if len(line_values) != 14:
                raise ValueError()
            line_data.append(line_values)
        except ValueError:
            logger.warning(
                'Parser of logical record 0300: at line number %d, '
                'skipping bad line \'%s\'', nline, txt_data[nline])

    variable_names = (
        'day', 'minute',
        'reflected', 'reflected_std', 'reflected_min', 'reflected_max',
        'upward_longwave', 'upward_longwave_std', 'upward_longwave_min',
        'upward_longwave_max', 'net_radiation', 'net_radiation_std',
        'net_radiation_min', 'net_radiation_max')

    for varname, values in zip(variable_names, np.stack(line_data).T):
        contents[varname] = values

    contents['day'] = contents['day'].astype(int)
    contents['minute'] = contents['minute'].astype(int)

    hour, minute = np.divmod(contents['minute'], 60)
    contents['hour'] = hour
    contents['minute'] = minute

    for variable in variable_names[2:]:
        universe = contents[variable] == -999
        contents[variable][universe] = np.nan

    for variable in ('reflected_std', 'upward_longwave_std',
                     'net_radiation_std'):
        universe = contents[variable] == -99.9
        contents[variable][universe] = np.nan

    # from IPython import embed; embed()
    return contents


def parse_logical_record_1100(txt_data):
    # logical_record_id = '1100'
    contents = {}

    nline = 0

    line_data = []
    while nline < len(txt_data) - 1:
        nline += 1
        try:
            line_values = [parse_float(e) for e in txt_data[nline].split()]
            if len(line_values) != 10:
                raise ValueError()
            line_data.append(line_values)
        except ValueError:
            logger.warning(
                'Parser of logical record 1100: at line number %d, '
                'skipping bad line \'%s\'', nline, txt_data[nline])

    variable_names = (
        'day', 'minute',
        'level', 'pressure_hPa', 'height_m', 'air_temperature_degC',
        'dew_point_degC', 'wind_direction_azimuth', 'wind_speed_ms',
        'ozone_concentration')

    for varname, values in zip(variable_names, np.stack(line_data).T):
        contents[varname] = values

    contents['day'] = contents['day'].astype(int)
    contents['minute'] = contents['minute'].astype(int)
    contents['level'] = contents['level'].astype(int)

    hour, minute = np.divmod(contents['minute'], 60)
    contents['hour'] = hour
    contents['minute'] = minute

    for variable in ('pressure_hPa', 'height_m'):
        universe = contents[variable] == -999
        contents[variable][universe] = np.nan

    for variable in ('wind_direction_azimuth', 'wind_speed_ms'):
        universe = contents[variable] == -99
        contents[variable][universe] = np.nan

    for variable in ('air_temperature_degC',):
        universe = contents[variable] == -99.9
        contents[variable][universe] = np.nan

    for variable in ('dew_point_degC',):
        universe = contents[variable] == -999.9
        contents[variable][universe] = np.nan

    for variable in ('ozone_concentration',):
        universe = contents[variable] == -9.9
        contents[variable][universe] = np.nan

    return contents


metadata_parsers = {
    # basic station and data information, including measured quantities
    '0001': parse_logical_record_0001,
    # station scientist and deputy contact data
    '0002': parse_logical_record_0002,
    # ancillary messages
    '0003': parse_logical_record_0003,
    # station horizon description
    '0004': parse_logical_record_0004,
    # radiosonde equipment
    '0005': parse_logical_record_0005,
    # ozone measurement equipment
    '0006': parse_logical_record_0006,
    # station history
    '0007': parse_logical_record_0007,
    # radiation instruments
    '0008': parse_logical_record_0008,
    # assignment of radiation quantities to instruments
    '0009': parse_logical_record_0009
}

data_parsers = {
    # expanded measurements
    '0100': parse_logical_record_0100,
    # other measurements in minutes intervals
    '0300': parse_logical_record_0300,
    # radiosonde measurements in launch intervals
    '1100': parse_logical_record_1100,
}
