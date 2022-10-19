import os
import sys

sys.path.append('.')
sys.path.append('..')

os.environ['TMPDIR'] = os.environ.get('OUTPUT_DATA')

import glob
import logging
import itertools
import numpy as np
import pandas as pd

# read in the raw data
def read(input_folder):
    # list all files in the folder
    all_files = glob.glob(input_folder + '/*')

    # check for _SUCCESS
    for filename in all_files:
        if filename.endswith('_SUCCESS'):
            continue
        response_time_series = pd.read_csv(os.environ.get('INPUT_DATA') + '/response_data.csv')
        cost_time_series = pd.read_csv(os.environ.get('INPUT_DATA') + '/cost_data.csv')

    return response_time_series, cost_time_series

# create a lookup file - geo as a name and as a number
def create_dictionary(response_time_series):
    df = response_time_series.loc[:, ['geo', 'geo_name']].drop_duplicates().reset_index(drop = True)
    return df

# join the data together and create the geo-level time series
def create_time_series(response_time_series, cost_time_series, geo_dictionary):

    # fix all the variable types
    response_time_series['date'] = pd.to_datetime(response_time_series['date'])
    cost_time_series['date'] = pd.to_datetime(cost_time_series['date'])

    # variable selection
    response_time_series = response_time_series.loc[:, ['geo', 'date', 'response']]
    cost_time_series = cost_time_series.loc[:, ['geo', 'date', 'cost']]

    # create the full index of dates and geos
    idx_date = pd.date_range(response_time_series['date'].min(),
                             response_time_series['date'].max())
    idx_geos = np.sort(response_time_series['geo'].unique())
    full_idx = list(itertools.product(idx_geos, idx_date))

    response_fix = response_time_series.set_index(['geo', 'date']).reindex(full_idx, fill_value = 0)
    cost_fix = cost_time_series.set_index(['geo', 'date']).reindex(full_idx, fill_value = 0)

    geo_level_time_series = pd.merge(response_fix, cost_fix, left_index = True, right_index = True, how = 'outer')
    geo_level_time_series = geo_level_time_series.reset_index()
    geo_level_time_series = pd.merge(geo_level_time_series, geo_dictionary, how = 'left', on = 'geo')
    
    return geo_level_time_series

# main function for Docker
def main():

    # setup logging handler
    logs_directory = os.environ.get('HABU_CONTAINER_LOGS')
    log_file = f'{logs_directory}/container.log'
    logging.basicConfig(
        handlers = [logging.FileHandler(filename = log_file,
                                        encoding = 'utf-8',
                                        mode = 'a+')],
                    format = '%(asctime)s %(name)s:%(levelname)s:%(message)s',
                    datefmt = '%F %A %T',
                    level = logging.INFO,
    )

    logging.info(f'Start Processing...')

    # load the data
    input_location = os.environ.get('INPUT_DATA')
    logging.info(f'Reading data in from {input_location}')
    
    response_time_series, cost_time_series = read(os.environ.get('INPUT_DATA'))

    logging.info(f'Cost and Response data successfully read.')

    geo_dictionary = create_dictionary(response_time_series)
    geo_dictionary.to_csv(os.environ.get('OUTPUT_DATA') + '/geo_dictionary.csv', index = False)

    logging.info(f'Geo dictionary successfully output.')
    logging.info(f'Creating the geo-level time series...')

    geo_level_time_series = create_time_series(response_time_series, cost_time_series, geo_dictionary)

    logging.info(f'Time series successfully created.')
    logging.info(f'Outputting csv...')

    geo_level_time_series.to_csv(os.environ.get('OUTPUT_DATA') + '/geo_level_time_series.csv', index = False)

    logging.info(f'Time series successfully output.')

if __name__ == '__main__':
    main()