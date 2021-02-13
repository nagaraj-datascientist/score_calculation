'''This module will generate parquet file for all the tables in a keyspace.

Details:
    Based on the cassandra_config.ini file and sql_config.ini file,
        this module will generate parquet file in data directory.
'''

import configparser
import json
import logging.config
import os
import sys

from cassandra_connection import CassandraCluster

# Reading Cassandra Configurations
c_cfg = configparser.ConfigParser()
c_cfg.read('cassandra_config.ini')

# Reading sql configuration
with open('sql_config.json') as f:
    s_cfg = json.load(f)

all_tables_query = s_cfg.get('ALL_TABLES_QUERY')
individual_table_data_query = s_cfg.get('INDIVIDUAL_TABLE_DATA_QUERY')
all_tables_query = f'''{all_tables_query} '{keyspace}' '''

# Cassandra config details
ip_address = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'IP_ADDRESS')
port = c_cfg.getint('CASSANDRA_SERVER_DETAILS', 'PORT')
user = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'USER')
password = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'PWD')
keyspace = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')

# Data and log folders
data_folder = c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')
log_folder = c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER')

# Creating data and log folders
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Log details
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def main():
    '''Makes cassandra connection and pulls all table data in parquet file.'''

    logger.info('started.')
    logger.debug(f'Query to get all the tables - {all_tables_query}')

    cc = CassandraCluster(ip_address, port, user, password)

    with cc.connect_cassandra(keyspace) as cas_con:

        all_tables_df = cas_con.query_result_set_to_pandas(cas_con.session,
                                                           all_tables_query)

        file_location = f'{data_folder}/{keyspace}_all_tables'
        cas_con.query_result_set_to_file(session=cas_con.session,
                                         query=all_tables_query,
                                         file_location=file_location,
                                         file_type='csv')

        all_tables = all_tables_df['table_name'].tolist()
        logger.info('Data pull is started...')

        for table in all_tables:
            table_data_query = f'{individual_table_data_query} {table}'
            logger.debug(f'table data query - {table_data_query}')
            file_location = f'{data_folder}/{keyspace}_{table}'

            cas_con.query_result_set_to_file(session=cas_con.session,
                                             query=table_data_query,
                                             file_location=file_location,
                                             file_type='csv')


if __name__ == '__main__':
    main()
    logger.info('completed.')
    sys.exit(0)
