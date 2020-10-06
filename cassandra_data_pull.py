'''This module will generate parquet file for all the tables in a keyspace.

Details:
    Based on the cassandra_config.ini file and sql_config.ini file,
        this module will generate parquet file in data directory.
'''

import configparser
import logging.config
import os
import sys

import cassandra_connection as cc

# Configurations
c_cfg = configparser.ConfigParser()
c_cfg.read('cassandra_config.ini')

s_cfg = configparser.ConfigParser()
s_cfg.read('sql_config.ini')

# Creates log and data folder
if not os.path.exists(c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER')):
    os.makedirs(c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER'))

if not os.path.exists(c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')):
    os.makedirs(c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER'))

# Initialize log
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def main():
    '''Makes cassandra connection and pulls all table data in parquet file.'''

    logger.info('started.')

    all_tables_query = f'''{
        s_cfg.get('SYSTEM', 'ALL_TABLES_QUERY')} '{
            c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')}' '''

    logger.debug(f'Query to get all the tables - {all_tables_query}')
    cas_con = cc.CassandraCluster(
        c_cfg.get('CASSANDRA_SERVER_DETAILS', 'IP_ADDRESS'),
        c_cfg.getint('CASSANDRA_SERVER_DETAILS', 'PORT'),
        c_cfg.get('CASSANDRA_SERVER_DETAILS', 'USER'),
        c_cfg.get('CASSANDRA_SERVER_DETAILS', 'PWD'),)

    cluster, session = cas_con.cassandra_session()
    logger.info('Cassandra connection is established.')

    # Get all tables from the key space
    all_tables_df = cas_con.pandas_result_set(session, c_cfg.get(
        'CASSANDRA_SERVER_DETAILS', 'KEY_SPACE'), all_tables_query)

    all_tables = all_tables_df['table_name'].tolist()
    all_tables_df.to_csv(
        f'''{
            c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')}/{
                c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')
                }_all_tables.csv''', index=False)

    logger.info('Data pull is processing...')

    # Write all table data into parquet file
    for table in all_tables:
        table_data_query = s_cfg.get('DATA', 'TABLES_DATA_QUERY') + ' ' + table
        logger.debug(f'table data query - {table_data_query}')
        table_data_df = cas_con.pandas_result_set(
            session, c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE'),
            table_data_query)

        table_data_df.to_parquet(
            f'''{
                c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')}/{
                    c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')
                    }_{table}.parquet''', index=False)

        # table_data_df.to_csv(
        #     f'''{
        #         c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')}/{
        #             c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')
        #             }_{t}.csv''', index=False)

    cas_con.cluster_shutdown(cluster)


if __name__ == '__main__':
    main()
    logger.info('completed.')
    sys.exit(0)
