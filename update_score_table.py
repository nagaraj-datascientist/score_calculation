''' This module will load the score data into table'''

import configparser
import json
import logging.config
import os
from datetime import datetime
from functools import reduce

import pandas as pd

from cassandra_connection import CassandraCluster


# Initialize log
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

logger.info('Started.')

# Cassandra Configurations
c_cfg = configparser.ConfigParser()
c_cfg.read('cassandra_config.ini')

# Cassandra details
ip_address = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'IP_ADDRESS')
port = c_cfg.getint('CASSANDRA_SERVER_DETAILS', 'PORT')
user = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'USER')
password = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'PWD')
keyspace = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')

table_entry_user = c_cfg.get('TABLE_ENTRY', 'SCRIPT_EXECUTION_USER')

score_folder = c_cfg.get('FOLDER_DETAILS', 'SCORE_FOLDER')
with open('sql_config.json') as f:
    s_cfg = json.load(f)

# Query details
score_details_insert_query = s_cfg['SCORE_DETAILS_INSERT_QUERY']
score_details_update_query = s_cfg['SCORE_DETAILS_UPDATE_QUERY']
score_details_select_query = s_cfg['SCORE_DETAILS_SELECT_QUERY']
personal_info_update_query = s_cfg['PERSONAL_INFO_RECALCULATE_UPDATE_QUERY']
score_history_details_insert_query = s_cfg['SCORE_HISTORY_DETAILS_INSERT_QUERY']

with open('data_file_meta_data.json') as f:
    data_file_details = json.load(f)

logger.debug(
    f'Data file meta data - {json.dumps(data_file_details, indent=4)}')

score_output_file_format = data_file_details['score_output_file_format']

ps_skill_set_file = os.path.join(score_folder,
                                 f'''{data_file_details['ps_skill_set_file']}.{score_output_file_format}''')

ps_reliability_file = os.path.join(score_folder,
                                   f'''{data_file_details['ps_reliability_file']}.{score_output_file_format}''')

ps_interview_file = os.path.join(score_folder,
                                 f'''{data_file_details['ps_interview_file']}.{score_output_file_format}''')

ps_education_file = os.path.join(score_folder,
                                 f'''{data_file_details['ps_education_file']}.{score_output_file_format}''')

ps_domain_file = os.path.join(score_folder,
                              f'''{data_file_details['ps_domain_file']}.{score_output_file_format}''')

ps_certificate_file = os.path.join(score_folder,
                                   f'''{data_file_details['ps_certificate_file']}.{score_output_file_format}''')

ps_reporting_file = os.path.join(score_folder,
                                 f'''{data_file_details['ps_reporting_file']}.{score_output_file_format}''')

ps_referral_file = os.path.join(score_folder,
                                f'''{data_file_details['ps_referral_file']}.{score_output_file_format}''')

ps_offer_file = os.path.join(score_folder,
                             f'''{data_file_details['ps_offer_file']}.{score_output_file_format}''')

ps_exp_year_file = os.path.join(score_folder,
                                f'''{data_file_details['ps_exp_year_file']}.{score_output_file_format}''')

ms_total_exp_file = os.path.join(score_folder,
                                 f'''{data_file_details['ms_total_exp_file']}.{score_output_file_format}''')

ms_skill_set_file = os.path.join(score_folder,
                                 f'''{data_file_details['ms_skill_set_file']}.{score_output_file_format}''')

ms_domain_file = os.path.join(score_folder,
                              f'''{data_file_details['ms_domain_file']}.{score_output_file_format}''')

ms_certificate_file = os.path.join(score_folder,
                                   f'''{data_file_details['ms_certificate_file']}.{score_output_file_format}''')

score_data_rel_cols = data_file_details['score_data_rel_cols']


if score_output_file_format == 'parquet':
    ps_skill_set_df = pd.read_parquet(ps_skill_set_file)
    ps_reliability_df = pd.read_parquet(ps_reliability_file)
    ps_interview_df = pd.read_parquet(ps_interview_file)
    ps_education_df = pd.read_parquet(ps_education_file)
    ps_domain_df = pd.read_parquet(ps_domain_file)
    ps_certificate_df = pd.read_parquet(ps_certificate_file)
    ps_reporting_df = pd.read_parquet(ps_reporting_file)
    ps_referral_df = pd.read_parquet(ps_referral_file)
    ps_offer_df = pd.read_parquet(ps_offer_file)
    ps_exp_year_df = pd.read_parquet(ps_exp_year_file)

    ms_total_exp_df = pd.read_parquet(ms_total_exp_file)
    ms_skill_set_df = pd.read_parquet(ms_skill_set_file)
    ms_domain_df = pd.read_parquet(ms_domain_file)
    ms_certificate_df = pd.read_parquet(ms_certificate_file)

elif score_output_file_format == 'csv':
    ps_skill_set_df = pd.read_csv(ps_skill_set_file)
    ps_reliability_df = pd.read_csv(ps_reliability_file)
    ps_interview_df = pd.read_csv(ps_interview_file)
    ps_education_df = pd.read_csv(ps_education_file)
    ps_domain_df = pd.read_csv(ps_domain_file)
    ps_certificate_df = pd.read_csv(ps_certificate_file)
    ps_reporting_df = pd.read_csv(ps_reporting_file)
    ps_referral_df = pd.read_csv(ps_referral_file)
    ps_offer_df = pd.read_csv(ps_offer_file)
    ps_exp_year_df = pd.read_csv(ps_exp_year_file)

    ms_total_exp_df = pd.read_csv(ms_total_exp_file)
    ms_skill_set_df = pd.read_csv(ms_skill_set_file)
    ms_domain_df = pd.read_csv(ms_domain_file)
    ms_certificate_df = pd.read_csv(ms_certificate_file)

data_frames = [ps_skill_set_df, ps_reliability_df, ps_interview_df,
               ps_education_df, ps_domain_df, ps_certificate_df,
               ps_reporting_df, ps_referral_df, ps_offer_df, ps_exp_year_df,
               ms_total_exp_df, ms_skill_set_df, ms_domain_df, ms_certificate_df]

overall_score_df = reduce(lambda left, right: pd.merge(
    left, right, on='emp_id', how='outer'), data_frames)

overall_rel_score_df = overall_score_df[score_data_rel_cols].fillna(0)

overall_rel_score_df['prscore'] = (overall_rel_score_df['cert_score']
                                   + overall_rel_score_df['domain_score']
                                   + overall_rel_score_df['education_score']
                                   + overall_rel_score_df['interview_score']
                                   + overall_rel_score_df['rel_score2']
                                   + overall_rel_score_df['skill_set_score']
                                   + overall_rel_score_df['reporting_score']
                                   + overall_rel_score_df['referral_score']
                                   + overall_rel_score_df['no_of_offers_score']
                                   + overall_rel_score_df['exp_year_score'])

overall_rel_score_df['vrscore'] = (overall_rel_score_df['ms_cert_score']
                                   + overall_rel_score_df['ms_domain_score']
                                   + overall_rel_score_df['total_exp_ratio']
                                   + overall_rel_score_df['ms_skill_set_score'])


# overall_rel_score_df.to_csv('overall_calculated_score.csv', index=False)

cc = CassandraCluster(ip_address, port, user, password)

with cc.connect_cassandra(keyspace) as cas_con:

    score_details_table_df = cas_con.query_result_set_to_pandas(
        cas_con.session,
        score_details_select_query)

    score_merge_df = pd.merge(overall_rel_score_df, score_details_table_df,
                              suffixes=('_derived', '_table'), how='outer', indicator=True, on='emp_id')

    update_df = score_merge_df[score_merge_df['_merge'] == 'both']
    insert_df = score_merge_df[score_merge_df['_merge'] == 'left_only']

    logger.info(
        f'Insert and update records = {insert_df.shape[0]} & {update_df.shape[0]}')

    # Reference: https://docs.datastax.com/en/developer/python-driver/3.24/api/cassandra/cluster/

    if insert_df.shape[0] > 0:

        logger.debug(f'Insert query - {score_details_insert_query}')
        insertion_prepare = cas_con.session.prepare(score_details_insert_query)
        logger.debug(f'Query execution prepare - {insertion_prepare}')

        for index, row in insert_df.iterrows():
            # continue
            # Insert into score_detail_info
            cas_con.session.execute(insertion_prepare, (
                row['emp_id'], 'PR_' + row['emp_id'], 'VR_' + row['emp_id'],
                str(row['cert_score']), str(row['ms_cert_score']),
                table_entry_user, datetime.now(), str(row['domain_score']),
                str(row['ms_domain_score']), str(row['education_score']),
                str(row['exp_year_score']),
                str(row['total_exp_ratio']), str(row['interview_score']),
                str(row['rel_score2']), str(row['skill_set_score']),
                str(row['no_of_offers_score']), str(row['prscore_derived']),
                str(row['referral_score']), str(row['reporting_score']),
                str(row['ms_skill_set_score']), '10', 'Y', str(row['vrscore_derived'])))
            logger.info(
                f'Out of {overall_rel_score_df.shape[0]}, {index+1} - inserted in score_detail_info table.')

            personal_info_emp_id_update_query = f'''{personal_info_update_query} '{row['emp_id']}' '''
            logger.debug(
                f'personal_info update query = {personal_info_emp_id_update_query}')

            cas_con.session.execute(personal_info_emp_id_update_query)
            logger.info(
                f'Out of {overall_rel_score_df.shape[0]}, {index+1} - updated in personal_info table.')
    if update_df.shape[0] > 0:

        logger.debug(
            f'History Insert query - {score_history_details_insert_query}')
        history_insertion_prepare = cas_con.session.prepare(
            score_history_details_insert_query)
        logger.debug(f'Query execution prepare - {history_insertion_prepare}')

        logger.debug(f'Detail Insert query - {score_details_update_query}')
        detail_update_prepare = cas_con.session.prepare(
            score_details_update_query)
        logger.debug(f'Query execution prepare - {detail_update_prepare}')

        for index, row in update_df.iterrows():

            cas_con.session.execute(history_insertion_prepare, (
                row['emp_id'], row['proficiency_score_id'], row['varaiable_score_id'],
                str(row['certification_point']), str(
                    row['certification_population_point']),
                str(row['created_by']), row['created_time'], str(
                    row['domain_point']),
                str(row['domain_population_point']), str(
                    row['education_point']),
                str(row['experience_point']),
                str(row['experience_population_point']), str(
                    row['interview_point']),
                str(row['job_longevity_point']), str(row['niche_skill_point']),
                str(row['noofoffers_point']), str(row['prscore_table']),
                str(row['referrals_point']), str(row['reporting_point']),
                str(row['skillset_population_point']), str(
                    row['special_rating_point']),
                str(row['status']), str(row['vrscore_table'])))

            logger.info(
                f'Out of {overall_rel_score_df.shape[0]}, {index+1} - inserted in score_history_detail_info table.')

            cas_con.session.execute(detail_update_prepare, (
                row['emp_id'], 'PR_' + row['emp_id'], 'VR_' + row['emp_id'],
                str(row['cert_score']), str(row['ms_cert_score']),
                table_entry_user, datetime.now(), str(row['domain_score']),
                str(row['ms_domain_score']), str(row['education_score']),
                str(row['exp_year_score']),
                str(row['total_exp_ratio']), str(row['interview_score']),
                str(row['rel_score2']), str(row['skill_set_score']),
                str(row['no_of_offers_score']), str(row['prscore_derived']),
                str(row['referral_score']), str(row['reporting_score']),
                str(row['ms_skill_set_score']), '10', 'Y', str(row['vrscore_derived'])))
            logger.info(
                f'Out of {overall_rel_score_df.shape[0]}, {index+1} - inserted in score_detail_info table.')

            personal_info_emp_id_update_query = f'''{personal_info_update_query} '{row['emp_id']}' '''
            logger.debug(
                f'personal_info update query = {personal_info_emp_id_update_query}')

            cas_con.session.execute(personal_info_emp_id_update_query)
            logger.info(
                f'Out of {overall_rel_score_df.shape[0]}, {index+1} - updated in personal_info table.')
