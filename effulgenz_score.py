'''This module will calculate score for all the profiles.

'''

import configparser
import json
import logging.config
import os

import pandas as pd

import cassandra_connection as cc
from data_manipulation import DataManipulation
from market_score_calculator import MarketScoreCalculator
from personal_score_calculator import PersonalScoreCalculator

# Configurations
c_cfg = configparser.ConfigParser()
c_cfg.read('cassandra_config.ini')

# Creates log and data folder
if not os.path.exists(c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER')):
    os.makedirs(c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER'))

if not os.path.exists(c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')):
    os.makedirs(c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER'))

if not os.path.exists(c_cfg.get('FOLDER_DETAILS', 'SCORE_FOLDER')):
    os.makedirs(c_cfg.get('FOLDER_DETAILS', 'SCORE_FOLDER'))


# Initialize log
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

logger.info('Started.')

# Read SQL details
with open('sql_config.json') as f:
    sql_details = json.load(f)

logger.debug(f'SQL details - {json.dumps(sql_details, indent=2)}')

# Cassandra connection
cas_con = cc.CassandraCluster(
    c_cfg.get('CASSANDRA_SERVER_DETAILS', 'IP_ADDRESS'),
    c_cfg.getint('CASSANDRA_SERVER_DETAILS', 'PORT'),
    c_cfg.get('CASSANDRA_SERVER_DETAILS', 'USER'),
    c_cfg.get('CASSANDRA_SERVER_DETAILS', 'PWD'),)

cluster, session = cas_con.cassandra_session()

logger.info('Cassandra connection is established.')
logger.info('Data pull is processing...')

# Write all table data into parquet file
if sql_details.get('TABLES_LIST') is not None:
    for table in sql_details.get('TABLES_LIST'):
        table_data_query = f'SELECT * FROM {table}'

        logger.debug(f'Table data query - {table_data_query}')

        table_data_df = cas_con.pandas_result_set(
            session, c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE'),
            table_data_query)

        table_data_df.to_parquet(
            f'''{c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')}/{
                c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')}_{
                    table}.parquet''', index=False)

cas_con.cluster_shutdown(cluster)

with open('data_file_meta_data.json') as f:
    data_file_details = json.load(f)

logger.debug(
    f'Data file meta data - {json.dumps(data_file_details, indent=4)}')

dm = DataManipulation()
msc = MarketScoreCalculator()
psc = PersonalScoreCalculator()

personal_info_df = pd.read_parquet(
    f'''data/{data_file_details['personal_info']['file_name']}''',
    columns=data_file_details['personal_info']['rel_cols'])

score_eligible_prof_df = personal_info_df[
    personal_info_df.recalculate_score_eligible == 'Y']

logger.info(f'Score eligible profiles - {score_eligible_prof_df.shape[0]}')

work_df = pd.read_parquet(
    f'''data/{data_file_details['work_info']['file_name']}''',
    columns=data_file_details['work_info']['rel_cols'])

technology_stack_df = pd.read_parquet(
    f'''data/{data_file_details['employee_technology_stack']['file_name']}''',
    columns=data_file_details['employee_technology_stack']['rel_cols'])

certificate_df = pd.read_parquet(
    f'''data/{data_file_details['certificate_info']['file_name']}''',
    columns=data_file_details['certificate_info']['rel_cols'])

education_df = pd.read_parquet(
    f'''data/{data_file_details['education_info']['file_name']}''')

interview_schedule_df = pd.read_parquet(
    f'''data/{data_file_details['interview_schedule']['file_name']}''',
    columns=data_file_details['interview_schedule']['rel_cols'])

# Merging person and work
eligible_person_work_df = pd.merge(score_eligible_prof_df, work_df,
                                   on='emp_id', suffixes=('_person', '_work'))

# Population person_work_df. No population time limit filter for exp score
population_person_work_df = pd.merge(personal_info_df, work_df,
                                     on='emp_id', suffixes=('_person', '_work'))

logger.info('Market score calculation is started...')


logger.debug('Toal experience with population score is processing...')

eligible_person_work_agg_df = dm.work_aggregation(eligible_person_work_df)
population_person_work_agg_df = dm.work_aggregation(population_person_work_df)

exp_ratio_df = dm.category_ratio(population_person_work_agg_df, 'total_exp')

tot_exp_score_df = msc.total_exp_with_population(
    eligible_person_work_agg_df, exp_ratio_df, 'total_exp')

logger.debug('Toal experience with population score is completed.')

logger.debug('Domain with population score is processing...')

# Population domain. No population time limit filter for domain score
population_person_domain_df = pd.merge(personal_info_df, work_df,
                                       on='emp_id',
                                       suffixes=('_person', '_work'))

population_domain_df = population_person_domain_df[[
    'emp_id', 'domain']].drop_duplicates()

domain_ratio_df = dm.category_ratio(population_domain_df, 'domain')

eligible_person_domain_df = eligible_person_work_df[[
    'emp_id', 'domain']].drop_duplicates()

domain_score_df = msc.domain_with_population(
    eligible_person_domain_df, domain_ratio_df, 'domain')

logger.debug('Domain with population score is completed.')

logger.debug('Skill set with population score is processing...')

# Population Skillset. No population time limit filter for Skillset score
population_person_skillset_df = pd.merge(personal_info_df, technology_stack_df,
                                         on='emp_id',
                                         suffixes=('_person', '_tech'))

# Merging person and technology
eligible_person_tech_df = pd.merge(score_eligible_prof_df, technology_stack_df,
                                   on='emp_id', suffixes=('_person', '_tech'))


population_skill_set_df = population_person_skillset_df[[
    'emp_id', 'technology_description']].drop_duplicates()

skill_set_ratio_df = dm.category_ratio(population_skill_set_df,
                                       'technology_description')

eligible_person_tech_df = eligible_person_tech_df[[
    'emp_id', 'technology_description']].drop_duplicates()


skill_set_score_df = msc.skill_set_with_population(
    eligible_person_tech_df, skill_set_ratio_df, 'technology_description')

logger.debug('Skillset with population score is completed.')

logger.debug('Certificate trend score is processing...')

# Population certificate trend.
# Last 2 years completed certificates are considered as trend
cert_trend_df = dm.certificate_trend(certificate_df, active_days=730)

cert_trend_ratio_df = dm.category_ratio(cert_trend_df, 'certificate_name')

eligible_person_cert_df = pd.merge(score_eligible_prof_df, certificate_df,
                                   on='emp_id', suffixes=('_person', '_cert'))

cert_trend_score_df = msc.certificate_with_trend(
    eligible_person_cert_df, cert_trend_ratio_df)

logger.debug('Certificate trend score is completed.')

tot_exp_score_df.to_csv(
    'score/MS_Total_Experience_With_Population_Score.csv', index=False)
domain_score_df.to_csv(
    'score/MS_Domain_With_Population_Score.csv', index=False)
skill_set_score_df.to_csv(
    'score/MS_Skillset_With_Population_Score.csv', index=False)
cert_trend_score_df.to_csv(
    'score/MS_Certificate_Trend_Score.csv', index=False)

logger.info('Market score calculation is completed...')
logger.info('Personal score calculation is started...')


eligible_person_edu_df = pd.merge(score_eligible_prof_df, education_df,
                                  on='emp_id', suffixes=('_person', '_edu'))


eligible_person_int_df = pd.merge(score_eligible_prof_df, interview_schedule_df,
                                  on='emp_id', suffixes=('_person', '_int'))

logger.debug('Education score is processing...')
education_score_df = psc.education_score(eligible_person_edu_df)
logger.debug('Education score is completed...')
logger.debug('Valid certificate score is processing...')
valid_cert_score_df = psc.valid_certificate_score(eligible_person_cert_df)
logger.debug('Valid certificate score is completed...')
logger.debug('Domain score is processing...')
domain_score_df = psc.domain_score(eligible_person_work_agg_df)
logger.debug('Domain score is completed...')
logger.debug('Reliablity score is processing...')
reliablity_score_df = psc.reliablity_score(eligible_person_work_agg_df)
logger.debug('Reliablity score is completed...')
logger.debug('Skill set score is processing...')
skill_set_score_df = psc.skill_set_score(eligible_person_tech_df)
logger.debug('Skil set score is completed...')
logger.debug('Interview score is processing...')
interview_score_df = psc.interview_score(eligible_person_int_df)
logger.debug('Interview score is completed...')

education_score_df.to_csv('score/PS_Education_Score.csv', index=False)
valid_cert_score_df.to_csv('score/PS_Certificate_Score.csv', index=False)
domain_score_df.to_csv('score/PS_Domain_Score.csv', index=False)
reliablity_score_df.to_csv('score/PS_Reliability_Score.csv', index=False)
skill_set_score_df.to_csv('score/PS_Skill_Set_Score.csv', index=False)
interview_score_df.to_csv('score/PS_Interview_Score.csv', index=False)

logger.info('Personal score calculation is completed...')

logger.info('completed')
