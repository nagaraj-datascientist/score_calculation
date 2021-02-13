'''This module will calculate score for all the profiles.

'''

import configparser
import json
import logging.config
import os
import sys

import pandas as pd

from cassandra_connection import CassandraCluster
from data_manipulation import DataManipulation
from market_score_calculator import MarketScoreCalculator
from personal_score_calculator import PersonalScoreCalculator

# Cassandra configurations
c_cfg = configparser.ConfigParser()
c_cfg.read('cassandra_config.ini')

# Cassandra connection details
ip_address = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'IP_ADDRESS')
port = c_cfg.getint('CASSANDRA_SERVER_DETAILS', 'PORT')
user = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'USER')
password = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'PWD')
keyspace = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'KEY_SPACE')

# Folder details
data_folder = c_cfg.get('FOLDER_DETAILS', 'DATA_FOLDER')
log_folder = c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER')
score_folder = c_cfg.get('FOLDER_DETAILS', 'SCORE_FOLDER')

# Creates log, data and score folder
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

if not os.path.exists(score_folder):
    os.makedirs(score_folder)

# Initialize log
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

logger.info('Started.')

# Read SQL details
with open('sql_config.json') as f:
    sql_details = json.load(f)

logger.debug(f'SQL details - {json.dumps(sql_details, indent=2)}')

with open('data_file_meta_data.json') as f:
    data_file_details = json.load(f)

logger.debug(
    f'Data file meta data - {json.dumps(data_file_details, indent=4)}')

# Reading files
personal_info_file = data_file_details['personal_info']['file_name']
personal_info_rel_cols = data_file_details['personal_info']['rel_cols']

work_info_file = data_file_details['work_info']['file_name']
work_info_rel_cols = data_file_details['work_info']['rel_cols']

emp_tech_file = data_file_details['employee_technology_stack']['file_name']
emp_tech_rel_cols = data_file_details['employee_technology_stack']['rel_cols']

certificate_info_file = data_file_details['certificate_info']['file_name']
certificate_info_rel_cols = data_file_details['certificate_info']['rel_cols']

education_info_file = data_file_details['education_info']['file_name']

interview_sch_file = data_file_details['interview_schedule']['file_name']
interview_sch_rel_cols = data_file_details['interview_schedule']['rel_cols']

offers_info_file = data_file_details['offer_info']['file_name']
offers_info_rel_cols = data_file_details['offer_info']['rel_cols']

referral_info_file = data_file_details['referral_info']['file_name']
referral_info_rel_cols = data_file_details['referral_info']['rel_cols']

reporting_info_file = data_file_details['reporting_info']['file_name']
reporting_info_rel_cols = data_file_details['reporting_info']['rel_cols']

static_score_meta_file = data_file_details['static_score_meta']['file_name']

score_output_file_format = data_file_details['score_output_file_format']


def store_score_data(dataframe, file_name, file_type):
    '''Stores the dataframe into file

    Args:
        dataframe (pandas dataframe): Dataframe to be saved as file.
        file_name (str): file name to be saved.
        file_type (str): file format. Ex(csv, parquet)
    '''
    file_name = f'{file_name}.{file_type}'
    file_path = f'{os.path.join(score_folder, file_name)}'
    if file_type == 'csv':
        dataframe.to_csv(file_path, index=False)
    elif file_type == 'parquet':
        dataframe.to_parquet(file_path, index=False)


def main():
    # Pulling data from cassandra
    cc = CassandraCluster(ip_address, port, user, password)
    with cc.connect_cassandra(keyspace) as cas_con:

        logger.info('Data pull is started...')

        for table in sql_details.get('TABLES_LIST'):
            if table == 'static_score_meta':
                table_data_query = f'''SELECT 
                category, range_end, range_start, score, score_name, score_type 
                FROM {table}'''
            else:
                table_data_query = f'SELECT * FROM {table}'

            logger.debug(f'Table data query - {table_data_query}')
            file_location = os.path.join(data_folder, f'{keyspace}_{table}')
            cas_con.query_result_set_to_file(session=cas_con.session,
                                             query=table_data_query,
                                             file_location=file_location)

    dm = DataManipulation()
    msc = MarketScoreCalculator()
    psc = PersonalScoreCalculator()

    personal_info_df = pd.read_parquet(
        f'{os.path.join(data_folder, personal_info_file)}',
        columns=personal_info_rel_cols)

    score_eligible_prof_df = personal_info_df[
        personal_info_df.recalculate_score_eligible == 'Y']

    logger.info(f'Score eligible profiles - {score_eligible_prof_df.shape[0]}')

    work_df = pd.read_parquet(
        f'{os.path.join(data_folder, work_info_file)}',
        columns=work_info_rel_cols)

    technology_stack_df = pd.read_parquet(
        f'{os.path.join(data_folder, emp_tech_file)}',
        columns=emp_tech_rel_cols)

    certificate_df = pd.read_parquet(
        f'{os.path.join(data_folder, certificate_info_file)}',
        columns=certificate_info_rel_cols)

    education_df = pd.read_parquet(
        f'{os.path.join(data_folder, education_info_file)}')

    # Max eduction by max education id
    latest_education_df = dm.groupby_agg_func(
        education_df, 'education_id', 'emp_id', 'max', 'education_id')

    education_df = pd.merge(education_df, latest_education_df,
                            on=['emp_id', 'education_id'])

    interview_schedule_df = pd.read_parquet(
        f'{os.path.join(data_folder, interview_sch_file)}',
        columns=interview_sch_rel_cols)

    offers_info_df = pd.read_parquet(
        f'{os.path.join(data_folder, offers_info_file)}',
        columns=offers_info_rel_cols)

    no_of_offers_df = dm.groupby_agg_func(
        offers_info_df, 'offer_id', 'emp_id', 'count', 'no_of_offers')

    referral_info_df = pd.read_parquet(
        f'{os.path.join(data_folder, referral_info_file)}',
        columns=referral_info_rel_cols)

    referral_details_df = dm.groupby_agg_func(
        referral_info_df, 'referral_req_id', 'emp_id', 'count', 'no_of_referral')

    reporting_info_df = pd.read_parquet(
        f'{os.path.join(data_folder, reporting_info_file)}',
        columns=reporting_info_rel_cols)

    reporting_details_df = dm.groupby_agg_func(
        reporting_info_df, 'office_repo_id', 'emp_id', 'count', 'no_of_reporting')

    static_score_meta_df = pd.read_parquet(
        f'{os.path.join(data_folder, static_score_meta_file)}')

    # Merging person and work
    eligible_person_work_df = pd.merge(score_eligible_prof_df, work_df,
                                       on='emp_id', suffixes=('_person', '_work'))

    # Population person_work_df. No population time limit filter for exp score
    population_person_work_df = pd.merge(personal_info_df, work_df,
                                         on='emp_id', suffixes=('_person', '_work'))

    logger.info('Market score calculation is started...')
    logger.debug('Toal experience with population score is processing...')

    eligible_person_work_agg_df = dm.work_aggregation(eligible_person_work_df)
    population_person_work_agg_df = dm.work_aggregation(
        population_person_work_df)

    exp_ratio_df = dm.category_ratio(
        population_person_work_agg_df, 'total_exp')

    tot_exp_score_df = msc.total_exp_with_population(
        eligible_person_work_agg_df, exp_ratio_df, 'total_exp')

    logger.debug('Toal experience with population score is completed.')
    logger.debug('Domain with population score is processing...')

    # Population domain. No population time limit filter for domain score
    population_person_domain_df = pd.merge(personal_info_df, work_df,
                                           on='emp_id', suffixes=('_person', '_work'))

    population_domain_df = population_person_domain_df[[
        'emp_id', 'domain']].drop_duplicates()

    domain_ratio_df = dm.category_ratio(population_domain_df, 'domain')

    eligible_person_domain_df = eligible_person_work_df[[
        'emp_id', 'domain']].drop_duplicates()

    domain_score_df = msc.domain_with_population(
        eligible_person_domain_df, domain_ratio_df, 'domain')

    ms_domain_score_df = dm.groupby_agg_func(
        domain_score_df, 'domain_ratio', 'emp_id', 'sum', 'ms_domain_score')

    logger.debug('Domain with population score is completed.')
    logger.debug('Skill set with population score is processing...')

    # Population Skillset. No population time limit filter for Skillset score
    population_person_skillset_df = pd.merge(personal_info_df,
                                             technology_stack_df, on='emp_id', suffixes=('_person', '_tech'))

    # Merging person and technology
    eligible_person_tech_df = pd.merge(score_eligible_prof_df,
                                       technology_stack_df, on='emp_id', suffixes=('_person', '_tech'))

    population_skill_set_df = population_person_skillset_df[[
        'emp_id', 'technology_description']].drop_duplicates()

    skill_set_ratio_df = dm.category_ratio(population_skill_set_df,
                                           'technology_description')

    eligible_person_tech_df = eligible_person_tech_df[[
        'emp_id', 'technology_description']].drop_duplicates()

    skill_set_score_df = msc.skill_set_with_population(
        eligible_person_tech_df, skill_set_ratio_df, 'technology_description')

    ms_skill_set_score_df = dm.groupby_agg_func(
        skill_set_score_df, 'technology_description_ratio', 'emp_id', 'sum',
        'ms_skill_set_score')

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

    ms_cert_trend_score_df = dm.groupby_agg_func(
        cert_trend_score_df, 'certificate_name_ratio', 'emp_id', 'sum',
        'ms_cert_score')

    logger.debug('Certificate trend score is completed.')
    logger.info('Market score calculation is completed...')
    logger.info('Personal score calculation is started...')

    eligible_person_edu_df = pd.merge(score_eligible_prof_df, education_df,
                                      on='emp_id', suffixes=('_person', '_edu'))

    eligible_person_int_df = pd.merge(score_eligible_prof_df,
                                      interview_schedule_df, on='emp_id', suffixes=('_person', '_int'))

    eligible_person_offer_df = pd.merge(score_eligible_prof_df,
                                        no_of_offers_df, on='emp_id', suffixes=('_person', '_offer'))

    eligible_person_referral_df = pd.merge(score_eligible_prof_df,
                                           referral_details_df, on='emp_id', suffixes=('_person', '_referral'))

    eligible_person_reporting_df = pd.merge(score_eligible_prof_df,
                                            reporting_details_df, on='emp_id', suffixes=('_person', '_reporting'))

    logger.debug('Education score is processing...')
    education_score_df = psc.education_type_score(
        eligible_person_edu_df, static_score_meta_df)
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
    logger.debug('Offer score is processing...')
    offer_score_df = psc.no_of_offer_score(eligible_person_offer_df)
    logger.debug('Offer score is completed...')
    logger.debug('Referral score is processing...')
    referral_score_df = psc.referral_score(eligible_person_referral_df)
    logger.debug('Referral score is completed...')
    logger.debug('Reporting score is processing...')
    reporting_score_df = psc.reporting_score(eligible_person_reporting_df)
    logger.debug('Reporting score is completed...')
    logger.debug('Experience score is processing...')
    exp_year_score_df = psc.exp_year_score(
        eligible_person_work_agg_df, static_score_meta_df)
    logger.debug('Experience score is completed...')

    store_score_data(tot_exp_score_df,
                     'MS_Total_Experience_With_Population_Score', score_output_file_format)

    store_score_data(domain_score_df,
                     'MS_Domain_With_Population_Score_Details', score_output_file_format)

    store_score_data(ms_domain_score_df,
                     'MS_Domain_With_Population_Score', score_output_file_format)

    store_score_data(skill_set_score_df,
                     'MS_Skillset_With_Population_Score_Details', score_output_file_format)

    store_score_data(ms_skill_set_score_df,
                     'MS_Skillset_With_Population_Score', score_output_file_format)

    store_score_data(cert_trend_score_df,
                     'MS_Certificate_Trend_Score_Details', score_output_file_format)

    store_score_data(ms_cert_trend_score_df,
                     'MS_Certificate_Trend_Score', score_output_file_format)

    store_score_data(education_score_df,
                     'PS_Education_Score', score_output_file_format)

    store_score_data(valid_cert_score_df,
                     'PS_Certificate_Score', score_output_file_format)

    store_score_data(domain_score_df,
                     'PS_Domain_Score', score_output_file_format)

    store_score_data(reliablity_score_df,
                     'PS_Reliability_Score', score_output_file_format)

    store_score_data(skill_set_score_df,
                     'PS_Skill_Set_Score', score_output_file_format)

    store_score_data(interview_score_df,
                     'PS_Interview_Score', score_output_file_format)

    store_score_data(offer_score_df,
                     'PS_Offer_Score', score_output_file_format)

    store_score_data(exp_year_score_df, 'PS_Exp_Year_Score',
                     score_output_file_format)

    store_score_data(referral_score_df, 'PS_Referral_Score',
                     score_output_file_format)

    store_score_data(reporting_score_df, 'PS_Reporting_Score',
                     score_output_file_format)

    logger.info('Personal score calculation is completed...')


if __name__ == '__main__':
    main()
    logger.info('completed.')
    sys.exit(0)
