'''This module will calculate score for all the profiles.

'''

import configparser
import json
import logging.config
import os
import sys
from datetime import datetime
from functools import reduce

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
dev_keyspace = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'DEV_KEY_SPACE')
staging_keyspace = c_cfg.get('CASSANDRA_SERVER_DETAILS', 'STAGING_KEY_SPACE')
requested_user = c_cfg.get('TABLE_ENTRY', 'SCRIPT_EXECUTION_USER')

staging_config_table = c_cfg.get(
    'CASSANDRA_SERVER_DETAILS', 'STAGING_CONFIG_TABLE')
batch_table = c_cfg.get(
    'CASSANDRA_SERVER_DETAILS', 'BATCH_TABLE')

cc = CassandraCluster(ip_address, port, user, password)

with open('sql_config.json') as f:
    s_cfg = json.load(f)

# Query details
batch_process_insert_query = s_cfg['BATCH_PROCESS_INSERT_QUERY']
batch_process_update_query = s_cfg['BATCH_PROCESS_UPDATE_QUERY']

ms_calc_tot_exp_insert_query = s_cfg['MS_CALC_TOT_EXP_INSERT_QUERY']
ms_calc_domain_insert_query = s_cfg['MS_CALC_DOMAIN_INSERT_QUERY']
ms_calc_skillset_insert_query = s_cfg['MS_CALC_SKILLSET_INSERT_QUERY']
ms_calc_cert_trend_insert_query = s_cfg['MS_CALC_CERT_TREND_INSERT_QUERY']

ps_calc_education_insert_query = s_cfg['PS_CALC_EDUCATION_INSERT_QUERY']
ps_calc_cert_insert_query = s_cfg['PS_CALC_CERT_INSERT_QUERY']
ps_calc_domain_insert_query = s_cfg['PS_CALC_EDUCATION_INSERT_QUERY']
ps_calc_reliability_insert_query = s_cfg['PS_CALC_RELIABILITY_INSERT_QUERY']
ps_calc_skillset_insert_query = s_cfg['PS_CALC_SKILLSET_INSERT_QUERY']
ps_calc_interview_insert_query = s_cfg['PS_CALC_INTERVIEW_INSERT_QUERY']
ps_calc_offers_insert_query = s_cfg['PS_CALC_OFFERS_INSERT_QUERY']
ps_calc_referral_insert_query = s_cfg['PS_CALC_REFERRAL_INSERT_QUERY']
ps_calc_reporting_insert_query = s_cfg['PS_CALC_REPORTING_INSERT_QUERY']
ps_calc_exp_insert_query = s_cfg['PS_CALC_EXP_INSERT_QUERY']

score_details_insert_query = s_cfg['SCORE_DETAILS_INSERT_QUERY']
score_details_update_query = s_cfg['SCORE_DETAILS_UPDATE_QUERY']
score_details_select_query = s_cfg['SCORE_DETAILS_SELECT_QUERY']
personal_info_update_query = s_cfg['PERSONAL_INFO_RECALCULATE_UPDATE_QUERY']
score_history_details_insert_query = s_cfg['SCORE_HISTORY_DETAILS_INSERT_QUERY']

stg_config_select_query = f'SELECT * FROM {staging_config_table}'
batch_select_query = f'SELECT * FROM {batch_table}'

# Folder details
log_folder = c_cfg.get('FOLDER_DETAILS', 'LOG_FOLDER')

# Creates log folder
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Initialize log
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

logger.info('Started.')


def update_batch_table(status, batch_uuid):

    with cc.connect_cassandra(staging_keyspace) as cas_con:

        batch_update_prepare = cas_con.session.prepare(
            batch_process_update_query)

        cas_con.session.execute(batch_update_prepare,
                                (status, datetime.now(), batch_uuid))


def main():
    # Pulling data from cassandra
    with cc.connect_cassandra(staging_keyspace) as cas_con:

        logger.info('Pulling Staging configuration details...')

        staging_config_df = cas_con.query_result_set_to_pandas(
            cas_con.session, stg_config_select_query)

        batch_df = cas_con.query_result_set_to_pandas(
            cas_con.session, batch_select_query)

        batch_insert_prepare = cas_con.session.prepare(
            batch_process_insert_query)

        batch_id = batch_df.shape[0]+1
        cas_con.session.execute(batch_insert_prepare,
                                (batch_id,
                                 requested_user,
                                 'DATA PULL STARTED',
                                 datetime.now()))

        batch_df = cas_con.query_result_set_to_pandas(
            cas_con.session, batch_select_query)

        batch_uuid = batch_df[batch_df['batch_id']
                              == batch_id].reset_index().at[0, 'id']

    with cc.connect_cassandra(dev_keyspace) as cas_con:

        logger.info('Data pull started...')

        dev_data = {}
        for index, row in staging_config_df.iterrows():
            select_query = f'''
                SELECT {row['dev_rel_columns']}
                FROM {row['dev_table_name']}'''
            logger.debug(f'select query = {select_query}')

            dev_data[row['dev_table_name']] = cas_con.query_result_set_to_pandas(
                cas_con.session, select_query)

            logger.info(
                f'''Out of {staging_config_df.shape[0]} table, {index+1} pulled''')

    update_batch_table('DATA PULL COMPLETED', batch_uuid)

    dm = DataManipulation()
    msc = MarketScoreCalculator()
    psc = PersonalScoreCalculator()

    personal_info_df = dev_data.get('employee_personal_info')

    score_eligible_prof_df = personal_info_df[
        personal_info_df.recalculate_score_eligible == 'Y']

    logger.info(f'Score eligible profiles - {score_eligible_prof_df.shape[0]}')

    work_df = dev_data.get('employee_work_info')
    technology_stack_df = dev_data.get('employee_technology_stack_v1')
    certificate_df = dev_data.get('employee_certificate_info')
    education_df = dev_data.get('employee_education_info')
    # Max eduction by max education id
    latest_education_df = dm.groupby_agg_func(
        education_df, 'education_id', 'emp_id', 'max', 'education_id')

    education_df = pd.merge(education_df, latest_education_df,
                            on=['emp_id', 'education_id'])

    interview_schedule_df = dev_data.get('interview_schedule')
    offers_info_df = dev_data.get('employee_offer_info')
    no_of_offers_df = dm.groupby_agg_func(
        offers_info_df, 'offer_id', 'emp_id', 'count', 'no_of_offers')

    referral_info_df = dev_data.get('employee_req_referral_info')
    referral_details_df = dm.groupby_agg_func(
        referral_info_df, 'referral_req_id', 'emp_id', 'count', 'no_of_referral')

    reporting_info_df = dev_data.get('employee_office_reporting')
    reporting_details_df = dm.groupby_agg_func(
        reporting_info_df, 'office_repo_id', 'emp_id', 'count', 'no_of_reporting')

    static_score_meta_df = dev_data.get('static_score_meta')

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

    ms_tot_exp_score_df = msc.total_exp_with_population(
        eligible_person_work_agg_df, exp_ratio_df, 'total_exp')

    logger.debug('Toal experience with population score is completed.')
    logger.debug('Domain with population score is processing...')

    # Population domain. No population time limit to filter for domain score
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
    ps_education_score_df = psc.education_type_score(
        eligible_person_edu_df, static_score_meta_df)
    logger.debug('Education score is completed...')
    logger.debug('Valid certificate score is processing...')
    ps_valid_cert_score_df = psc.valid_certificate_score(
        eligible_person_cert_df)
    logger.debug('Valid certificate score is completed...')
    logger.debug('Domain score is processing...')
    ps_domain_score_df = psc.domain_score(eligible_person_work_agg_df)
    logger.debug('Domain score is completed...')
    logger.debug('Reliablity score is processing...')
    ps_reliability_score_df = psc.reliability_score(
        eligible_person_work_agg_df)
    logger.debug('Reliablity score is completed...')
    logger.debug('Skill set score is processing...')
    ps_skill_set_score_df = psc.skill_set_score(eligible_person_tech_df)
    logger.debug('Skil set score is completed...')
    logger.debug('Interview score is processing...')
    ps_interview_score_df = psc.interview_score(eligible_person_int_df)
    logger.debug('Interview score is completed...')
    logger.debug('Offer score is processing...')
    ps_offer_score_df = psc.no_of_offer_score(eligible_person_offer_df)
    logger.debug('Offer score is completed...')
    logger.debug('Referral score is processing...')
    ps_referral_score_df = psc.referral_score(eligible_person_referral_df)
    logger.debug('Referral score is completed...')
    logger.debug('Reporting score is processing...')
    ps_reporting_score_df = psc.reporting_score(eligible_person_reporting_df)
    logger.debug('Reporting score is completed...')
    logger.debug('Experience score is processing...')
    ps_exp_year_score_df = psc.exp_year_score(
        eligible_person_work_agg_df, static_score_meta_df)
    logger.debug('Experience score is completed...')

    logger.info('Personal score calculation is completed...')

    update_batch_table('SCORE CALCULATION COMPLETED', batch_uuid)

    ms_tot_exp_score_df.fillna(0, inplace=True)
    ms_domain_score_df.fillna(0, inplace=True)
    ms_skill_set_score_df.fillna(0, inplace=True)
    ms_cert_trend_score_df.fillna(0, inplace=True)

    ps_education_score_df.fillna(0, inplace=True)
    ps_valid_cert_score_df.fillna(0, inplace=True)
    ps_domain_score_df.fillna(0, inplace=True)
    ps_reliability_score_df.fillna(0, inplace=True)
    ps_skill_set_score_df.fillna(0, inplace=True)
    ps_interview_score_df.fillna(0, inplace=True)
    ps_offer_score_df.fillna(0, inplace=True)
    ps_referral_score_df.fillna(0, inplace=True)
    ps_reporting_score_df.fillna(0, inplace=True)
    ps_exp_year_score_df.fillna(0, inplace=True)

    with cc.connect_cassandra(staging_keyspace) as cas_con:

        ms_calc_tot_exp_insert_prepare = cas_con.session.prepare(
            ms_calc_tot_exp_insert_query)
        ms_calc_domain_insert_prepare = cas_con.session.prepare(
            ms_calc_domain_insert_query)
        ms_calc_skillset_insert_prepare = cas_con.session.prepare(
            ms_calc_skillset_insert_query)
        ms_calc_cert_trend_insert_prepare = cas_con.session.prepare(
            ms_calc_cert_trend_insert_query)

        ps_calc_education_insert_prepare = cas_con.session.prepare(
            ps_calc_education_insert_query)
        ps_calc_cert_insert_prepare = cas_con.session.prepare(
            ps_calc_cert_insert_query)
        ps_calc_domain_insert_prepare = cas_con.session.prepare(
            ps_calc_domain_insert_query)
        ps_calc_reliability_insert_prepare = cas_con.session.prepare(
            ps_calc_reliability_insert_query)
        ps_calc_skillset_insert_prepare = cas_con.session.prepare(
            ps_calc_skillset_insert_query)
        ps_calc_interview_insert_prepare = cas_con.session.prepare(
            ps_calc_interview_insert_query)
        ps_calc_offers_insert_prepare = cas_con.session.prepare(
            ps_calc_offers_insert_query)
        ps_calc_referral_insert_prepare = cas_con.session.prepare(
            ps_calc_referral_insert_query)
        ps_calc_reporting_insert_prepare = cas_con.session.prepare(
            ps_calc_reporting_insert_query)
        ps_calc_exp_insert_prepare = cas_con.session.prepare(
            ps_calc_exp_insert_query)

        for _, row in ms_tot_exp_score_df.iterrows():
            cas_con.session.execute(
                ms_calc_tot_exp_insert_prepare, (batch_id, row['emp_id'],
                                                 row['total_exp'],
                                                 row['total_switch'],
                                                 row['switch_rel'], row['total_exp_ratio']))

        logger.debug('MS total experience score inserted')

        for _, row in ms_domain_score_df.iterrows():
            cas_con.session.execute(
                ms_calc_domain_insert_prepare, (batch_id, row['emp_id'], row['ms_domain_score']))

        logger.debug('MS domain score inserted')

        for _, row in ms_skill_set_score_df.iterrows():
            cas_con.session.execute(ms_calc_skillset_insert_prepare,
                                    (batch_id, row['emp_id'], row['ms_skill_set_score']))

        logger.debug('MS skillset score inserted')

        for _, row in ms_cert_trend_score_df.iterrows():
            cas_con.session.execute(
                ms_calc_cert_trend_insert_prepare, (batch_id, row['emp_id'], row['ms_cert_score']))

        logger.debug('MS certificate trend score inserted')

        for _, row in ps_education_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_education_insert_prepare, (batch_id, row['emp_id'], row['education_score']))

        logger.debug('PS education score inserted')

        for _, row in ps_valid_cert_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_cert_insert_prepare, (batch_id, row['emp_id'], row['cert_score']))

        logger.debug('PS certificate score inserted')

        for _, row in ps_domain_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_domain_insert_prepare, (batch_id, row['emp_id'], row['domain_score']))

        logger.debug('PS domain score inserted')

        for _, row in ps_reliability_score_df.iterrows():
            cas_con.session.execute(ps_calc_reliability_insert_prepare, (
                batch_id, row['emp_id'], row['total_exp'], row['total_switch'],
                row['switch_rel'], row['switch_rel_count'], row['rel_score1'], row['rel_score2']))

        logger.debug('PS reliability score inserted')

        for _, row in ps_skill_set_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_skillset_insert_prepare, (batch_id, row['emp_id'], row['skill_set_score']))

        logger.debug('PS skillset score inserted')

        for _, row in ps_interview_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_interview_insert_prepare, (batch_id, row['emp_id'], row['interview_score']))

        logger.debug('PS interview score inserted')

        for _, row in ps_offer_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_offers_insert_prepare, (batch_id, row['emp_id'], row['offers_score']))

        logger.debug('PS offers score inserted')

        for _, row in ps_referral_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_referral_insert_prepare, (batch_id, row['emp_id'], row['referral_score']))

        logger.debug('PS referral score inserted')

        for _, row in ps_reporting_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_reporting_insert_prepare, (batch_id, row['emp_id'], row['reporting_score']))

        logger.debug('PS reporting score inserted')

        for _, row in ps_exp_year_score_df.iterrows():
            cas_con.session.execute(
                ps_calc_exp_insert_prepare, (batch_id, row['emp_id'], row['exp_year_score']))

        logger.debug('PS experience score inserted')

        data_frames = [ms_tot_exp_score_df, ms_domain_score_df, ms_skill_set_score_df, ms_cert_trend_score_df,
                    ps_education_score_df, ps_valid_cert_score_df, ps_domain_score_df,
                    ps_reliability_score_df, ps_skill_set_score_df, ps_interview_score_df,
                    ps_offer_score_df, ps_referral_score_df, ps_reporting_score_df, ps_exp_year_score_df]

        overall_score_df = reduce(lambda left, right: pd.merge(
            left, right, on='emp_id', how='outer'), data_frames)

        score_data_rel_cols = [
            "emp_id", "cert_score", "ms_cert_score", "domain_score",
            "ms_domain_score", "education_score", "interview_score",
            "rel_score2", "skill_set_score", "ms_skill_set_score",
            "total_exp_ratio", "reporting_score", "referral_score",
            "no_of_offers_score", "exp_year_score"
        ]
        
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

        score_details_table_df = cas_con.query_result_set_to_pandas(cas_con.session,
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
            insertion_prepare = cas_con.session.prepare(
                score_details_insert_query)
            logger.debug(f'Query execution prepare - {insertion_prepare}')

            for index, row in insert_df.iterrows():
                cas_con.session.execute(insertion_prepare, (
                    row['emp_id'], 'PR_' +
                    row['emp_id'], 'VR_' + row['emp_id'],
                    str(row['cert_score']), str(row['ms_cert_score']),
                    requested_user, datetime.now(), str(row['domain_score']),
                    str(row['ms_domain_score']), str(row['education_score']),
                    str(row['exp_year_score']),
                    str(row['total_exp_ratio']), str(row['interview_score']),
                    str(row['rel_score2']), str(row['skill_set_score']),
                    str(row['no_of_offers_score']), str(
                        row['prscore_derived']),
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
            logger.debug(
                f'Query execution prepare - {history_insertion_prepare}')

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
                    str(row['job_longevity_point']), str(
                        row['niche_skill_point']),
                    str(row['noofoffers_point']), str(row['prscore_table']),
                    str(row['referrals_point']), str(row['reporting_point']),
                    str(row['skillset_population_point']), str(
                        row['special_rating_point']),
                    str(row['status']), str(row['vrscore_table'])))

                logger.info(
                    f'Out of {overall_rel_score_df.shape[0]}, {index+1} - inserted in score_history_detail_info table.')

                cas_con.session.execute(detail_update_prepare, (
                    row['emp_id'], 'PR_' +
                    row['emp_id'], 'VR_' + row['emp_id'],
                    str(row['cert_score']), str(row['ms_cert_score']),
                    requested_user, datetime.now(), str(row['domain_score']),
                    str(row['ms_domain_score']), str(row['education_score']),
                    str(row['exp_year_score']),
                    str(row['total_exp_ratio']), str(row['interview_score']),
                    str(row['rel_score2']), str(row['skill_set_score']),
                    str(row['no_of_offers_score']), str(
                        row['prscore_derived']),
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


if __name__ == '__main__':
    main()
    logger.info('completed.')
    sys.exit(0)
