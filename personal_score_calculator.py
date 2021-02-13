'''This Module is for Personal Score Calculation
'''
import logging.config
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class PersonalScoreCalculator:
    '''PersonalScoreCalculator class defines all the personal scroe functions
        Methods
        ------
            education_score
            valid_certificate_score
            domain_score
            skill_set_score
            reliablity_score
            interview_score
    '''

    INTERVIEW_STATUS = ['Selected', 'Rejected']

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def education_type_score(self, employee_edu_df, static_score_meta_df):
        '''Calculates Eduction type score

        Args:
            employee_edu_df (dataframe): Person employee details dataframe
            static_score_meta_df (dataframe): Static score meta table dataframe

        Returns:
            pandas dataframe: Education type score based on the static score meta
        '''

        edu_static_score_df = static_score_meta_df[static_score_meta_df['score_name']
                                                   == 'education_type']

        edu_static_score_df = edu_static_score_df.rename(
            columns={"score": "education_score"})

        edu_static_score_df = edu_static_score_df.astype(
            {"education_score": 'int'})

        edu_score_df = pd.merge(
            employee_edu_df, edu_static_score_df,
            left_on='education_type_desc', right_on='category', how='left')

        return edu_score_df

    def exp_year_score(self, exp_agg_df, static_score_meta_df):
        '''Calculates Experience score

        Args:
            exp_agg_df (dataframe): person with total experience aggregated data
            static_score_meta_df (dataframe): Static score meta table dataframe

        Returns:
            pandas dataframe: Total Exp year score based on the static score meta
        '''
        exp_static_score_df = static_score_meta_df[static_score_meta_df['score_name']
                                                   == 'experience_year']
        exp_static_score_df = exp_static_score_df.astype({"range_start": 'int',
                                                          "range_end": 'int'})

        exp_agg_df = exp_agg_df[['emp_id', 'total_exp']]

        exp_ranges = []
        for _, row in exp_static_score_df.iterrows():
            exp = pd.DataFrame(
                range(row['range_start'], row['range_end']+1), columns=['exp_year'])
            exp['exp_year_score'] = int(row['score'])
            exp_ranges.append(exp)

        exp_static_score_details_df = pd.concat(exp_ranges)

        exp_score_df = pd.merge(exp_agg_df, exp_static_score_details_df,
                                how='left', left_on='total_exp', right_on='exp_year')
        return exp_score_df

    def valid_certificate_score(self, certificate_df, valid_years=5):
        '''Calculates certificate score for valid one
        based on valid_years parameter

        Args:
            certificate_df (dataframe): person certificate data
            valid_years (int, optional): valid years. Defaults to 5.

        Returns:
            dataframe: Certificate score for the valid on
        '''

        certificate_df.loc[:, 'valid_periods'] = (datetime.today()
                                                  - certificate_df[
            'certificate_completion_date'])

        certificate_df.loc[:, 'valid_years'] = (certificate_df['valid_periods']
                                                // np.timedelta64(1, 'Y'))

        valid_cert_df = certificate_df[certificate_df['valid_years']
                                       <= valid_years]

        valid_cert_score_df = (valid_cert_df.groupby('emp_id').agg(
            no_of_certs=('certificate_id', 'nunique')).reset_index())

        valid_cert_score_df.loc[:, 'cert_score'] = (
            valid_cert_score_df['no_of_certs'] * 10)

        return valid_cert_score_df

    def domain_score(self, work_agg_df):
        '''Calculates Domain score for the person with domain aggregated data

        Args:
            work_agg_df (dataframe): person with domain aggregated data

        Returns:
            dataframe: domain score for the person with domain agg. data
        '''

        domain_score_df = work_agg_df.copy()

        domain_score_df.loc[:, 'domain_score'] = (
            work_agg_df['no_of_domain'] * 10)

        return domain_score_df

    def skill_set_score(self, tech_df):
        '''Calculates skill set score

        Args:
            tech_df (dataframe): person tech data

        Returns:
            dataframe: skill set score for the person tech data
        '''

        tech_count_df = (tech_df.groupby('emp_id').agg(
            tech_count=('technology_description', 'nunique')).reset_index())

        tech_count_df.loc[:,
                          'skill_set_score'] = tech_count_df['tech_count'] * 10

        return tech_count_df

    def reliablity_score(self, work_agg_df):
        '''calculates Reliability score  based on the number of career switches

        Args:
            work_agg_df (dataframe): person with work aggregated data

        Returns:
            dataframe: reliablity score for the no of career switches
        '''

        work_agg_df['switch_rel_count'] = work_agg_df['switch_rel'] + 1

        work_agg_df.loc[:, 'rel_score1'] = (work_agg_df['total_exp'] * 10
                                            // work_agg_df['total_switch'])

        work_agg_df.loc[:, 'rel_score2'] = (work_agg_df['total_exp'] * 10
                                            // work_agg_df['switch_rel_count'])

        return work_agg_df

    def interview_score(self, interview_df, interview_status=INTERVIEW_STATUS):
        '''Calculates Interview score based on interview results

        Args:
            interview_df (dataframe ): interview data
            interview_status (list, optional): valid interview status.
                            Defaults to list ['Selected', 'Rejected']

        Returns:
            dataframe: interview score based on the results cummulatively.
        '''

        interview_df = interview_df[interview_df['int_status_desc'].isin(
            interview_status)]

        interview_df = interview_df.sort_values(['emp_id', 'int_date'])

        interview_df.loc[:, 'int_order'] = interview_df.groupby('emp_id')[
            'int_date'].rank('dense')

        interview_df.loc[:, 'status_value'] = (interview_df[
            'int_status_desc'].replace('Selected', 1).replace('Rejected', -1))

        interview_df.loc[:, 'interview_score'] = (interview_df['status_value']
                                                  * 10
                                                  * interview_df['int_order'])

        interview_score_df = (interview_df.groupby('emp_id').agg(
            interview_score=('interview_score', 'sum')).reset_index())

        return interview_score_df

    def no_of_offer_score(self, person_offer_agg_df):
        '''Calculates No of offers score for the person with no of offers aggregated data

        Args:
            person_offer_agg_df (dataframe): person with no of offers aggregated data

        Returns:
            dataframe: offer score for the person with no of offers agg. data
        '''

        offer_score_df = person_offer_agg_df.copy()

        offer_score_df.loc[:, 'no_of_offers_score'] = (
            person_offer_agg_df['no_of_offers'] * 10)

        return offer_score_df

    def referral_score(self, person_referral_agg_df):
        '''Calculates No of referral score for the person with no of referral aggregated data

        Args:
            person_referral_agg_df (dataframe): person with no of referral aggregated data

        Returns:
            dataframe: offer score for the person with no of referral agg. data
        '''

        referral_score_df = person_referral_agg_df.copy()

        referral_score_df.loc[:, 'referral_score'] = (
            person_referral_agg_df['no_of_referral'] * 10)

        return referral_score_df

    def reporting_score(self, person_reporting_agg_df):
        '''Calculates No of reporting score for the person with no of reporting aggregated data

        Args:
            person_reporting_agg_df (dataframe): person with no of reporting aggregated data

        Returns:
            dataframe: offer score for the person with no of reporting agg. data
        '''

        reporting_score_df = person_reporting_agg_df.copy()

        reporting_score_df.loc[:, 'reporting_score'] = (
            person_reporting_agg_df['no_of_reporting'] * 10)

        return reporting_score_df
