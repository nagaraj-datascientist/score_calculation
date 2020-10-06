'''This Module is for Personal Score Calculation
'''
import logging.config
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class PersonalScoreCalculator:
    '''PersonalScoreCalculator class defines all the personal scroe functions
        Functions:
            education_score
            valid_certificate_score
            domain_score
            skill_set_score
            reliablity_score
            interview_score
    '''

    EDUCATION_SCORE = {
        'education_type_desc': ['Postgraduate/Master of Engineering', 'Phd',
                                'Undergraduate/Bachelor of Engineering', 'High School'],
        'education_score': [75, 90, 50, 25]}

    INTERVIEW_STATUS = ['Selected', 'Rejected']

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def education_score(self, employee_edu_df, education_score=EDUCATION_SCORE):
        '''Calculates education score

        Args:
            employee_edu_df (dataframe): Person employee details dataframe

            education_score (dict, optional): Education score for every grade.
                                    Defaults to class variable EDUCATION_SCORE.

        Returns:
            pandas dataframe: Education score based on the grade.
        '''

        self.logger.debug(f'Education grade static score - {education_score}')
        edu_static_score_df = pd.DataFrame.from_dict(education_score)

        edu_score_df = pd.merge(
            employee_edu_df, edu_static_score_df, on='education_type_desc',
            how='left')

        return edu_score_df

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
