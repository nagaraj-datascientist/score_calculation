'''This Module is for Market score calculation
'''

import logging.config

import pandas as pd


class MarketScoreCalculator:

    '''MarketScoreCalculator class defines all the market score functions
        Functions:
            total_exp_with_population
            domain_with_population
            skill_set_with_population
            certificate_population_trend
    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def total_exp_with_population(self, person_work_agg_df,
                                  exp_ratio_df, join_col='total_exp'):
        '''Calculates total experience with population and
                            returns total experience score.
            Inputs:
                person_work_agg_df  : aggregated person work data
                exp_ratio_df        : experience ratio data
                join_col            : joining column
                                        default value is total_exp
            Outputs:
                tot_exp_score_df    : total experience score
        '''

        # 100 - Experience ratio numbers. Low ratio will get high score.
        exp_ratio_df.loc[:, f'{join_col}_ratio'] = (
            100 - exp_ratio_df[f'{join_col}_ratio'])

        # Merging experience ratio to person with total experience.
        tot_exp_score_df = pd.merge(person_work_agg_df,
                                    exp_ratio_df, on=join_col, how='left')

        tot_exp_score_df[f'{join_col}_ratio'].fillna(0, inplace=True)

        return tot_exp_score_df

    def domain_with_population(self, person_work_df,
                               domain_ratio_df, join_col='domain'):
        '''Merge person work data to domain ratio data and
                                    returns domain score
            Inputs:
                person_work_df  : person work data
                domain_ratio_df : domain ratio data
                join_col        : joining column
                                    default value is domain
            Outputs:
                domain_score_df : domain score
         '''

        # Merging domain ratio data with person_work(contains domain details)
        domain_score_df = pd.merge(person_work_df,
                                   domain_ratio_df, on=join_col, how='left')

        domain_score_df[f'{join_col}_ratio'].fillna(0, inplace=True)

        return domain_score_df

    def skill_set_with_population(self, person_tech_df, skill_set_ratio_df,
                                  join_col='technology_description'):
        '''Merge person tech data to skill set ratio data and
                            returns skill set score
            Inputs:
                person_tech_df      : person tech data
                skill_set_ratio_df  : skill set ratio data
                join_col            : joining column
                                        default value is
                                            technology_description
            Outputs:
                skill_set_score_df : skill set score (pandas dataframe)
         '''

        # Merging skill set ratio with person_tech(contains skill set details)
        skill_set_score_df = pd.merge(person_tech_df, skill_set_ratio_df,
                                      on=join_col, how='left')

        skill_set_score_df[f'{join_col}_ratio'].fillna(0, inplace=True)

        return skill_set_score_df

    def certificate_with_trend(self, person_cert_df,
                               cert_trend_ratio_df,
                               join_col='certificate_name'):
        '''Merge person cert data to cert trend ratio data and
                                returns cert trend score
            Inputs:
                person_cert_df      : person cert data
                cert_trend_ratio_df : cert trend ratio data
                join_col            : joining column
                                        default value is certificate_name
            Outputs:
                cert_trend_score_df : cert trend score (pandas dataframe)
         '''

        # Merging certificate trend ratio with person_cert(contains cert)
        cert_trend_score_df = pd.merge(person_cert_df, cert_trend_ratio_df,
                                       on=join_col, how='left')

        cert_trend_score_df[f'{join_col}_ratio'].fillna(0, inplace=True)

        return cert_trend_score_df
