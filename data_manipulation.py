'''This module is used for data preparation.'''

import logging.config
from datetime import datetime, timedelta

import numpy as np

logger = logging.getLogger(__name__)


class DataManipulation:
    '''DataManipulation class defines data preparation for the score

    Methods
    -------

    active_profiles(self, dataframe, date_col='updated_time',
                        active_days=30):
        Returns the active profile for the given dataframe
                                    based on the active days.
                By default active days is 30 days.

        Args:
            dataframe (pandas dataframe): dataframe of a profile data
            date_col (str, optional): columns to apply active days.
                                        Defaults to 'updated_time'.
            active_days (int, optional): no. of active days from current date.
                                        Defaults to 30.

        Returns:
            pandas dataframe: dataframe of active profile

    certificate_trend(self, certificate_df,
                          compl_col='certificate_completion_date',
                          active_days=730):
        Returns latest certificates data based on the active days.
                By default active days is 730 days.

        Args:
            certificate_df (pandas dataframe): dataframe of certificate data
            compl_col (str, optional): column to apply active days condition.
                                    Defaults to 'certificate_completion_date'.
            active_days (int, optional): no. of active days from current date.
                                        Defaults to 730.

        Returns:
            pandas dataframe: dataframe of latest certificates

    work_aggregation(self, dataframe, work_start_date='start_date',
                         work_end_date='end_date',
                         emp_type_col='employeement_type'):

        Aggregate candidate work information, returns the
                                                    aggregated dataframe
        Args:
            dataframe (pandas dataframe): active profile dataframe (pandas dataframe)
            work_start_date (str): start date of work column
                                Defaults to 'start_date'.
            work_end_date (str): end date of work column
                                Defaults to 'end_date'
            emp_type_col (str): type of employment column
                                Defaults to 'employeement_type'
        Returns:
            pandas dataframe : dataframe of work aggregated value

    category_ratio(self, dataframe, category_col):
        Calculate category ratio, returns the ratio dataframe

        Args:
            dataframe (pandas dataframe): dataframe with categorical
            category_col (str): category column name

        Returns:
            pandas dataframe: dataframe of a categorical value ratio

    groupby_agg_func(self, dataframe, agg_col, groupby_col='emp_id',
                         agg_func='sum', agg_col_name='score'):
        Aggregates values of a dataframe for given groupby column and aggregate column

        Args:
            dataframe (pandas dataframe): Dataframe needs to groupby
            agg_col (str, optional): column name to be aggregated.
            groupby_col (str): Group by column name
                                            Defaults to 'emp_id'.
            agg_func (str, optional): Aggregate function. Defaults to 'sum'.
            agg_col_name (str, optional): column name to be saved.
                                            Defaults to 'score'.

        Returns:
            pandas dataframe: Aggregated datafame
    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def active_profiles(self, dataframe, date_col='updated_time',
                        active_days=30):
        '''Returns the active profile for the given dataframe
                                    based on the active days.
                By default active days is 30 days.

        Args:
            dataframe (pandas dataframe): dataframe of a profile data
            date_col (str, optional): columns to apply active days.
                                        Defaults to 'updated_time'.
            active_days (int, optional): no. of active days from current date.
                                        Defaults to 30.

        Returns:
            pandas dataframe: dataframe of active profile
        '''

        active_datetime = datetime.today() - timedelta(active_days)
        self.logger.debug(f'Active profiles from the date-{active_datetime}')
        active_profile_df = dataframe[dataframe[date_col] >= active_datetime]

        return active_profile_df

    def certificate_trend(self, certificate_df,
                          compl_col='certificate_completion_date',
                          active_days=730):
        '''Returns latest certificates data based on the active days.
                By default active days is 730 days.

        Args:
            certificate_df (pandas dataframe): dataframe of certificate data
            compl_col (str, optional): column to apply active days condition.
                                    Defaults to 'certificate_completion_date'.
            active_days (int, optional): no. of active days from current date.
                                        Defaults to 730.

        Returns:
            pandas dataframe: dataframe of latest certificates
        '''

        active_datetime = datetime.today() - timedelta(active_days)
        self.logger.debug(
            f'Certificate trends from the date - {active_datetime}')
        cert_trend_df = certificate_df[certificate_df[compl_col]
                                       >= active_datetime]

        return cert_trend_df

    def work_aggregation(self, dataframe, work_start_date='start_date',
                         work_end_date='end_date',
                         emp_type_col='employeement_type'):
        '''
            Aggregate candidate work information, returns the
                                                        aggregated dataframe
            Args:
                dataframe (pandas dataframe): active profile dataframe (pandas dataframe)
                work_start_date (str): start date of work column
                                    Defaults to 'start_date'.
                work_end_date (str): end date of work column
                                    Defaults to 'end_date'
                emp_type_col (str): type of employment column
                                    Defaults to 'employeement_type'
            Returns:
                pandas dataframe : dataframe of work aggregated value

        '''

        dataframe.loc[:, 'exp_date_diff'] = (
            dataframe[work_end_date].values
            - dataframe[work_start_date].values)

        dataframe.loc[:, 'exp_days'] = (dataframe['exp_date_diff']
                                        // np.timedelta64(1, 'D'))

        dataframe.loc[:, 'exp_years'] = (dataframe['exp_date_diff']
                                         // np.timedelta64(1, 'Y'))

        dataframe.loc[:, 'contract_2y'] = ~(
            (dataframe[emp_type_col] == 'Contracting')
            & (dataframe['exp_years'] <= 2))

        work_agg_df = (dataframe.groupby('emp_id').agg(
            total_exp=('exp_years', 'sum'),
            total_switch=('work_exp_id', 'count'),
            switch_rel=('contract_2y', 'sum'),
            no_of_domain=('domain', 'nunique'),
        ).astype({'switch_rel': 'int'}).reset_index())

        return work_agg_df

    def category_ratio(self, dataframe, category_col):
        '''Calculate category ratio, returns the ratio dataframe

        Args:
            dataframe (pandas dataframe): dataframe with categorical
            category_col (str): category column name

        Returns:
            pandas dataframe: dataframe of a categorical value ratio
        '''
        category_ratio_df = (
            (dataframe[category_col].value_counts(normalize=True) * 100)
            .astype('int')
            .to_frame()
            .reset_index()
            .rename(columns={'index': category_col,
                             category_col: f'{category_col}_ratio'}))

        return category_ratio_df

    def groupby_agg_func(self, dataframe, agg_col, groupby_col='emp_id',
                         agg_func='sum', agg_col_name='score'):
        '''Aggregates values of a dataframe for given groupby column and aggregate column

        Args:
            dataframe (pandas dataframe): Dataframe needs to groupby
            agg_col (str, optional): column name to be aggregated.
            groupby_col (str): Group by column name
                                            Defaults to 'emp_id'.
            agg_func (str, optional): Aggregate function. Defaults to 'sum'.
            agg_col_name (str, optional): column name to be saved.
                                            Defaults to 'score'.

        Returns:
            pandas dataframe: Aggregated datafame
        '''
        agg_df = dataframe.groupby(groupby_col).agg(
            agg_col_name=(agg_col, agg_func)).reset_index()

        agg_df.rename(columns={'agg_col_name': agg_col_name}, inplace=True)

        return agg_df
