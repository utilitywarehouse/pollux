"""Main module."""

import warnings
warnings.simplefilter(action='ignore')
import pandas as pd
from pollux.queries import base_table_query
from bqhelper import BQConnection
import numpy as np
from collections import OrderedDict
from sortedcontainers import SortedSet
import logging
log = logging.getLogger(__name__)

bq = BQConnection()


class Cohort:

    def __init__(self, customers, base_data=None):
        self.customers = customers
        if base_data is not None:
            self.base_data = base_data


    def __len__(self):
        return len(self.customers)

    @classmethod
    def init_all_customers(cls):
        log.warning('Loading customer information, this may take a few minutes')
        query = base_table_query()
        base_data = bq.get_df_from_query(query)
        base_data = base_data
        customers = [Customer(row) for row in base_data.itertuples()]
        log.info('Customers were loaded')
        return cls(customers=customers, base_data=base_data)


    def generate_cohort(self, cohort_function):
        cohort_customers = [cus for cus in self.customers if cohort_function(cus)]
        return Cohort(customers=cohort_customers)

    #TODO load(attribute) to update attribute on all customers


class Customer:

    def __init__(self, row):
        self.base_row = row
        timestamps = [self.account_churn_date,self.end_of_onboarding_period,
            self.first_service_live_date, self.end_of_onboarding_period, self.sign_up_date]
        self.significant_timestamps = SortedSet([stamp for stamp in timestamps if stamp])

    @classmethod
    def from_id(cls, account_id):
        query = base_table_query([account_id])
        data = bq.get_df_from_query(query)
        for i, row in data.iterrows():
            break
        return cls(row=row)

    def __repr__(self):
        return f'Customer: {self.account_number}'


    @property
    def customer_id(self):
        return self.account_number

    @property
    def account_number(self):
        return self.base_row.account_number

    @property
    def sign_up_date(self):
        return self.base_row.sign_up_date

    @property
    def account_churn_date(self):
        return self.base_row.churn_date

    @property
    def end_of_onboarding_period(self):
        return self.base_row.end_of_onboarding_period

    @property
    def first_service_live_date(self):
        return self.base_row.first_service_live_date

    @property
    def property_occupier_status_at_sign_up(self):
        return self.base_row.property_occupier_status_at_sign_up

    @property
    def sign_up_source(self):
        return self.base_row.sign_up_source

    @property
    def referrers_account_number(self):
        return self.base_row.referrers_account_number

    @property
    def account_status(self):
        return self.base_row.account_status
