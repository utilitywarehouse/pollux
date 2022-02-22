"""Main module."""

import warnings
warnings.simplefilter(action='ignore')
import pandas as pd
from pollux.queries.queries import base_table_query
from pollux.enrichment.enrichments import registry
from bqhelper import BQConnection
import numpy as np
from collections import OrderedDict
from copy import deepcopy
import logging
log = logging.getLogger(__name__)

bq = BQConnection()


class Cohort:

    #TODO Generate cohort from query that returns customer ids

    def __init__(self, customers, base_data=None, enrichments=None):
        self.customers = customers
        if base_data is not None:
            self.base_data = base_data

        if enrichments:
            self.enrichments = enrichments
        else:
            self.enrichments = []

        self.customer_lookup = {cus.customer_id:cus for cus in self.customers}

    def __len__(self):
        return len(self.customers)

    def __getitem__(self, a):
        return self.customers[a]

    @classmethod
    def init_all_customers(cls):
        log.warning('Loading customer information, this may take a few minutes')
        query = base_table_query()
        base_data = bq.get_df_from_query(query)
        base_data = base_data
        customers = [Customer(row) for row in base_data.itertuples()]
        log.info('Customers were loaded')
        return cls(customers=customers, base_data=base_data)

    @classmethod
    def init_sample(cls, sample_size=1000):
        sample_query = f"""SELECT * FROM
        uw-data-models-prod.customer_dataform_models.customer_base
        WHERE account_status != 'TML'
        LIMIT {sample_size}"""
        base_data = bq.get_df_from_query(sample_query)
        base_data = base_data
        customers = [Customer(row) for row in base_data.itertuples()]
        return cls(customers=customers, base_data=base_data)


    def generate_cohort(self, cohort_function):
        cohort_customers = [cus for cus in self if cohort_function(cus)]
        # The enrichments are inherited from the parent cohort
        return Cohort(customers=cohort_customers, enrichments=deepcopy(self.enrichments))


    def update_customers(self, enrichment_name, registry=registry):
        if enrichment_name not in registry:
            log.warning('Enrichment not registered')
        else:
            enrichment = registry[enrichment_name]()
            # Loads the data as specified by the cohort and updates the
            # customers with that data
            enrichment.query_data()
            enrichment.update_cohort(self)
            self.enrichments.append(enrichment_name)



class Customer:

    def __init__(self, row, additional_data=None):
        self.base_row = row
        self.bb_info = None


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
        return int(self.account_number)

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
