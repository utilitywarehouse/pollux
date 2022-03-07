from pollux.enrichment.enrichment_utils import EnrichmentData
from bqhelper import BQConnection
import pollux.queries.queries as queries
import logging
import pandas as pd
log = logging.getLogger(__name__)

class BroadBandData(EnrichmentData):

        def query_data(self, customer_ids=None):
            conn = BQConnection('uw-data-models-prod')
            query = queries.bb_query(customer_ids)
            log.warning('Loading broadband data, this might take several minutes')
            self.broadband_data = conn.get_df_from_query(query)
            self.broadband_data['account_number'] = self.broadband_data.account_number.astype('int')
            self.customer_ids = list(self.broadband_data.account_number.unique())
            self.grouped_data = {customer_id:group for customer_id, group in self.broadband_data.groupby('account_number')}
            log.warning('Broadband Data loading complete')

        def update_cohort(self, cohort):
            """Decides how to update the customers with the pulled data"""
            empty_frame = pd.DataFrame(columns = self.broadband_data.columns)
            for cus in cohort:
                if cus.customer_id in self.grouped_data:
                    cus.broadband_data = self.grouped_data[cus.customer_id]
                else:
                    cus.broadband_data = empty_frame


class InitialServices(EnrichmentData):

    def query_data(self, customer_ids=None):
        conn = BQConnection('uw-data-models-prod')
        query = queries.initial_services_query(customer_ids)
        log.warning('Loading initial services data, this might take several minutes')
        self.initial_services_data = conn.get_df_from_query(query)
        log.warning('Loading complete')
        self.initial_services_data['account_number'] = self.initial_services_data.account_number.astype('int')
        log.warning('Initial services data loading complete')

    def update_cohort(self, cohort):
            empty_frame = pd.DataFrame(columns = self.initial_services_data.columns)
            for i, row in self.initial_services_data.iterrows():
                if row.account_number in cohort.customer_lookup:
                    cohort.customer_lookup[row.account_number].initial_services_data = row


class Cashback(EnrichmentData):

    def query_data(self, customer_ids=None):
        conn = BQConnection('uw-data-models-prod')
        v2_query = queries.cb_v2_query(customer_ids)
        v3_query = queries.cb_v3_query(customer_ids)
        log.warning('Loading cashback data, this might take several minutes')
        cb_v2_data = conn.get_df_from_query(v2_query)
        cb_v3_data = conn.get_df_from_query(v3_query)
        cb_v2_data['version'] = 'v2'
        cb_v3_data['version'] = 'v3'
        cb_data = pd.concat([cb_v2_data, cb_v3_data], axis=0)
        print(len(cb_data))
        cb_data['account_number'] = cb_data.account_number.astype('int')
        self.cb_data = cb_data
        self.customer_ids = list(self.cb_data.account_number.unique())
        self.grouped_data = {customer_id:group for customer_id, group in self.cb_data.groupby('account_number')}

        log.warning('Cashback data loading complete')

    def update_cohort(self, cohort):
        """Decides how to update the customers with the pulled data"""
        empty_frame = pd.DataFrame(columns = self.cb_data.columns)
        for cus in cohort:
            if cus.customer_id in self.grouped_data:
                cus.cb_data = self.grouped_data[cus.customer_id]
            else:
                cus.cb_data = empty_frame


registry = {
    'broadband':BroadBandData,
    'initial_services':InitialServices,
    'cashback':Cashback
}
