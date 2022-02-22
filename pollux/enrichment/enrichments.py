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


registry = {
    'broadband':BroadBandData
}
