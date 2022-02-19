from pollux.enrichment.enrichment_utils import EnrichmentData
from bqhelper import BQConnection
from sortedcontainers import SortedSet
import pollux.queries.queries as queries
import logging
log = logging.getLogger(__name__)

class BroadBandData(EnrichmentData):

        def query_data(self, customer_ids=None):
            conn = BQConnection('uw-data-models-prod')
            query = queries.bb_query(customer_ids)
            log.warning('Loading broadband data, this might take several minutes')
            self.broadband_data = conn.get_df_from_query(query)
            self.customer_ids = list(self.broadband_data.account_number.unique())
            self.grouped_data = {customer_id:group for customer_id, group in self.broadband_data.groupby('account_number')}
            log.warning('Broadband Data loading complete')

        @staticmethod
        def generate_significant_timestamps(customer):
            starts = [val for val in customer.broadband_data.broadband_service_start_date if val]
            ends = [val for val in customer.broadband_data.broadband_service_end_date if val]

            return SortedSet(starts + ends)

        def update_cohort(self, cohort):
            """Decides how to update the customers with the pulled data"""
            for cus in cohort.customers:
                if cus.customer_id in self.grouped_data:
                    cus.broadband_data = self.grouped_data[cus.customer_id]
                    timestamps = self.generate_significant_timestamps(cus)
                    cus.significant_timestamps = cus.significant_timestamps.union(timestamps)
                else:
                    cus.broadband_data = None


registry = {
    'broadband':BroadBandData
}
