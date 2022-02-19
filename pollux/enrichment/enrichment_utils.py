from abc import ABC, abstractmethod

class EnrichmentData(ABC):

    @staticmethod
    @abstractmethod
    def generate_significant_timestamps(customer):
        """If the new data adds to the significant events,
            it should return the timestamps for a customer"""
        pass

    @abstractmethod
    def update_cohort(self, cohort):
        """Decides how to update the customers with the pulled data"""
        pass

    @abstractmethod
    def query_data(self, customer_ids=None):
        """Makes a call to bq or other sources and pulls the data

        Stores the data as one or more attributes"""
