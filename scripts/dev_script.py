from pollux.customer import Cohort
from pollux.enrichment.enrichments import registry
from datetime import date

customer_base = Cohort.init_all_customers()

def post_2021_customers(customer):
    return customer.sign_up_date > date(2021,1,1)

late_cohort = customer_base.generate_cohort(post_2021_customers)
late_cohort.update_customers('broadband')


def broad_band_user(customer):
    return customer.broadband_data is not None


broad_band_users = late_cohort.generate_cohort(broad_band_user)
