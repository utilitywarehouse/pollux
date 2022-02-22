# Pollux

## About

Pollux is an initiative to create a programming gateway to our customers.
Each customer is represented as an object with properties that can be queried from data base and exposed.
This has the benefit that other downstream applications and tools can make use of an interface and other people's analytical work.

The form of object/class about a customer is not necessarily a natural choice, as the data usually has a very different structure underneath.

### Installation

Running
```
pip install -r requirements.txt
```
should get all external requirements.
Internally we'll need  `bqhelper` for the big query accesses.
Git clone and run a local install, e.g.
```
git clone git@github.com:utilitywarehouse/bqhelper.git
cd bqhelper
python setup.py develop
```


## Initialisation

### Individual Customers

This usage is the exception, but can potentially be useful if we need to answer queries about an individual.

```
from pollux import Customer
customer = Customer.from_id('9693465') # Customer ids are strings
customer.first_service_live_date
```
which would return `datetime.date(2022, 1, 8)`.

### Cohorts

Cohorts are following the `Sequence` protocol, meaning they behave like lists of `Customer`s.

The most common way to initialise a cohort would be to initialise all of them at once.

```
from pollux import Cohort
customer_base = Cohort.init_all_customers()
assert len(customer_base) > 2e6
```

For coding and debugging getting a sample can be just as useful, but will take considerably less time.

```
sample_cohort = Cohort.init_sample()
```

Because our overall customerbase (including churned) is in the millions, it can be sensible to create new Cohorts on subsets of customers that share a property.

```
def post_2020_customers(customer):
    return customer.sign_up_date >= date(2021,1,1)


post_2020_cohort = customer_base.generate_cohort(post_2021_customers)
```

## Enrichments

So far, we only have queried data that sits in a single table. However, customers generate a wealth of information.
Naturally, we do not want to load all information of all customers whenever we initialise the full cohort of customers, but rather load only the data necessary for our use case. To do this, we can use `Enrichment`s.
Enrichments are classes that allow the system to query new data and allocate it among the cohort. An `Enrichment` needs two methods to work,
`query_data(self)` which loads the data from the warehouse and `update_cohort(self, cohort)`.

For example, the `BroadBand` `Enrichment` looks like this

```{python}

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
```

Because `Enrichments` interface  bulk data and customer specific data, they can be optimised for speed by use case. In the above example the grouping by customer id is a fairly effective way of creating a lookup table for further allocation.
The `Enrichments` are normally stored in `pollux/enrichment/enrichments.py`, but can also be used adhoc.
At the end of `enrichments.py` is a registry, where `Enrichment`'s are effectively named.

```
registry = {
    'broadband':BroadBandData
}
```
Any further enrichments are either registered there, or the registry is updated before initialising a cohort

```
from pollux import registry

class MyEnrichment(EnrichmentData):

  [...]

registry['my_enrichment'] = MyEnrichment

cohort.update_customers('my_enrichment', registry=registry)
```
