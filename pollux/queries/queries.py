from pollux.queries.query_utils import query_generator

base_table_query = """
    SELECT account_number,
    sign_up_date,
    churn_date,
    account_status,
    end_of_onboarding_period,
    first_service_live_date,
    property_occupier_status_at_sign_up,
    sign_up_source,
    referral_type,
    referral_type_code,
    referrers_account_number
FROM uw-data-models-prod.customer_dataform_models.customer_base as cb
WHERE account_status != 'TML'"""

base_table_identifier = 'cb.account_number'


broadband_query = """
SELECT *
FROM uw-data-models-prod.telecoms_dataform_models.broadband_base as bb
ORDER BY bb.broadband_service_start_date
"""

broadband_identifier = 'bb.account_number'

is_query = """
SELECT * FROM `uw-data-models-prod.customer_dataform_models.customer_initial_services` as iserv
"""

initial_services_identifier = 'iserv.account_number'

cb_v2_query_string = """
SELECT * FROM `uw-data-models-prod.cashback_dataform_models.cashback_card_v2_base` as cbc
"""

cb_v2_identifier = 'cbc.account_number'

cb_v3_query_string = """
SELECT * FROM `uw-data-models-prod.cashback_dataform_models.cashback_card_v3_base` as cbc
"""

cb_v3_identifier = 'cbc.account_number'

base_table_query = query_generator(base_table_query, base_table_identifier)
bb_query = query_generator(broadband_query, broadband_identifier)
initial_services_query = query_generator(is_query, initial_services_identifier)
cb_v2_query = query_generator(cb_v2_query_string, cb_v2_identifier)
cb_v3_query = query_generator(cb_v3_query_string, cb_v3_identifier)
