def transform_to_sql_str(item_list):
    if isinstance(item_list[0], str) :
        str_list = ["'"+item+"'" for item in item_list]
    else:
        str_list = [str(item) for item in item_list]
    return str.join(',', str_list)


def query_generator(query_string, identifier):

    def query_function(ids=None):

        query = query_string
        if ids is not None:
            query += f"""
                        AND
                        {identifier} IN ({transform_to_sql_str(ids)})"""
        return query

    return query_function




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

base_table_query = query_generator(base_table_query, base_table_identifier)
base_table_query()
