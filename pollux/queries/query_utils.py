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
