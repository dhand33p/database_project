import re

def improved_manual_parse_query(query):

    table_name = re.search(r"FROM\s+'?([^' ]+)'?", query).group(1)

    condition_part = re.search(r"GIVE\s+(.+?)\s*(?:RETURN|SORT BY|CONTAIN|$)", query).group(1)
    match = re.search(r"('?[^' ]+'?)\s*(<=|>=|!=|==|<|>)\s*(.+)$", condition_part)
    column_name, operator, value = match.groups()

    return_columns = []
    return_columns_match = re.search(r"RETURN\s+\[(.*?)\]", query)
    if return_columns_match:
        return_columns_str = return_columns_match.group(1)
        return_columns = [col.strip().replace("'", "").replace("\"", "") for col in return_columns_str.split(',')]

    sort_by_column = None
    sort_by_match = re.search(r"SORT BY\s+'?([^' ]+)'?", query)
    if sort_by_match:
        sort_by_column = sort_by_match.group(1)

    row_limit = None
    row_limit_match = re.search(r"CONTAIN\s+(\d+)", query)
    if row_limit_match:
        row_limit = int(row_limit_match.group(1))

    return {
        "table_name": table_name,
        "column_name": column_name.strip("'\""),
        "operator": operator,
        "value": value.strip("'\""),
        "return_columns": return_columns,
        "sort_by_column": sort_by_column,
        "row_limit": row_limit
    }

