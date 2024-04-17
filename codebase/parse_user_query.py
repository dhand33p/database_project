import re
import os
import shutil
import ast

from handle_return_all_contain import handle_return_all_contain
from handle_from_return_all import handle_from_return_all
from handle_group import handle_group
from handle_delete_row import handle_delete_row
from handle_update_row import handle_update_row
from handle_make_table_query import handle_make_table_query
from handle_enter_query import handle_enter_query
from handle_condition_query import handle_condition_query
from upload_data_and_process_in_chunks import upload_data_and_process_in_chunks
from hash_join_on_chunked_data import efficient_hash_join
from handle_delete_table import handle_delete_table
from improved_manual_parse_query import improved_manual_parse_query
from list_tables import list_tables

def parse_user_query():
    joined_directory = None
    
    while True:
        user_query = input("DS> ")

        if user_query.upper() == "EXIT":
            break
            
        try:
            patterns = {
                'FROM_RETURN_ALL_CONTAIN': re.compile(r"FROM\s+['\"]?([^'\"']+?)['\"]?\s+RETURN\s+ALL\s+CONTAIN\s+(\d+)", re.IGNORECASE),
                'FROM_RETURN_SPECIFIC_COLUMNS_CONTAIN': re.compile(r"FROM\s+['\"]?([^'\"']+?)['\"]?\s+RETURN\s+(\[.*?\])\s+CONTAIN\s+(\d+)", re.IGNORECASE),
                'FROM_RETURN_SPECIFIC_COLUMNS': re.compile(r"FROM\s+['\"]?([^'\"']+?)['\"]?\s+RETURN\s+(\[.*?\])", re.IGNORECASE),
                'FROM_RETURN_ALL': re.compile(r"FROM\s+['\"]?([^'\"']+?)['\"]?\s+RETURN\s+(ALL)", re.IGNORECASE),
                'GROUP': re.compile(r"FROM\s+['\"]?([^'\"']+?)['\"]?\s+GROUP\s+['\"]?([^'\"']+?)['\"]?\s+RETURN\s+(\[.*\]|ALL)", re.IGNORECASE),          
                'DELETE_ROW': re.compile(r"FROM\s+['\"]?([^'\"']+?)['\"]?\s+DELETE\s+['\"]?([^'\"']+?)['\"]?\s*(<=|>=|!=|==|<|>)\s+('?[^']+'?|\d+\.?\d*)", re.IGNORECASE),
                'DELETE_TABLE': re.compile(r"DELETE\s+['\"]?([^'\"].*?)['\"]?$", re.IGNORECASE),
                'UPDATE': re.compile(r"UPDATE\s+['\"]?([^'\"']+?)['\"]?\s+GIVE\s+['\"]?([^'\"']+?)['\"]?\s*(<=|>=|!=|==|<|>)\s+('?[^']+'?|\d+\.?\d*)\s+CHANGE\s+['\"]?([^'\"']+?)['\"]?\s+=\s+('?[^']+'?|\d+\.?\d*)", re.IGNORECASE),
                'MAKE': re.compile(r"MAKE\s+['\"]?([^'\"']+?)['\"]?\s+GIVE\s+(.*)\s+TYPE\s+(.*)", re.IGNORECASE),
                'ENTER': re.compile(r"ENTER\s+['\"]?([^'\"']+?)['\"]?\s+VALUES\s+(\[.*\])", re.IGNORECASE | re.DOTALL),
                'UPLOAD': re.compile(r"UPLOAD\s+['\"]?(.+?)['\"]?$", re.IGNORECASE),
                # old 'JOIN': re.compile(r"JOIN\s+['\"]?(.+?)['\"]?\s+AND\s+['\"]?(.+?)['\"]?\s+LINKING\s+['\"]?(.+?)['\"]?\s+TO\s+['\"]?(.+?)['\"]?\s+AS\s+['\"]?(.+?)['\"]?$", re.IGNORECASE),
                'JOIN': re.compile(r"JOIN\s+['\"]?(.+?)['\"]?\s+AND\s+['\"]?(.+?)['\"]?\s+LINKING\s+['\"]?(.+?)['\"]?\s+TO\s+['\"]?(.+?)['\"]?\s+AS\s+['\"]?(.+?)['\"]?$", re.IGNORECASE),
                'SEE_TABLES': re.compile(r"SEE tables", re.IGNORECASE)

            }

            # Process the query based on its type
            for key, pattern in patterns.items():
                match = pattern.match(user_query)
                if match:
                    query_data = match.groups()
                    # Call specific handler functions based on the query type
                    if key == 'FROM_RETURN_ALL_CONTAIN':
                        table_name, row_limit = query_data
                        print(handle_return_all_contain(table_name, None, int(row_limit)))

                    elif key == 'FROM_RETURN_SPECIFIC_COLUMNS_CONTAIN':
                        table_name, return_columns, row_limit = query_data
                        return_columns = ast.literal_eval(return_columns)
                        print(handle_return_all_contain(table_name, return_columns, int(row_limit)))

                    elif key == 'FROM_RETURN_SPECIFIC_COLUMNS':
                        table_name, return_columns = query_data
                        print(handle_from_return_all(table_name, return_columns=return_columns))

                    elif key == 'FROM_RETURN_ALL':
                        table_name = query_data[0]
                        print(handle_from_return_all(table_name, return_columns="ALL"))
                        
                    elif key == 'GROUP':
                        table_name = query_data[0]  # Should correctly get 'new_table'
                        group_column = query_data[1]  # Should correctly get 'ID'

                        # Process the return specification
                        return_spec_raw = query_data[2]  # Should correctly get '[sum("Value"), max("Value")]'
                        if return_spec_raw.strip().upper() == "ALL":
                            return_specification = ["ALL"]
                        else:
                            return_specification = [spec.strip().replace("'", "").replace("\"", "") for spec in return_spec_raw.strip("[]").split(',')]

                        result = handle_group(table_name, group_column, return_specification)
                        print(result)


                    elif key == 'DELETE_ROW':
                        table_name = query_data[0]
                        column_name = query_data[1].strip("'\"")   
                        operator = query_data[2]
                        value = query_data[3].strip("'\"")

                        print(handle_delete_row(table_name, column_name, operator, value))
                        print('')
                    elif key == 'DELETE_TABLE':
                        table_name = query_data[0]
                        print(handle_delete_table(table_name))
                        print('')
                    elif key == 'UPDATE':
                        table_name = query_data[0]
                        condition_column = query_data[1].strip("'\"")
                        operator = query_data[2]
                        condition_value = query_data[3].strip("'\"")
                        update_column = query_data[4].strip("'\"")
                        update_value = query_data[5].strip("'\"")

                        print(handle_update_row(table_name, condition_column, operator, condition_value, update_column, update_value))
                        print('')
                        
                    elif key == 'MAKE':
                        table_name = query_data[0]
                        columns_str = query_data[1]
                        types_str = query_data[2]

                        columns = [col.strip() for col in columns_str.split(',')]
                        types = [typ.strip() for typ in types_str.split(',')]

                        print(handle_make_table_query(table_name, columns, types))
                        print('')
                    elif key == 'ENTER':
                        table_name = query_data[0]
                        values_str = query_data[1]
                        print(handle_enter_query(table_name, values_str))
                        print('')
                    elif key == 'UPLOAD':
                        file_path = query_data[0]
                        print(upload_data_and_process_in_chunks(file_path))
                        print('')
                    elif key == 'JOIN':
                        table1, table2, column1, column2, joined_table_name = query_data
                        folder1_path = os.path.join("table_chunks", table1)
                        folder2_path = os.path.join("table_chunks", table2)

                        joined_directory = os.path.join("table_chunks", joined_table_name)
                        os.makedirs(joined_directory, exist_ok=True)

                        efficient_hash_join(folder1=folder1_path, folder2=folder2_path, join_column1=column1, join_column2=column2, output_folder=joined_directory)
                        print(f"Joined tables {table1} and {table2} on columns '{column1}' and '{column2}' respectively and saved to '{joined_directory}'")
                        print('')



                    elif key == 'SEE_TABLES':
                        print(list_tables())

                    break

            if not match and user_query.startswith("FROM ") and " GIVE " in user_query:
                query_result = improved_manual_parse_query(user_query)
                table_name = query_result['table_name']
                column_name = query_result['column_name']
                operator = query_result['operator']
                value = query_result['value']
                return_columns = query_result['return_columns']
                sort_by_column = query_result['sort_by_column']
                row_limit = query_result['row_limit']

                print(handle_condition_query(table_name, column_name, operator, value, return_columns, sort_by_column, row_limit))
                print('')
                match = True

            if not match:
                print("Invalid query format.") 

        except FileNotFoundError as e:
            print(f"Error: {e}. Please check the table name or file path.")
            continue 

        except Exception as e:
            print(f"An error occurred: {e}")
            continue 
            
    if joined_directory and os.path.exists(joined_directory):
        handle_delete_table(joined_table_name)
        print(f"Deleted joined table '{joined_table_name}'.")
    print("Exited")








