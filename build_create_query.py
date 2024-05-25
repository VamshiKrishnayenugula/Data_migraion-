def build_create_query(table_name, columns):
    query = f"CREATE TABLE {table_name} ("
    for column in columns:
        query += f"{column['name']} {column['type']},"
    return query.rstrip(',') + ")"

# Example usage
table_name = "example_table"
columns = [
    {"name": "id", "type": "INT"},
    {"name": "name", "type": "STRING"}
]
create_query = build_create_query(table_name, columns)
print(create_query)


"""Explanation:

build_create_query Function:

Constructs a CREATE TABLE SQL query for the given table name and columns.
Iterates through columns and appends each column definition to the query.
Returns the final query string, ensuring it ends with a closing parenthesis.
Example Usage:

Demonstrates how to use the build_create_query function to generate a CREATE TABLE query."""