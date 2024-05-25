"""Usage: Python script to build SQL queries for altering tables."""

def build_alter_query(table_name, column_changes):
    query = f"ALTER TABLE {table_name}"
    for change in column_changes:
        query += f" {change},"
    return query.rstrip(',')

# Example usage
table_name = "example_table"
column_changes = [
    "ADD COLUMN new_column STRING",
    "MODIFY COLUMN existing_column INT"
]
alter_query = build_alter_query(table_name, column_changes)
print(alter_query)


"""Explanation:

build_alter_query Function:

Constructs an ALTER TABLE SQL query for the given table name and column changes.
Iterates through column_changes and appends each change to the query.
Returns the final query string, removing the trailing comma.
Example Usage:

Demonstrates how to use the build_alter_query function to generate an ALTER TABLE query."""