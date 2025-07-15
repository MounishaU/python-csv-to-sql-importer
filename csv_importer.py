import pandas as pd
import mysql.connector
from mysql.connector import Error # for better error handling

# --- MySQL Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ONLINE_FOOD_DEL', # actual database name
    'user': 'root',               #  MySQL username
    'password': '12345678'        #  MySQL password
}
# ------------------------------------

# --- CSV File Details ---
CSV_FILE_PATH = 'sample_data.csv'
TARGET_TABLE_NAME = 'employees_data' 
# -----------------------------------------------

def infer_sql_type(pandas_dtype):
    """
    Infers a suitable SQL data type based on a Pandas Series dtype.
    This is a basic inference and can be expanded later.
    """
    if pd.api.types.is_integer_dtype(pandas_dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "DECIMAL(10, 2)" # Using DECIMAL for currency/precise numbers
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        return "DATE" # Or DATETIME if time component is expected
    else: # Default to VARCHAR for strings and objects
        # This will be overridden by the more detailed inference in main()
        return "VARCHAR(255)"

def generate_create_table_sql(df, table_name, inferred_schema):
    """
    Generates a SQL CREATE TABLE statement based on DataFrame columns and inferred types.
    Uses the provided inferred_schema for specific VARCHAR sizes.
    """
    columns_sql = []
    for column, sql_type in inferred_schema.items():
        # Using f-string here as it worked before the CLI changes
        columns_sql.append(f"    `{column}` {sql_type}")

    # Using f-string here as it worked before the CLI changes
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
    create_table_sql += ",\n".join(columns_sql)
    create_table_sql += "\n);"
    return create_table_sql

def generate_insert_statements_for_executemany(df, table_name):
    """
    Prepares data and the base INSERT statement for efficient executemany.
    It does NOT generate individual SQL strings for each row.
    """
    # Clean column names first to avoid issues with backticks later
    df.columns = df.columns.str.strip()

    # Using f-string here as it worked before the CLI changes
    columns = ", ".join([f"`{col}`" for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns)) # Use %s for MySQL placeholders
    # Using f-string here as it worked before the CLI changes
    base_insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders});"

    data_to_insert = []
    for index, row in df.iterrows():
        processed_row = []
        for item in row:
            if pd.isna(item): # Handle NaN/NaT values by converting to Python None
                processed_row.append(None)
            elif isinstance(item, pd.Timestamp): # Convert pandas datetime to string for MySQL DATE/DATETIME
                processed_row.append(item.strftime('%Y-%m-%d')) # Or '%Y-%m-%d %H:%M:%S' for DATETIME
            elif isinstance(item, bool): # Convert Python bool to MySQL's 0/1 representation
                processed_row.append(1 if item else 0)
            else:
                processed_row.append(item)
        data_to_insert.append(tuple(processed_row)) # Append as a tuple

    return base_insert_sql, data_to_insert


def execute_sql_statements(sql_statements_or_base_sql, data_to_insert=None, db_config=DB_CONFIG):
    """
    Connects to the MySQL database and executes SQL statements.
    Can execute a single CREATE TABLE statement or use executemany for INSERTs.
    """
    connection = None
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            print("\nSuccessfully connected to MySQL database.")

            if data_to_insert is None: # For single statements like CREATE TABLE
                for statement in sql_statements_or_base_sql: # Expects a list for safety, but often just one
                    try:
                        cursor.execute(statement)
                        connection.commit()
                        # Using f-string here as it worked before the CLI changes
                        print(f"Executed successfully: {statement.splitlines()[0]}...")
                    except Error as e:
                        # Using f-string here as it worked before the CLI changes
                        print(f"Error executing statement: {statement.splitlines()[0].strip()}...")
                        print(f"Error details: {e}")
                        # Temporarily simplified check for table exists
                        if "Table" in str(e) and "already exists" in str(e):
                            print("Table already exists, skipping CREATE TABLE but proceeding.")
                        else:
                            raise # Re-raise other critical errors
            else: # For executemany with INSERTs
                base_insert_sql = sql_statements_or_base_sql
                try:
                    cursor.executemany(base_insert_sql, data_to_insert)
                    connection.commit()
                    # Using f-string here as it worked before the CLI changes
                    print(f"Successfully inserted {len(data_to_insert)} rows into the table.")
                except Error as e:
                    # Using f-string here as it worked before the CLI changes
                    print(f"Error executing INSERT statements: {e}")
                    connection.rollback() # Rollback on error for batch inserts
                    raise # Re-raise the error

    except Error as e:
        # Using f-string here as it worked before the CLI changes
        print(f"Error connecting to MySQL database: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

def main():
    try:
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(CSV_FILE_PATH)
        # Clean column names immediately after reading
        df.columns = df.columns.str.strip()

        # Attempt to convert 'registration_date' to datetime objects
        if 'registration_date' in df.columns:
            df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')

        # Using f-string here as it worked before the CLI changes
        print(f"Successfully loaded '{CSV_FILE_PATH}'.")
        print("\nDataFrame Info (after cleaning and date parsing):")
        df.info() # Shows inferred Pandas dtypes

        # --- Dynamic VARCHAR length inference within main or a refined infer_sql_type ---
        inferred_schema = {}
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                inferred_schema[col] = "INT"
            elif pd.api.types.is_float_dtype(dtype):
                inferred_schema[col] = "DECIMAL(10, 2)"
            elif pd.api.types.is_bool_dtype(dtype):
                inferred_schema[col] = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                inferred_schema[col] = "DATE"
            else: # For string/object types, calculate max length
                max_len = 0
                if not df[col].isnull().all(): # Check if column is not entirely null
                    # Convert to string and find max length, add a buffer, cap at 255
                    max_len = df[col].astype(str).apply(len).max()
                # Using f-string here as it worked before the CLI changes
                inferred_schema[col] = f"VARCHAR({min(max_len + 10, 255) if max_len > 0 else 50})"


        print("\nInferred MySQL Schema:")
        for col, sql_type in inferred_schema.items():
            # Using f-string here as it worked before the CLI changes
            print(f"- {col}: {sql_type}")


        # Generate the CREATE TABLE SQL using the refined schema
        create_sql = generate_create_table_sql(df, TARGET_TABLE_NAME, inferred_schema)
        # Using f-string here as it worked before the CLI changes
        print(f"\nGenerated CREATE TABLE SQL for '{TARGET_TABLE_NAME}':")
        print(create_sql)

        # --- Execute CREATE TABLE Statement ---
        print("\n--- Executing CREATE TABLE ---")
        execute_sql_statements([create_sql], db_config=DB_CONFIG)

        # Prepare INSERT data for executemany
        base_insert_sql, data_to_insert = generate_insert_statements_for_executemany(df, TARGET_TABLE_NAME)
        # Using f-string here as it worked before the CLI changes
        print(f"\nGenerated base INSERT SQL for '{TARGET_TABLE_NAME}':")
        print(base_insert_sql)
        # Using f-string here as it worked before the CLI changes
        print(f"Prepared {len(data_to_insert)} rows for insertion.")


        # --- Execute INSERTs ---
        print("\n--- Executing INSERTs ---")
        execute_sql_statements(base_insert_sql, data_to_insert, db_config=DB_CONFIG)

        # Using f-string here as it worked before the CLI changes
        print(f"\nCSV data successfully imported into MySQL table '{TARGET_TABLE_NAME}'.")

    except FileNotFoundError:
        # Using f-string here as it worked before the CLI changes
        print(f"Error: CSV file not found at '{CSV_FILE_PATH}'")
    except Exception as e:
        # Using f-string here as it worked before the CLI changes
        print(f"An overall error occurred during import process: {e}")

if __name__ == "__main__":
    main()