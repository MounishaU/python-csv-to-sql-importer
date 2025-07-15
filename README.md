# Python CSV to SQL Importer

A powerful and intelligent Python tool designed to simplify the process of importing CSV data into SQL databases. This project aims to automate data type inference, handle basic data cleaning, and generate/execute SQL 'CREATE TABLE' and 'INSERT' statements, making data ingestion more efficient.

## Features (Planned)

* Automatic SQL data type inference from CSV columns.
* Generation of 'CREATE TABLE' statements.
* Generation and execution of 'INSERT' statements for data loading.
* Basic handling of missing values.
* Support for various CSV delimiters.
* Command line interface for easy usage.

## Technologies Used

* Python
* Pandas (for data manipulation)
* mysql-connector-python (for MySQL database interaction)
* SQL

## Getting Started (Work in Progress)

### Prerequisites

* Python 3.x
* A MySQL database instance (local or remote)

### Installation

1.  Clone the repository:
    ```bash
    git clone [https://github.com/MounishaU/python-csv-to-sql-importer.git](https://github.com/MounishaU/python-csv-to-sql-importer.git)
    cd python-csv-to-sql-importer
    ```
2.  Create and activate a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Usage

This tool allows you to import data from a CSV file into MySQL database table. Currently, the CSV file path and target table name are hardcoded within the 'csv_importer.py' script.

**1. Prepare Your CSV File:**
   - Ensure your CSV file is named 'sample_data.csv' and is located in the same directory as 'csv_importer.py'.
   - The script will infer SQL data types based on the data in this CSV.

**2. Configure Your MySQL Database:**
   - Open 'csv_importer.py'.
   - Locate the 'DB_CONFIG' dictionary at the top of the file.
   - Update 'host', 'database', 'user', and 'password' to match your MySQL server's credentials. For example:
     ```python
     DB_CONFIG = {
         'host': 'localhost',
         'database': 'ONLINE_FOOD_DEL', # Your database name
         'user': 'root',               # Your MySQL username
         'password': '12345678'        # Your MySQL password
     }
     ```

**3. Configure Target Table (Optional, if different from default):**
   - In 'csv_importer.py', you can also adjust 'TARGET_TABLE_NAME' if you want a different table name than 'employees_data':
     ```python
     TARGET_TABLE_NAME = 'employees_data'
     ```

**4. Run the Importer:**
   - Open your terminal.
   - Navigate to the project directory:
     ```bash
     cd /path/to/your/python-csv-to-sql-importer
     ```
   - Ensure your virtual environment is activated:
     ```bash
     source venv/bin/activate
     ```
   - Run the script:
     ```bash
     python csv_importer.py
     ```

**5. Verify Data in MySQL:**
   - After successful execution, connect to your MySQL database using a client (e.g., MySQL command line or Workbench).
   - Use your database: 'USE ONLINE_FOOD_DEL;'
   - Check the table content: 'SELECT * FROM employees_data;' (replace `employees_data` with your 'TARGET_TABLE_NAME' if changed).

## Contributing

Contributions are welcome! (For now, it is just me, but this is good practice.)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
*Built with ❤️ by Mounisha Udatha*
