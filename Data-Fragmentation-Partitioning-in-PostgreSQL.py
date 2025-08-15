# Import required libraries
# Do not install/import any additional libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math 


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "postgres"
dbname = "assignment4"
host = "127.0.0.1"


def get_open_connection():
    """
    Connect to the database and return connection object
    
    Returns:
        connection: The database connection object.
    """

    return psycopg2.connect(f"dbname='{dbname}' user='{username}' host='{host}' password='{password}'")



def load_data(table_name, csv_path, connection, header_file):
    """
    Create a table with the given name and load data from the CSV file located at the given path.

    Args:
        table_name (str): The name of the table where data is to be loaded.
        csv_path (str): The path to the CSV file containing the data to be loaded.
        connection: The database connection object.
        header_file (str): The path to where the header file is located
    """

    cursor = connection.cursor()

    # Creating the table
    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {table_rows_formatted}
            )'''

    cursor.execute(create_table_query)
    connection.commit()


    # # TODO: Implement code to insert data here
    with open(csv_path, 'r') as f:
        next(f)  # Skip header row
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)

    connection.commit()


def range_partition(data_table_name, partition_table_name, num_partitions, header_file, column_to_partition, connection):
    """
    Use this function to partition the data in the given table using a range partitioning approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        column_to_partition (str): The column based on which we are creating the partition.
        connection: The database connection object.
    """

    # TODO: Implement code to perform range_partition here
    
    cursor = connection.cursor()

    with open(header_file) as f:
        headers = json.load(f)
    schema = ', '.join(f"{col} {dtype}" for col, dtype in headers.items())

    # Get min, max values
    cursor.execute(f"SELECT MIN({column_to_partition}), MAX({column_to_partition}) FROM {data_table_name}")
    min_val, max_val = cursor.fetchone()
    interval = math.ceil((max_val - min_val + 1) / num_partitions)

    # Create parent partitioned table
    cursor.execute(f"DROP TABLE IF EXISTS {partition_table_name} CASCADE")
    cursor.execute(f"CREATE TABLE {partition_table_name} ({schema}) PARTITION BY RANGE ({column_to_partition})")

    # Create child partitions
    for i in range(num_partitions):
        start = min_val + i * interval
        end = start + interval
        cursor.execute(f"""
            CREATE TABLE {partition_table_name}{i}
            PARTITION OF {partition_table_name}
            FOR VALUES FROM ({start}) TO ({end})
        """)

    # Insert into parent table 
    cursor.execute(f"INSERT INTO {partition_table_name} SELECT * FROM {data_table_name}")
    connection.commit()
    

def round_robin_partition(data_table_name, partition_table_name, num_partitions, header_file, connection):
    """
    Use this function to partition the data in the given table using a round-robin approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        connection: The database connection object.
    """

    # TODO: Implement code to perform round_robin_partition here
    cursor = connection.cursor()
    with open(header_file) as f:
        header_dict = json.load(f)
    column_definitions = ', '.join(f"{col} {dtype}" for col, dtype in header_dict.items())
    column_names = ', '.join(header_dict.keys())

    cursor.execute(f"DROP TABLE IF EXISTS {partition_table_name} CASCADE")
    for i in range(num_partitions):
        cursor.execute(f"DROP TABLE IF EXISTS {partition_table_name}{i} CASCADE")

    # Create parent table
    cursor.execute(f"CREATE TABLE {partition_table_name} ({column_definitions})")

    # Create child tables (partitions)
    for i in range(num_partitions):
        cursor.execute(f"""
            CREATE TABLE {partition_table_name}{i} (
                LIKE {partition_table_name} INCLUDING ALL
            ) INHERITS ({partition_table_name});
        """)

    # Create round-robin sequence
    cursor.execute("DROP SEQUENCE IF EXISTS rr_count_seq")
    cursor.execute(f"CREATE SEQUENCE rr_count_seq START 0 INCREMENT 1 MINVALUE 0 MAXVALUE {num_partitions - 1} CYCLE")

    # Create trigger function
    cursor.execute(f"""
    CREATE OR REPLACE FUNCTION rr_insert_trigger()
    RETURNS TRIGGER AS $$
    DECLARE
        partition INT;
        target TEXT;
    BEGIN
        partition := nextval('rr_count_seq');
        target := TG_TABLE_NAME || partition::TEXT;
        EXECUTE format('INSERT INTO %I VALUES ($1.*)', target) USING NEW;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """)
    
    # Attach trigger to parent table
    cursor.execute(f"""
    CREATE TRIGGER rr_insert_trigger
    BEFORE INSERT ON {partition_table_name}
    FOR EACH ROW EXECUTE FUNCTION rr_insert_trigger();
    """)

    # Insert data into parent (which will be redirected via trigger)
    cursor.execute(f"INSERT INTO {partition_table_name} SELECT * FROM {data_table_name}")
    connection.commit()


def delete_partitions(table_name, num_partitions, connection):
    """
    This function in NOT graded and for your own testing convinience.
    Use this function to delete all the partitions that are created by you.

    Args:
        table_name (str): The name of the table containing the partitions to be deleted.
        num_partitions (int): The number of partitions to be deleted.
        connection: The database connection object.
    """

    # TODO: UNGRADED: Implement code to delete partitions here
    cursor = connection.cursor()
    for i in range(num_partitions):
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}{i} CASCADE")
        except:
            continue
    connection.commit()
