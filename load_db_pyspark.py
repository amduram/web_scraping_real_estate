from pyspark.sql import SparkSession
from pyspark.sql.functions import when, asc
import sqlite3
import mysql.connector
from sqlalchemy import create_engine
import json

TABLE_NAMES = ['living_space', 'cities', 'properties']
DB_NAME = "col_real_estate"

def impute_values(df: DataFrame) -> DataFrame:

    # Drop duplicate rows
    df = df.dropDuplicates()

    # Filter rows where area is not null
    df = df.filter(df.area.isNotNull())

    # Impute missing 'rooms' for 'Apartaestudio'
    df = df.withColumn(
        "rooms",
        when(
            (df.rooms.isNull()) & (df.property_type == "Apartaestudio"), 1)
        .otherwise(df.rooms)
    )
    
    # Replace remaining null values in 'rooms' with 0
    df = df.withColumn(
        "rooms",
        when(
            df.rooms.isNull(), 0)
        .otherwise(df.rooms)
    )
    
    # Impute 'stratum' values above 6 to 6
    df = df.withColumn(
        "stratum",
        when(
            df.stratum > 6, 6)
        .otherwise(df.stratum)
    )
    
    # Impute 'stratum' values below 1 to 1
    df = df.withColumn(
        "stratum",
        when(
            df.stratum < 1, 1)
        .otherwise(df.stratum)
    )
    
    return df


def create_tables(df: DataFrame) -> list[DataFrame]:
  
    # Create cities table
    cities = pl.DataFrame({
        "city_name": (
            df.get_column("city"))
            .unique()
            .sort()
            }).with_row_index("city_id", 1)

    # Create property type table
    property_type = pl.DataFrame({
        "property_type": (
            df.get_column("property_type"))
            .unique()
            .sort()
            }).with_row_index("property_type_id", 1)

    # Merge cities and property type into main DataFrame
    df = df.join(cities, left_on='city', right_on='city_name', how='left')
    df = df.join(property_type, on='property_type', how='left').select(['id', 'price', 'area', 'rooms', 'bathrooms', 'garage', 'property_type_id', 'stratum', 'location', 'city_id'])

    tables_list = [df, cities, property_type]
    
    return tables_list


def connect_to_db(db_type: str) -> (sqlite3.Connection | mysql.connector.connection_cext.CMySQLConnection | None):
    """
    Connect to a specified database type (SQLite or MySQL).

    Parameters
    ----------
    db_type : str
        Type of the database ("sqlite3" or "mysql").

    Returns
    -------
    sqlite3.Connection or mysql.connector.connection_cext.CMySQLConnection or None
        Database connection object or None if the type is unknown.
    """
    if db_type == "sqlite3":
        # Connect to SQLite database
        db_connection = sqlite3.connect(f"{DB_NAME}.db")

    elif db_type == "mysql":
        with open('mysql_credentials.json','r') as file:
            data = json.load(file)
        # Connect to mysql database
        db_connection = mysql.connector.connect(
            host = data["host"],
            user = data["user"],
            password = data["password"]
        )

        db_connection.cursor().execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        db_connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    else:
        print("Unknown connection")
    
    return db_connection


def load_to_db(tables_list: list[pl.DataFrame], db_connection: (sqlite3.Connection | mysql.connector.connection_cext.CMySQLConnection), table_names: list[str]) -> None:
    """
    Load DataFrames into a database.

    Parameters
    ----------
    tables_list : list of pd.DataFrame
        List of DataFrames representing the tables to load.
    db_connection : sqlite3.Connection or mysql.connector.connection_cext.CMySQLConnection
        Database connection object.
    table_names : list of str
        List of table names to be created in the database.

    Returns
    -------
    None
    """
    if isinstance(db_connection, sqlite3.Connection):

        [tables_list[count].write_database(table_names[count], f"sqlite:///./{DB_NAME}.db", if_table_exists='replace') for count in range(len(tables_list))]
        db_connection.close()

    elif isinstance(db_connection, mysql.connector.connection_cext.CMySQLConnection):

        engine = create_engine(f"mysql+pymysql://{db_connection._user}:{db_connection._password}@{db_connection._host}/{DB_NAME}")
        [tables_list[count].write_database(table_names[count], engine, if_table_exists="replace") for count in range(len(tables_list))]
        db_connection.close()


if __name__ == '__main__':
    # Load data from CSV
    spark = SparkSession.builder.appName("PySpark_tl_colombia_real_estate").getOrCreate()
    df = spark.read.csv("/home/amduram/amduram/data_mentoring/web_scraping_real_estate/COLOMBIA_REAL_STATE_2024-11-25.csv")

    # Process and load data
    df = impute_values(df)
    tables_list = create_tables(df)
    db_connection = connect_to_db("mysql")
    load_to_db(tables_list, db_connection, TABLE_NAMES)