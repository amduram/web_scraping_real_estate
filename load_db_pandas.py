import pandas as pd
import numpy as np
import sqlite3
import mysql.connector
from sqlalchemy import create_engine
import json

TABLE_NAMES = ['living_space', 'cities', 'properties']
DB_NAME = "col_real_estate"

def impute_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Impute missing values and clean up the data in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing property data.

    Returns
    -------
    pd.DataFrame
        DataFrame with imputed values for `rooms` and `stratum`, and duplicate rows removed.
    """
    df['rooms'] = df.apply(lambda x: 1 if (not(x['rooms']>0) and x['property_type']=='Apartaestudio') else x['rooms'], axis=1)
    df['rooms'].fillna(0,inplace=True)
    df['stratum'] = df.apply(lambda x: 6 if x['stratum']>6 else x['stratum'], axis=1)
    df = df.drop_duplicates()
    
    return df


def create_tables(df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Create tables from a DataFrame with real estate data for database insertion.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing property data.

    Returns
    -------
    list of pd.DataFrame
        List of DataFrames containing tables for the database:
        main properties table, cities, and property types.
    """
    #Create cities table
    cities = pd.DataFrame({
        'city_id' : np.arange(1,len(df['city'].unique())+1).tolist(),
        'city_name' : df['city'].unique().tolist()
    })

    #Create property type table
    property_type = pd.DataFrame({
        'property_type_id' : np.arange(1,len(df['property_type'].unique())+1),
        'property_type' : df['property_type'].unique().tolist()
    })

    # Merge cities and property type into main DataFrame
    df = df.merge(cities, left_on='city',right_on='city_name', how='left').drop(columns=['city_name','city'])
    df = df.merge(property_type, on='property_type', how='left')[['id','price','area','rooms','bathrooms','garage','property_type_id','stratum','location','city_id']]

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


def load_to_db(tables_list: list[pd.DataFrame], db_connection: (sqlite3.Connection | mysql.connector.connection_cext.CMySQLConnection), table_names: list[str]) -> None:
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

        [tables_list[count].to_sql(table_names[count], db_connection, if_exists='replace', index=False) for count in range(len(tables_list))]
        db_connection.close()

    elif isinstance(db_connection, mysql.connector.connection_cext.CMySQLConnection):

        engine = create_engine(f"mysql+pymysql://{db_connection._user}:{db_connection._password}@{db_connection._host}/{DB_NAME}")
        [tables_list[count].to_sql(table_names[count], engine, if_exists="replace", index=False) for count in range(len(tables_list))]
        db_connection.close()


if __name__ == '__main__':
    # Load data from CSV
    df = pd.read_csv('/home/asus/amduram/Data_Engineer/datamentoring/03_web_scraping/COLOMBIA_REAL_STATE.csv')

    # Process and load data
    df = impute_values(df)
    tables_list = create_tables(df)
    db_connection = connect_to_db("mysql")
    load_to_db(tables_list, db_connection, TABLE_NAMES)