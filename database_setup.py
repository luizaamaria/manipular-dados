import mysql.connector
from mysql.connector import Error
import pandas as pd
from main import connection, create_db_connection, execute_query, user_password


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


create_database_query = "CREATE DATABASE school IF NOT EXISTS"

create_database(connection, create_database_query)

create_teacher_table = """
CREATE TABLE teacher (
  teacher_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  language_1 VARCHAR(3) NOT NULL,
  language_2 VARCHAR(3),
  dob DATE,
  tax_id INT UNIQUE,
  phone_no VARCHAR(20)
  );
 """

connection = create_db_connection("localhost", "root", user_password, "school")  # Connect to the Database
execute_query(connection, create_teacher_table)  # Execute our defined query
