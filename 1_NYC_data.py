import requests
import json
import snowflake.connector


def upload_data_to_snowflake(account,
                             user,
                             password,
                             database,
                             schema, 
                             warehouse,
                             role):
    #connect to snowflake
    conn = snowflake.connector.connect(user=user,
                                       password=password,
                                       account=account,
                                       role=role)
    
    #operate on databae
    conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    conn.cursor().execute(f"USE DATABASE {database}")
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.cursor().execute(f'CREATE WAREHOUSE IF NOT EXISTS {warehouse} WAREHOUSE_SIZE = XLARGE AUTO_SUSPEND = 300')
    conn.cursor().execute(f"USE SCHEMA {schema}")

    #define the table schema and structure
    table_name = "inspection"
    table_column = []
    table_schema = ""

    response = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json')
    data = json.loads(response.text)
    #print(data)

    if len(data) > 0:
        for column in data[0]:
            if column == "location_1":
                for sub_column in data[0][column]:
                    table_column.append(sub_column)
                    table_schema += sub_column + "STRING,"
            else:
                table_column.append(column)
                table_schema += column + " STRING,"

    #print(table_column)
    #print(table_schema)
    table_schema = table_schema[:-1]

    #create new table
    if len(table_column) > 0:
        print(f'CREATE TABLE IF NOT EXISTS {table_name} ({table_schema}')
        conn.cursor().execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})')

        for row in data:
            values = []

            for column in table_column:
                if column in row:
                    values.append(row[column])
                else:
                    values.append(None)

            query = f"INSERT INTO {table_name} ({','.join(table_column)}) VALUES ({','.join(['%s']*len(values))})"
            conn.cursor().execute(query, values)


    conn.close()
    print("Connection closed!!!")


print("Hello world")
url = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    num_records = int(response.headers.get("X-Total-Count", 0))
    print(f"Retrieved {len(data)} out of  {num_records} records...")
    #print(data[:15])
else:
    print("ERROR: failed to retrieve data from website...")


upload_data_to_snowflake("wtynwlj-nn04581",
                         "PYTHON_USER",
                         "Python2023",
                         "Pytohn_DB",
                         "Dev_Schema",
                         "PYTHON_PROJECTS",
                         "ACCOUNTADMIN")

