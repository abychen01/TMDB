# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f908ebc-d61f-4264-97d2-eaa9e2d1b9c3",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "b8e7a887-498e-4e85-af11-885c32a43aa5",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f908ebc-d61f-4264-97d2-eaa9e2d1b9c3"
# META         },
# META         {
# META           "id": "d54a4800-b077-4df7-a53b-4a79430916a4"
# META         },
# META         {
# META           "id": "ad0ef244-eccd-4390-9f4a-899dd2b819f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyodbc
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_df = spark.sql("SHOW TABLES")
tables_list = [row["tableName"] for row in tables_df.collect()]
print(tables_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.option("headers",True).csv("Files/creds")
password = df2.collect()[1]['_c0']

db = "TMDB"

jdbc_url = f"jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           f"databaseName={db};" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=master;"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE={db};"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



def converts(spark_type):

    spark_type_name = spark_type.simpleString()

    match spark_type_name:
        case "int":
            return "INT"
        case "string":
            return "NVARCHAR(255)"  # Using NVARCHAR as requested
        case "timestamp":
            return "DATETIME"
        case "double":
            return "FLOAT"
        case "boolean":
            return "BIT"
        case "decimal":
            return "DECIMAL(18,2)"
        case _:
            return "NVARCHAR(255)"  # Default for unsupported types


with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            if not exists(select name from sys.databases where name = 'TMDB')
            begin
            create database TMDB
            SELECT 'Database created.' 
            end
            else
            begin
            SELECT 'Database TMDB already exists.'
            end
        """)
        result = cursor.fetchone()
        print(result[0])
        

for table in tables_list:

    df = df.df = spark.read.table(table)

    if table == "Date":
        df = df.withColumnsRenamed({"Week of year": "week_of_year", "Day name": "Day_name"})
    
    cols = [f"{field.name} {converts(field.dataType)}" for field in df.schema.fields]
    
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(""" 
                        IF NOT EXISTS (SELECT name FROM sys.tables WHERE name = ?)
                        BEGIN
                            SELECT ? + ' does not exist.';
                            EXEC('CREATE TABLE [' + ? + '] (' + ? + ')');
                            SELECT ? + ' created.';
                        END
                        ELSE
                        BEGIN
                            SELECT ? + ' already exists' 
                        END
                    """, (table, f'[{table}]',table,','.join(cols), f'[{table}]', f'[{table}]'))
                
                
                
                while True:
                    result = cursor.fetchall()
                    if result:
                        print(result[0])
                    if not cursor.nextset():
                        break

    except Exception as e:
        print(e)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''

jdbc_url = "jdbc:sqlserver://fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com:1433;databaseName=TMDB;encrypt=true;trustServerCertificate=true"
jdbc_properties = {
    "user": "admin",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

'''

for table in tables_list:

    df = spark.read.table(table)
    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote data to RDS table '{table}'.")


    except Exception as e:
        print(f"Failed to write to RDS: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
for table in tables_list:

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            print()
            print(table)
            print()
            cursor.execute("EXEC('SELECT TOP 5 * FROM [' + ? + ']')",table)
            
            cursor.execute(""" 
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION;      
            """,table)



            while True:
                    result = cursor.fetchall()
                    if result:
                        print(result)
                    if not cursor.nextset():
                        break

'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
for table in tables_list:

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            print()
            print(table)
            print()
            cursor.execute("EXEC('DROP TABLE [' + ? + ']')",table)
            
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
