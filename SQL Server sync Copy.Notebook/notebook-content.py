# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "06daaab7-ad22-4d32-a869-25c3c0b18e90",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "3fc15e8d-b7ec-4d57-abd0-49cbf308b40a",
# META       "known_lakehouses": [
# META         {
# META           "id": "06daaab7-ad22-4d32-a869-25c3c0b18e90"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyodbc, os
from pyspark.sql.types import StructType, StringType, StructType
from pyspark.sql.functions import col, desc, asc
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_df = spark.sql("SHOW TABLES")
table_list = [row['tableName'] for row in tables_df.collect()]

db = "TMDB"

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

server_password = client.get_secret("sql-server-password").value

print(server_password)

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=master;"
            f"UID=admin2;"
            f"PWD={server_password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE={db};"
            f"UID=admin2;"
            f"PWD={server_password};"
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


with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
        
            IF NOT EXISTS (SELECT name from sys.databases WHERE name = ?)
                BEGIN
                SELECT ? + ' doesnt exists';
                END
            ELSE
                BEGIN
                SELECT ? + ' exist';
                END
        
        """,db,db,db)

        
        while True:
            result = cursor.fetchall()

            if result:    
                print(result)
            if not cursor.nextset():
                break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def converts(datatype):
    datatype = datatype.simpleString()

    match datatype:
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

for table in table_list:

    print(table)
    df = spark.read.table(table)
    

    table_cols = [f"{field.name} {converts(field.dataType)}" for field in df.schema.fields]

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                IF NOT EXISTS (SELECT name FROM sys.tables WHERE name = ?)
                    BEGIN
                    SELECT '['+ ? + '] doesnt exist'
                    EXEC('CREATE TABLE [' + ? + '] (' + ? + ')')
                    SELECT '['+ ? + '] created' 
                    END
                ELSE
                    BEGIN
                    SELECT '[' + ? + '] exists already'
                    END

            """,table,table,table,','.join(table_cols),table,table)

            while True:
                result = cursor.fetchall()
                if result:
                    print(result[0])   
                if not cursor.nextset():
                    break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbc_url = "jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           f"databaseName={db};" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": server_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

for table in table_list:

    try:
        df = spark.read.table(table)
        if table == "Date":
            df = df.withColumnsRenamed({"Week of year": "week_of_year","Day Name": "day_name"})

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
