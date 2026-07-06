# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "baaadc06-d5f8-4c13-b91b-43a72c5489d4",
# META       "default_lakehouse_name": "covid19_project_lakehouse",
# META       "default_lakehouse_workspace_id": "d33f9cfc-31fb-41ef-acaf-343d089f1372",
# META       "known_lakehouses": [
# META         {
# META           "id": "baaadc06-d5f8-4c13-b91b-43a72c5489d4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

tables = ['epidemiology','hospitalizations','vaccinations','index','demographics']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in tables:
    print(f'Processing: {t}')

    df = spark.read.option('header','true')\
                   .csv(f'Files/raw_files/{t}.csv')

    row_count = df.count()
    print(f'Rows Read: {row_count}')

    df.write.format('delta').mode('append').saveAsTable(f'raw.{t}')

    print(f'Written to table: raw.{t}')
    print('----------------------')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in tables:
    count = spark.sql(f"SELECT count(*) as cnt from raw.{t}").collect()[0]["cnt"]
    print(f"raw.{t}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
