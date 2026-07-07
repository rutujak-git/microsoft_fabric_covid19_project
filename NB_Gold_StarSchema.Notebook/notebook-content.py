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

# MARKDOWN ********************

# ## **<mark>Gold Layer — Star Schema</mark>**
# Builds FactCovidDaily, DimLocation, and DimDate from silver tables.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **DimDate**
# Standard calendar dimension, generated (not sourced from raw data).
# Covers the full date range present in FactCovidDaily.

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate a continuous date range covering the dataset's span
date_range_df = spark.sql("""
                            SELECT explode(sequence(to_date('2019-01-01'),to_date(current_date()),interval 1 day)) as Date
""")

display(date_range_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_date = date_range_df.select(
    date_format(col("Date"), "yyyyMMdd").cast("int").alias("DateKey"),
    col("Date"),
    year(col("Date")).alias("Year"),
    quarter(col("Date")).alias("Quarter"),
    month(col("Date")).alias("Month"),
    date_format(col("Date"), "MMMM").alias("MonthName"),
    dayofmonth(col("Date")).alias("Day"),
    dayofweek(col("Date")).alias("DayOfWeek"),
    date_format(col("Date"), "EEEE").alias("DayName"),
    weekofyear(col("Date")).alias("WeekOfYear"),
    when(dayofweek(col("Date")).isin(1, 7), True).otherwise(False).alias("IsWeekend"),
)

display(df_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.DimDate")

print(f"gold.DimDate: {df_date.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **DimLocation**
# Joins silver.index and silver.demographics on location_key.
# One row per country, with identifying info and population attributes.

# CELL ********************

df_index = spark.table("silver.index")
df_demo = spark.table("silver.demographics")

df_location = df_location = df_index.join(df_demo, on="location_key", how="left")

df_location = df_location.select(
    col("location_key").alias("LocationKey"),
    col("country_name").alias("CountryName"),
    col("country_code").alias("CountryCode"),
    col("country_code_iso3").alias("CountryCodeISO3"),
    col("population").alias("Population"),
    col("population_density").alias("PopulationDensity"),
    col("human_development_index").alias("HDI"),
    col("population_urban").alias("PopulationUrban"),
    col("population_rural").alias("PopulationRural"),
)

display(df_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_location.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.DimLocation")

print(f"gold.DimLocation: {df_location.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **FactCovidDaily**
# Joins silver.epidemiology (base) + silver.hospitalizations + silver.vaccinations
# on (location_key, date). Left joins — every epidemiology row is kept even if
# hospitalization/vaccination data is missing for that location/date.
# Grain: one row per location per day.

# CELL ********************

df_epi = spark.table("silver.epidemiology")
df_hosp = spark.table("silver.hospitalizations")
df_vax = spark.table("silver.vaccinations")

df_fact = df_epi.alias('e')\
                .join(df_hosp.alias('h'),(col('e.location_key') == col("h.location_key")) & (col("e.date") == col("h.date")), how="left") \
                .join(df_vax.alias('v'),(col('e.location_key') == col('v.location_key')) & (col("e.date") == col("v.date")), how="left")

df_fact = df_fact.select(
    date_format(col("e.date"), "yyyyMMdd").cast("int").alias("DateKey"),
    col("e.location_key").alias("LocationKey"),
    col("e.new_confirmed").alias("NewConfirmed"),
    col("e.new_deceased").alias("NewDeceased"),
    col("e.new_tested").alias("NewTested"),
    col("e.cumulative_confirmed").alias("CumulativeConfirmed"),
    col("e.cumulative_deceased").alias("CumulativeDeceased"),
    col("h.new_hospitalized_patients").alias("NewHospitalized"),
    col("h.current_hospitalized_patients").alias("CurrentHospitalized"),
    col("h.new_intensive_care_patients").alias("NewICU"),
    col("h.new_ventilator_patients").alias("NewVentilator"),
    col("v.new_persons_vaccinated").alias("NewVaccinated"),
    col("v.cumulative_persons_vaccinated").alias("CumulativeVaccinated"),
    col("v.new_persons_fully_vaccinated").alias("NewFullyVaccinated"),
    col("v.cumulative_persons_fully_vaccinated").alias("CumulativeFullyVaccinated"),
)

df_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.FactCovidDaily")

print(f"gold.FactCovidDaily: {df_fact.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Row counts across all three gold tables
print(f"gold.FactCovidDaily: {spark.table('gold.FactCovidDaily').count()} rows")
print(f"gold.DimLocation: {spark.table('gold.DimLocation').count()} rows")
print(f"gold.DimDate: {spark.table('gold.DimDate').count()} rows")

# Check for orphaned fact rows - LocationKeys in fact that don't exist in DimLocation
orphans = spark.sql("""
    SELECT DISTINCT f.LocationKey
    FROM gold.FactCovidDaily f
    LEFT JOIN gold.DimLocation d ON f.LocationKey = d.LocationKey
    WHERE d.LocationKey IS NULL
""")
print(f"Orphaned LocationKeys in fact table: {orphans.count()}")
orphans.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# How many distinct countries actually appear in the fact table?
spark.sql("SELECT COUNT(DISTINCT LocationKey) as country_count FROM gold.FactCovidDaily").show()

# Average days of data per country
spark.sql("""
    SELECT LocationKey, COUNT(*) as day_count 
    FROM gold.FactCovidDaily 
    GROUP BY LocationKey 
    ORDER BY day_count DESC 
    LIMIT 10
""").show()

spark.sql("""
    SELECT LocationKey, COUNT(*) as day_count 
    FROM gold.FactCovidDaily 
    GROUP BY LocationKey 
    ORDER BY day_count ASC 
    LIMIT 10
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"silver.epidemiology (clean): {spark.table('silver.epidemiology').count()} rows")
print(f"silver.epidemiology_quarantine: {spark.table('silver.epidemiology_quarantine').count()} rows")

print(f"Distinct countries in silver.epidemiology: {spark.sql('SELECT COUNT(DISTINCT location_key) FROM silver.epidemiology').collect()[0][0]}")
print(f"Distinct countries in silver.epidemiology_quarantine: {spark.sql('SELECT COUNT(DISTINCT location_key) FROM silver.epidemiology_quarantine').collect()[0][0]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
