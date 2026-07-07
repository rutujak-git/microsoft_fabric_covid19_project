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

# ## <mark>**Silver Layer — Data Cleaning & Transformation**</mark>

# CELL ********************

spark.sql('create schema if not exists silver')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

df_index_raw = spark.table("raw.index")

country_location_keys = (
    df_index_raw
    .filter(col("aggregation_level").cast("int") == 0)
    .select("location_key")
    .distinct()
)

country_keys_list = [row.location_key for row in country_location_keys.collect()]

print(f"Country-level locations found: {len(country_keys_list)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### <u>**Epidemiology**</u>
# - Cast date and numeric columns
# - Flag negative values in new_confirmed / new_deceased / new_tested (data correction artifacts) without discarding them
# - Dedup on (location_key, date)

# CELL ********************

df_epi = spark.table('raw.epidemiology')
display(df_epi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_epi = df_epi.select(
    col('date').cast('date').alias('date'),
    col('location_key'),
    col('new_confirmed').cast('int').alias('new_confirmed'),
    col('new_deceased').cast('int').alias('new_deceased'),
    col('new_recovered').cast('int').alias('new_recovered'),
    col('new_tested').cast('int').alias('new_tested'),
    col('cumulative_confirmed').cast('int').alias('cumulative_confirmed'),
    col('cumulative_deceased').cast('int').alias('cumulative_deceased'),
    col('cumulative_recovered').cast('int').alias('cumulative_recovered'),
    col('cumulative_tested').cast('int').alias('cumulative_tested')
)

df_epi = df_epi.filter(col("location_key").isin(country_keys_list))

display(df_epi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when

df_epi = df_epi.withColumn(
    "has_negative_value",
    when(
        (col("new_confirmed") < 0) | (col("new_deceased") < 0) |
        (col("new_recovered") < 0) | (col("new_tested") < 0),
        True
    ).otherwise(False)
)

df_epi = df_epi.withColumn(
    "has_missing_value",
    when(
        col("new_confirmed").isNull() | col("new_deceased").isNull() |
        col("new_recovered").isNull() | col("new_tested").isNull(),
        True
    ).otherwise(False)
)

display(df_epi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_epi_clean = df_epi.filter((col('has_negative_value')== False) & (col('has_missing_value') == False))
display(df_epi_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_epi_quarantine = df_epi.filter((col('has_missing_value') == True) | (col('has_negative_value')== True))
display(df_epi_quarantine)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write both
df_epi_clean.write.format("delta").mode("overwrite").saveAsTable("silver.epidemiology")
df_epi_quarantine.write.format("delta").mode("overwrite").saveAsTable("silver.epidemiology_quarantine")

print(f"silver.epidemiology (clean): {df_epi_clean.count()} rows")
print(f"silver.epidemiology_quarantine (flagged): {df_epi_quarantine.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Hospitalizations**
# - Cast date and numeric columns
# - Flag negative and missing values (same pattern as epidemiology)
# - Dedup on (location_key, date)
# - Split into clean vs. quarantine

# CELL ********************

spark.table("raw.hospitalizations").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hosp = spark.table('raw.hospitalizations')
display(df_hosp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hosp = df_hosp.select(
    col('date').cast('date').alias('date'),
    col('location_key'),
    col('new_hospitalized_patients').cast('int').alias('new_hospitalized_patients'),
    col('cumulative_hospitalized_patients').cast('int').alias('cumulative_hospitalized_patients'),
    col('current_hospitalized_patients').cast('int').alias('current_hospitalized_patients'),
    col('new_intensive_care_patients').cast('int').alias('new_intensive_care_patients'),
    col('cumulative_intensive_care_patients').cast('int').alias('cumulative_intensive_care_patients'),
    col('current_intensive_care_patients').cast('int').alias('current_intensive_care_patients'),
    col('new_ventilator_patients').cast('int').alias('new_ventilator_patients'),
    col('cumulative_ventilator_patients').cast('int').alias('cumulative_ventilator_patients'),
    col('current_ventilator_patients').cast('int').alias('current_ventilator_patients')
)

df_hosp = df_hosp.filter(col("location_key").isin(country_keys_list))

display(df_hosp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MIN_VALID_DATE = to_date(lit("2019-01-01"))
MAX_VALID_DATE = current_date()

df_hosp = df_hosp.withColumn(
    "has_invalid_date",
    when(
        col("date").isNull() | (col("date") < MIN_VALID_DATE) | (col("date") > MAX_VALID_DATE),
        True
    ).otherwise(False)
)

df_hosp = df_hosp.withColumn(
    "date",
    when(col("has_invalid_date") == True, None).otherwise(col("date"))
)

df_hosp = df_hosp.withColumn(
    "has_negative_value",
    when(
        (col("new_hospitalized_patients") < 0) | (col("current_hospitalized_patients") < 0) |
        (col("new_intensive_care_patients") < 0) | (col("current_intensive_care_patients") < 0) |
        (col("new_ventilator_patients") < 0) | (col("current_ventilator_patients") < 0),
        True
    ).otherwise(False)
)

df_hosp = df_hosp.withColumn(
    "has_missing_value",
    when(
        col("new_hospitalized_patients").isNull() | col("current_hospitalized_patients").isNull() |
        col("new_intensive_care_patients").isNull() | col("current_intensive_care_patients").isNull() |
        col("new_ventilator_patients").isNull() | col("current_ventilator_patients").isNull(),
        True
    ).otherwise(False)
)

df_hosp = df_hosp.dropDuplicates(["location_key", "date"])

display(df_hosp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hosp_clean = df_hosp.filter(
    (col("has_negative_value") == False) & (col("has_missing_value") == False) & (col("has_invalid_date") == False)
)

df_hosp_quarantine = df_hosp.filter(
    (col("has_negative_value") == True) | (col("has_missing_value") == True) | (col("has_invalid_date") == True)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hosp_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.hospitalizations")
df_hosp_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.hospitalizations_quarantine")

print(f"silver.hospitalizations (clean): {df_hosp_clean.count()} rows")
print(f"silver.hospitalizations_quarantine (flagged): {df_hosp_quarantine.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Vaccinations**
# - Two tables built from raw.vaccinations:
#   1. **silver.vaccinations** — aggregate metrics (new_* and cumulative_*), cleaned and flagged
#   2. **silver.vaccinations_by_manufacturer** — unpivoted brand-level data (Pfizer, Moderna, Janssen, Sinovac), one row per date/location/manufacturer
# - Cast date and numeric columns
# - Flag negative, missing, and invalid-date values (aggregate table only)
# - Dedup on (location_key, date)
# - Split aggregate table into clean vs. quarantine

# CELL ********************

spark.table("raw.vaccinations").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vax_raw = spark.table("raw.vaccinations")

# ============================================================
# 1. Aggregate vaccinations table
# ============================================================

df_vax = df_vax_raw.select(
    col("date").cast("date").alias("date"),
    col("location_key"),
    col("new_persons_vaccinated").cast("int").alias("new_persons_vaccinated"),
    col("cumulative_persons_vaccinated").cast("long").alias("cumulative_persons_vaccinated"),
    col("new_persons_fully_vaccinated").cast("int").alias("new_persons_fully_vaccinated"),
    col("cumulative_persons_fully_vaccinated").cast("long").alias("cumulative_persons_fully_vaccinated"),
    col("new_vaccine_doses_administered").cast("int").alias("new_vaccine_doses_administered"),
    col("cumulative_vaccine_doses_administered").cast("long").alias("cumulative_vaccine_doses_administered"),
)

df_vax = df_vax.filter(col("location_key").isin(country_keys_list))

display(df_vax)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MIN_VALID_DATE = to_date(lit('2019-01-01'))
MAX_VALID_DATE = current_timestamp()

df_vax = df_vax.withColumn('has_invalid_date',
                            when(
                                col('date').isNull() | (col('date') < MIN_VALID_DATE) | (col('date') > MAX_VALID_DATE),
                                True
                            ).otherwise(False)
                            )

df_vax = df_vax.withColumn(
    "date",
    when(col("has_invalid_date") == True, None).otherwise(col("date"))
)

df_vax = df_vax.withColumn(
    "has_negative_value",
    when(
        (col("new_persons_vaccinated") < 0) | (col("new_persons_fully_vaccinated") < 0) |
        (col("new_vaccine_doses_administered") < 0),
        True
    ).otherwise(False)
)

df_vax = df_vax.withColumn(
    "has_missing_value",
    when(
        col("new_persons_vaccinated").isNull() | col("new_persons_fully_vaccinated").isNull() |
        col("new_vaccine_doses_administered").isNull(),
        True
    ).otherwise(False)
)

df_vax = df_vax.dropDuplicates(["location_key", "date"])

display(df_vax)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vax_clean = df_vax.filter(
    (col("has_negative_value") == False) & (col("has_missing_value") == False) & (col("has_invalid_date") == False)
)

df_vax_quarantine = df_vax.filter(
    (col("has_negative_value") == True) | (col("has_missing_value") == True) | (col("has_invalid_date") == True)
)

display(df_vax_quarantine)
display(df_vax_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vax_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.vaccinations")
df_vax_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.vaccinations_quarantine")

print(f"silver.vaccinations (clean): {df_vax_clean.count()} rows")
print(f"silver.vaccinations_quarantine (flagged): {df_vax_quarantine.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 2. Manufacturer-level vaccinations table 
# ============================================================

df_vax_mfr = df_vax_raw.select(
    col("date").cast("date").alias("date"),
    col("location_key"),
    col("new_persons_vaccinated_pfizer").cast("int").alias("pfizer"),
    col("new_persons_vaccinated_moderna").cast("int").alias("moderna"),
    col("new_persons_vaccinated_janssen").cast("int").alias("janssen"),
    col("new_persons_vaccinated_sinovac").cast("int").alias("sinovac"),
)

# Keep country-level locations only (based on index.aggregation_level = 0)
df_vax_mfr = df_vax_mfr.filter(col("location_key").isin(country_keys_list))

display(df_vax_mfr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vax_mfr.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.vaccinations_by_manufacturer")

print(f"silver.vaccinations_by_manufacturer: {df_vax_mfr.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_check = spark.table("silver.vaccinations_by_manufacturer")

total_rows = df_check.count()
non_null_any = df_check.filter(
    col("pfizer").isNotNull() | col("moderna").isNotNull() |
    col("janssen").isNotNull() | col("sinovac").isNotNull()
).count()

print(f"Total rows: {total_rows}")
print(f"Rows with at least one non-null manufacturer value: {non_null_any}")
print(f"Rows fully null across all 4: {total_rows - non_null_any}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check.filter(
    col("pfizer").isNotNull() | col("moderna").isNotNull() |
    col("janssen").isNotNull() | col("sinovac").isNotNull()
).select("location_key").distinct().show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Vaccinations by Manufacturer — Investigated and Dropped
# 
# Checked manufacturer-level vaccination data (Pfizer, Moderna, Janssen, Sinovac) across the full table (56,031 rows):
# - Only 569 rows (~1%) had any manufacturer data at all
# - All 569 rows belonged to a single country (MY - Malaysia)
# - No other country in the dataset reports brand-level vaccine breakdown
# 
# Decision: dropped this table. A global manufacturer-comparison report isn't feasible when 229/230 countries have zero coverage. The raw source (`raw.vaccinations`) still has these columns preserved if this ever needs revisiting.

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver.vaccinations_by_manufacturer")
print("silver.vaccinations_by_manufacturer dropped — see markdown above for reasoning")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Index (Location Dimension Seed)**
# - Filter to country-level only (aggregation_level = 0)
# - Keep: location_key, country_code, country_name, iso_3166_1_alpha_3
# - Drop: place_id, wikidata_id, datacommons_id, subregion*/locality* (all null at country level), iso_3166_1_alpha_2 (redundant with country_code)
# - This becomes the basis for DimLocation in the gold layer

# CELL ********************

spark.table("raw.index").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_index = spark.table("raw.index")

df_index = df_index.select(
    col("location_key"),
    col("country_code"),
    col("country_name"),
    col("iso_3166_1_alpha_3").alias("country_code_iso3"),
    col("aggregation_level").cast("int").alias("aggregation_level"),
)

df_index = df_index.filter(col("aggregation_level") == 0)

df_index = df_index.dropDuplicates(["location_key"])

df_index.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.index")

print(f"silver.index: {df_index.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Demographics (Population Dimension Data)**
# - Filter to country-level only (aggregation_level from index — join required since demographics itself has no aggregation_level column)
# - Keep: population, population_density, human_development_index, population_urban, population_rural
# - Drop: gender breakdown, age brackets, population_largest_city, population_clustered (out of scope for v1)
# - Cast numeric columns

# CELL ********************

spark.table("raw.demographics").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_demo = spark.table("raw.demographics")
df_index_silver = spark.table("silver.index")

df_demo = df_demo.select(
    col("location_key"),
    col("population").cast("long").alias("population"),
    col("population_density").cast("double").alias("population_density"),
    col("human_development_index").cast("double").alias("human_development_index"),
    col("population_urban").cast("long").alias("population_urban"),
    col("population_rural").cast("long").alias("population_rural"),
)

# Keep only country-level locations (join against silver.index, which is already filtered)
df_demo = df_demo.join(
    df_index_silver.select("location_key"),
    on="location_key",
    how="inner"
)

df_demo = df_demo.dropDuplicates(["location_key"])

df_demo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.demographics")

print(f"silver.demographics: {df_demo.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
