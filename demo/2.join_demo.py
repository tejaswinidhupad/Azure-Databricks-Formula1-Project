# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, on="circuit_id", how="inner") \
                    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.name,races_df.round)

# COMMAND ----------

display(races_circuits_df)
