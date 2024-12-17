# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename"),StringType(),True),
                                 StructField("surname"),StringType(),True)
                                 ])
