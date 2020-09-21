# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import * 

spark.conf.set("fs.azure.account.auth.type.azuresynapsepiero.dfs.core.windows.net", "SharedKey")
spark.conf.set("fs.azure.account.key.azuresynapsepiero.dfs.core.windows.net","JzKIpiBzW7WQQhcx0zNTtq52DlSmniO0RNKMwG0olbMYE4t1AUsYebRNWj5ZHGYnva/jRuCGXoN10cwhwBeBuQ==")

# COMMAND ----------

yellow_path = "abfss://root@azuresynapsepiero.dfs.core.windows.net/nyctlc/yellow/*/*/"

# COMMAND ----------

df_yellow = spark.read.parquet(yellow_path)


# COMMAND ----------

df_yellow.show(1)