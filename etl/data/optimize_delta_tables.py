# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Optimize variant data

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### optimize the delta table

# COMMAND ----------

from delta.tables import *
vcf_delta_table = DeltaTable.forPath(spark, output_vcf_delta)
vcf_delta_table.optimize().executeCompaction()
