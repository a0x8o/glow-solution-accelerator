# Databricks notebook source
import glow
spark = glow.register(spark)

from delta import DeltaTable

import pyspark.sql.functions as fx
from pyspark.sql.types import *

import random
import string
import pandas as pd
import numpy as np
import os 
import json

from pathlib import Path
import itertools
from collections import Counter

import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/pipeline_runs_info_hail_glow.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/pipeline_runs_info_hail_glow.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog.pipeline_runs_info_hail_glow LOCATION 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/pipeline_runs_info_hail_glow.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_optimized.pipeline_runs_info_hail_glow DEEP CLONE gwas_catalog.pipeline_runs_info_hail_glow

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_deep_optimized.pipeline_runs_info_hail_glow DEEP CLONE gwas_catalog.pipeline_runs_info_hail_glow

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL  gwas_catalog_deep_optimized.pipeline_runs_info_hail_glow 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog_deep_optimized.pipeline_runs_info_hail_glow 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog_deep_optimized.pipeline_runs_info_hail_glow WHERE n_variants = 500000

# COMMAND ----------

awsVMs = [
  {"node_type_id": "c5d.2xlarge", "cores_per_node": 8}, 
  {"node_type_id": "m5d.2xlarge", "cores_per_node": 8}, 
  {"node_type_id": "i3.xlarge", "cores_per_node": 4}, 
  {"node_type_id": "i3en.2xlarge", "cores_per_node": 8}, 
  {"node_type_id": "i3.8xlarge", "cores_per_node": 32}, 
  {"node_type_id": "i3.16xlarge", "cores_per_node": 64}, 
  {"node_type_id": "i4i.xlarge", "cores_per_node": 4}, 
  {"node_type_id": "i4i.2xlarge", "cores_per_node": 8},
  {"node_type_id": "i4i.2xlarge", "cores_per_node": 16}, 
  {"node_type_id": "i4i.8xlarge", "cores_per_node": 32}, 
  {"node_type_id": "i4i.16xlarge", "cores_per_node": 64}, 
  {"node_type_id": "i4i.32xlarge", "cores_per_node": 128}, 
  {"node_type_id": "r5d.2xlarge", "cores_per_node": 8},
  {"node_type_id": "r5d.4xlarge", "cores_per_node": 16}
]

cores_df = spark.createDataFrame(awsVMs)
display(cores_df)

# COMMAND ----------

run_info_df = spark.read.format("delta").load('dbfs:/user/hive/warehouse/gwas_catalog_deep_optimized.db/pipeline_runs_info_hail_glow') \
                                        .withColumnRenamed("worker_type", "n_workers") \
                                        .sort(fx.col("n_variants").desc(), fx.col("method"))
display(run_info_df)

# COMMAND ----------

run_info_core_hours_df = run_info_df.join(cores_df, "node_type_id", "inner"). \
                                     withColumn("core_hours", fx.round(((fx.col("runtime") / 3600) * fx.col("cores_per_node") * fx.col("n_workers")), 2)). \
                                     withColumn("DBUs", fx.col("core_hours") / 4)
display(run_info_core_hours_df)

# COMMAND ----------

run_info_core_hours_df.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog.gwas_core_hours_glow")

# COMMAND ----------

run_info_core_hours_df.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog_optimized.gwas_core_hours_glow")

# COMMAND ----------

run_info_core_hours_df.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog_deep_optimized.gwas_core_hours_glow")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog_deep_optimized.gwas_core_hours_glow WHERE n_variants = 500000

# COMMAND ----------

cols = ['n_samples', 'n_variants', 'n_covariates', 'n_phenotypes', 'method', 'test', 'library', 'spark_version']

# COMMAND ----------

run_info_core_hours_agg_glow_df = run_info_core_hours_df.where(fx.col("datetime") >= '2022-01-20'). \
                                                         groupBy(cols).agg(fx.min(fx.col("core_hours")).alias("core_hours"),
                                                                  fx.min(fx.col("DBUs")).alias("DBUs")
                                                                 )

# COMMAND ----------

display(run_info_core_hours_agg_glow_df)

# COMMAND ----------

run_info_core_hours_agg_glow_df.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog.gwas_core_hours_agg_glow")

# COMMAND ----------

run_info_core_hours_agg_glow_df.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog_optimized.gwas_core_hours_agg_glow")

# COMMAND ----------

run_info_core_hours_agg_glow_df.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog_deep_optimized.gwas_core_hours_agg_glow")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs)*2 AS TOTAL_DBUs, SUM(core_hours)*2 AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15*2 AS TOTAL_JOBS_DDBUs FROM gwas_catalog_deep_optimized.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------


