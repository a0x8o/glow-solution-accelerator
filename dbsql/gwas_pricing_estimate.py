# Databricks notebook source
# MAGIC %md 
# MAGIC # GWAS pipeline costs 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gwas_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog.pipeline_runs_info_hail_glow LOCATION '{}'.format(run_metadata_delta_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL  gwas_catalog.pipeline_runs_info_hail_glow 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.pipeline_runs_info_hail_glow 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.pipeline_runs_info_hail_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.pipeline_runs_info_hail_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.pipeline_runs_info_hail_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.pipeline_runs_info_hail_glow WHERE n_variants = 1000000

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
  {"node_type_id": "i4i.4xlarge", "cores_per_node": 16}, 
  {"node_type_id": "i4i.8xlarge", "cores_per_node": 32}, 
  {"node_type_id": "i4i.16xlarge", "cores_per_node": 64}, 
  {"node_type_id": "i4i.32xlarge", "cores_per_node": 128}, 
  {"node_type_id": "r5d.2xlarge", "cores_per_node": 8},
  {"node_type_id": "r5d.4xlarge", "cores_per_node": 16},
  {"node_type_id": "r6gd.xlarge", "cores_per_node": 4},
  {"node_type_id": "r6gd.2xlarge", "cores_per_node": 8},
  {"node_type_id": "r6gd.4xlarge", "cores_per_node": 16},
  {"node_type_id": "r6gd.8xlarge", "cores_per_node": 32},
  {"node_type_id": "r6gd.16xlarge", "cores_per_node": 64},
  {"node_type_id": "r6gd.32xlarge", "cores_per_node": 128}
]

cores_df = spark.createDataFrame(awsVMs)
display(cores_df)

# COMMAND ----------



# COMMAND ----------

run_info_df_benchmark = spark.table("gwas_catalog.pipeline_runs_info_glow_benchmark")

# COMMAND ----------

display(run_info_df_benchmark)

# COMMAND ----------

run_info_core_hours_df_benchmark = run_info_df_benchmark.join(cores_df, "node_type_id", "inner"). \
                                     withColumn("core_hours", fx.round(((fx.col("runtime") / 3600) * fx.col("cores_per_node") * fx.col("n_workers")), 2)). \
                                     withColumn("DBUs", fx.col("core_hours") / 4)
display(run_info_core_hours_df_benchmark)

# COMMAND ----------

run_info_core_hours_df_benchmark.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog.gwas_core_hours_glow_benchmark")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.gwas_core_hours_glow_benchmark

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO gwas_catalog.gwas_core_hours_glow SELECT * FROM gwas_catalog.gwas_core_hours_glow_benchmark

# COMMAND ----------

cols = ['n_samples', 'n_variants', 'n_covariates', 'n_phenotypes', 'method', 'test', 'library', 'spark_version']

# COMMAND ----------

run_info_core_hours_agg_glow_df_benchmark = run_info_core_hours_df_benchmark.where(fx.col("datetime") >= '2022-01-20'). \
                                                         groupBy(cols).agg(fx.min(fx.col("core_hours")).alias("core_hours"),
                                                                  fx.min(fx.col("DBUs")).alias("DBUs")
                                                                 )

# COMMAND ----------

display(run_info_core_hours_agg_glow_df_benchmark)

# COMMAND ----------

run_info_core_hours_agg_glow_df_benchmark.write.format("delta").mode("overwrite").saveAsTable("gwas_catalog.gwas_core_hours_agg_glow_benchmark")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow_benchmark

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO gwas_catalog.gwas_core_hours_agg_glow SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow_benchmark

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow

# COMMAND ----------



# COMMAND ----------

run_info_df = spark.read.format("delta").load(run_metadata_delta_path) \
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

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalo.gwas_core_hours_glow WHERE n_variants = 500000

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

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants BETWEEN 100000 AND 249999

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants BETWEEN 250000 AND 499999

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants BETWEEN 500000 AND 999999

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 500K samples, 100K variants

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.20 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 100000

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 500K samples, 250K variants

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.20 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 250000

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 500K samples, 500K variants

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.20 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs)*2 AS TOTAL_DBUs, SUM(core_hours)*2 AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15*2 AS TOTAL_JOBS_DDBUs FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_variants = 500000

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 35K samples, 500K variants

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT SUM(DBUs) AS TOTAL_DBUs, SUM(core_hours) AS TOTAL_CORE_HOURS, SUM(DBUs)*0.15 AS TOTAL_JOBS_DDBUs FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_samples = 35000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.15 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_samples = 35000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(DBUs)*0.20 FROM gwas_catalog.gwas_core_hours_agg_glow WHERE n_samples = 35000
