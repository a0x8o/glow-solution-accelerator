# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE gwas_catalog_optimized

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_optimized.alternative DEEP CLONE gwas_catalog.alternative

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_optimized.ancestry DEEP CLONE gwas_catalog.ancestry

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_optimized.full DEEP CLONE gwas_catalog.full

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE gwas_catalog_optimized.alternative

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE gwas_catalog_optimized.ancestry

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE gwas_catalog_optimized.full

# COMMAND ----------

spark.conf.set('spark.databricks.delta.optimize.maxFileSize',1024*1024*8)
spark.conf.get('spark.databricks.delta.optimize.maxFileSize')

# COMMAND ----------

spark.conf.set("parquet.block.size", 1024*1024*1024)
spark.conf.get("parquet.block.size")

# COMMAND ----------

spark.conf.set("spark.databricks.parquet.prefetchingReader.smallFileLimit", 1024*1024*32)
spark.conf.get("spark.databricks.parquet.prefetchingReader.smallFileLimit")

# COMMAND ----------

spark.conf.set("spark.databricks.photon.scan.enabled", "true")
spark.conf.set("spark.databricks.photon.sort.enabled", "true")
spark.conf.set("spark.databricks.photon.window.enabled", "true")
spark.conf.set("spark.databricks.streaming.forEachBatch.optimized.enabled", "true")
spark.conf.set("spark.databricks.streaming.forEachBatch.optimized.fastPath.enabled", "true")
spark.conf.set("spark.databricks.photon.allDataSources.enabled", "true")
spark.conf.set("spark.databricks.photon.photonRowToColumnar.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gwas_catalog_deep_optimized

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_deep_optimized.alternative DEEP CLONE gwas_catalog.alternative

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_deep_optimized.ancestry DEEP CLONE gwas_catalog.ancestry

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gwas_catalog_deep_optimized.full DEEP CLONE gwas_catalog.full

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE gwas_catalog_deep_optimized.alternative

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE gwas_catalog_deep_optimized.ancestry

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE gwas_catalog_deep_optimized.full
