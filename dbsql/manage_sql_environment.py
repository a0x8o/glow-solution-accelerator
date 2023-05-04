# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE alex_barreto_photon_variant_db_optimized

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS alex_barreto_photon_variant_db_optimized.annotations DEEP CLONE alex_barreto_photon_variant_db.annotations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS alex_barreto_photon_variant_db_optimized.exploded DEEP CLONE alex_barreto_photon_variant_db.exploded

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS alex_barreto_photon_variant_db_optimized.pvcf DEEP CLONE alex_barreto_photon_variant_db.pvcf

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_photon_variant_db_optimized.annotations ZORDER BY contigName

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_photon_variant_db_optimized.exploded ZORDER BY contigName

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_photon_variant_db_optimized.pvcf ZORDER BY contigName

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,vcf2delta
# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/home/genomics/delta/1kg-delta'

# COMMAND ----------

# DBTITLE 1,tertiary
# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/databricks-datasets/genomics/gwas/hapgen-variants.delta'

# COMMAND ----------

# DBTITLE 1,binaryglowgr
# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/tmp/binary_wgr_block_matrix.delta'

# COMMAND ----------

# DBTITLE 1,glowgr
# MAGIC %sql
# MAGIC DESCRIBE DETAIL '/dbfs/tmp/wgr_gwas_results.delta'

# COMMAND ----------

# DBTITLE 1,gwas-binary
# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/tmp/wgr_log_reg_results.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 'dbfs:/tmp/wgr_log_reg_results.delta'

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS alex_barreto_variant_db.wgr_log_reg_results LOCATION 'dbfs:/tmp/wgr_log_reg_results.delta'

# COMMAND ----------

# DBTITLE 1,gwas-quantitiative
# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/tmp/wgr_lin_reg_results.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 'dbfs:/tmp/wgr_lin_reg_results.delta'

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS alex_barreto_variant_db.wgr_lin_reg_results LOCATION 'dbfs:/tmp/wgr_lin_reg_results.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS alex_barreto_photon_variant_db_optimized.wgr_lin_reg_results DEEP CLONE alex_barreto_variant_db.wgr_lin_reg_results 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE alex_barreto_photon_variant_db_optimized.wgr_lin_reg_results

# COMMAND ----------

# MAGIC %python 
# MAGIC display(spark.sql("OPTIMIZE  alex_barreto_photon_variant_db_optimized.wgr_lin_reg_results ZORDER BY (start)"))

# COMMAND ----------

# DBTITLE 1,gff annotations
# MAGIC %sql
# MAGIC DESCRIBE DETAIL 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/gff_annotations.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/gff_annotations.delta'

# COMMAND ----------

# MAGIC %python
# MAGIC display(spark.sql("OPTIMIZE 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/gff_annotations.delta' ZORDER BY (contigName, start)"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE alex_barreto_photon_variant_db.gff_annotations LOCATION 'dbfs:/home/alex.barreto@databricks.com/genomics/photon/data/delta/gff_annotations.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE alex_barreto_photon_variant_db_optimized.gff_annotations DEEP CLONE alex_barreto_photon_variant_db.gff_annotations

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_photon_variant_db_optimized.gff_annotations ZORDER BY (contigName, start)
