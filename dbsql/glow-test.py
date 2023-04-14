# Databricks notebook source
# MAGIC %sh wget https://www.cottongen.org/cottongen_downloads/Gossypium_raimondii/JGI_221_G.raimondii_Dgenome/Udall_D5ref_snp_vcf/D13.snp4.0.vcf.gz

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %fs cp file:/databricks/driver/D13.snp4.0.vcf.gz /FileStore/D13.snp4.0.vcf.gz

# COMMAND ----------

import glow
spark = glow.register(spark)
df = spark.read \
  .format("com.databricks.vcf") \
  .option("includeSampleIds", True) \
  .option("flattenInfoFields", True) \
  .load("/FileStore/D13.snp4.0.vcf.gz")

# COMMAND ----------

display(df)

# COMMAND ----------


