# Databricks notebook source
# MAGIC %md Compile Glow for Spark 3.3.1 / Run on ML 12.2, use library feature

# COMMAND ----------

# MAGIC %md Use ML 

# COMMAND ----------

# MAGIC %sh which conda

# COMMAND ----------

# MAGIC %sh python --version

# COMMAND ----------

# MAGIC %pip install pybedtools

# COMMAND ----------

# MAGIC %pip install pybedtools click jinja2==2.11.3 nptyping==1.3.0  opt_einsum>=3.2.0 pandas==1.2.4 pytest pyyaml scipy==1.6.2 scikit-learn==0.24.1 sphinx sphinx_rtd_theme statsmodels==0.12.2 typeguard==2.9.1 yapf==0.30.0

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh which java

# COMMAND ----------

# MAGIC %sh java -version

# COMMAND ----------

# MAGIC %sh which git

# COMMAND ----------

# MAGIC %sh cd /home/ubuntu/; mkdir -p build; cd build; git clone https://github.com/projectglow/glow.git

# COMMAND ----------

#https://github.com/projectglow/glow

# COMMAND ----------

# MAGIC %sh find /home/ubuntu/build

# COMMAND ----------

# MAGIC %md command below can take while

# COMMAND ----------

# MAGIC %sh sudo apt-get update

# COMMAND ----------

# MAGIC %sh sudo apt-get install apt-transport-https curl gnupg -yqq

# COMMAND ----------

# MAGIC %sh echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
# MAGIC echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
# MAGIC curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
# MAGIC sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
# MAGIC sudo apt-get update

# COMMAND ----------

# MAGIC %sh export DEBIAN_FRONTEND=noninteractive; apt-get -yq install sbt

# COMMAND ----------

# MAGIC %sh cd /home/ubuntu/build/glow; export SCALA_VERSION=2.12.8; export SPARK_VERSION=3.3.1; sbt compile

# COMMAND ----------

# MAGIC %sh cd /home/ubuntu/build/glow; export SCALA_VERSION=2.12.8; export SPARK_VERSION=3.3.1; sbt core/assembly

# COMMAND ----------

# MAGIC %sh lscpu

# COMMAND ----------

# MAGIC %sh cd /home/ubuntu/build/glow/python; python setup.py bdist_wheel

# COMMAND ----------

# MAGIC %sh ls /home/ubuntu/build/glow/python/dist

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/glow_build/')

# COMMAND ----------

dbutils.fs.cp('file:/home/ubuntu/build/glow/python/dist/glow.py-1.2.1-py3-none-any.whl', 'dbfs:/FileStore/glow_build/glow.py-1.2.1-py3-none-any.whl')

# COMMAND ----------

# MAGIC %fs ls file:/home/ubuntu/build/glow/core/target/scala-2.12

# COMMAND ----------

dbutils.fs.cp('file:/home/ubuntu/build/glow/core/target/scala-2.12/glow-spark3-assembly-1.2.2-SNAPSHOT.jar', 'dbfs:/FileStore/glow_build/glow-spark3-assembly-1.2.2-SNAPSHOT.jar')


# COMMAND ----------

dbutils.fs.cp('file:/home/ubuntu/build/glow/python/dist/glow.py-1.2.1-py3-none-any.whl', 'dbfs:/FileStore/glow_build/glow.py-1.2.1-py3-none-any.whl')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh wget https://www.cottongen.org/cottongen_downloads/Gossypium_raimondii/JGI_221_G.raimondii_Dgenome/Udall_D5ref_snp_vcf/D13.snp4.0.vcf.gz

# COMMAND ----------

# MAGIC %sh cp D13.snp4.0.vcf.gz /dbfs/FileStore/D13.snp4.0.vcf.gz

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


