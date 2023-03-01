# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC script = """
# MAGIC   sudo apt-get install -y gdebi-core alien
# MAGIC   cd /tmp
# MAGIC   sudo wget https://download2.rstudio.org/rstudio-server-1.1.453-amd64.deb
# MAGIC   sudo gdebi -n rstudio-server-1.1.453-amd64.deb
# MAGIC   sudo rstudio-server restart
# MAGIC """
# MAGIC 
# MAGIC dbutils.fs.put("/databricks/init/rstudio/rstudio-install.sh", script, True)

# COMMAND ----------

# MAGIC %fs ls /databricks/init/

# COMMAND ----------

# MAGIC %fs ls /databricks/init/rstudio/

# COMMAND ----------


