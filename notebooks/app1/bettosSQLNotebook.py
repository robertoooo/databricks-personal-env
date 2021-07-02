# Databricks notebook source
blob_account_name = "pandemicdatalake"
blob_container_name = "public"
blob_relative_path = "curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet"
blob_sas_token = r""
# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)

spark.conf.set(
    'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
    blob_sas_token)
df = spark.read.parquet(wasbs_path)
display(df.limit(10))

# COMMAND ----------

covidpatients = df.select(
    "id", "country_region",
    "deaths")

# Populate a temporary view so we can query from SQL
covidpatients.createOrReplaceTempView("covid_patients")

covidpatients.show(100)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW top_country_deatsh
# MAGIC AS
# MAGIC     select country_region, SUM(deaths) AS totaldeaths
# MAGIC     from covid_patients
# MAGIC     where country_region != "Worldwide"
# MAGIC     group by country_region
# MAGIC     
# MAGIC     order by totaldeaths DESC

# COMMAND ----------

top5Products = sqlContext.table("top_country_deatsh")

top5Products.show(100)

# COMMAND ----------

CONTAINER = "bettocontqainer123"
SASTOKEN = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacuptfx&se=2021-10-15T20:06:39Z&st=2021-06-30T12:06:39Z&spr=https&sig=jsbVjOfttKkkoBkYqf15U3VEz04eeVZgKUfo6a4ylU4%3D"
# Redefine the source and URI for the new container
SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct="bettosblob123")
URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct="bettosblob123")
               
# Set up container SAS
spark.conf.set(URI, SASTOKEN)

# COMMAND ----------

dbutils.fs.ls(SOURCE)

# COMMAND ----------

# Creating widgets for leveraging parameters, and printing the parameters

dbutils.widgets.text("input", "","")
y = dbutils.widgets.get("input")

top5Products.write.parquet(SOURCE + str(y) + '.parquet')