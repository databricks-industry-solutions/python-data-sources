# Databricks notebook source
# MAGIC %md
# MAGIC # Read Zipped DICOM files saving time and storage
# MAGIC - WIth the custom "zipdcm" Python Data Source, we can read zipped (and non Zipped) up DICOM files directly to extract their metadata.

# COMMAND ----------

from dbx.zip_dcm_ds import ZipDCMDataSource
spark.dataSource.register(ZipDCMDataSource)

df = spark.read.format("zipdcm").load("./resources")
df.display()
