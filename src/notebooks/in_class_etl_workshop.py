# Databricks notebook source
# MAGIC %md #### In-Class Workshop - GR5069

# COMMAND ----------

# MAGIC %md ###### Read Dataset from S3

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes = spark.read.csv("s3://columbia-gr5069-main/raw/lap_times.csv", header = True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_drivers = spark.read.csv("s3://columbia-gr5069-main/raw/drivers.csv", header = True)

# COMMAND ----------

# MAGIC %md #### Transform Data

# COMMAND ----------

df_drivers = df_drivers.withColumn("age", datediff(current_date(), df_drivers.dob)/365.25)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_drivers = df_drivers.withColumn("age", df_drivers["age"].case(IntegerType()))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_drivers_laptimes = df_drivers.select("driverId", "forename", "surname", "nationality", "age").join(df_laptimes, on = ["driverId"])

# COMMAND ----------

display(df_drivers_laptimes)

# COMMAND ----------

df_drivers_laptimes.count()

# COMMAND ----------

# MAGIC %md ###### Aggregate by Age

# COMMAND ----------

df_drivers_laptimes = df_drivers_laptimes.groupby("surname", "age").agg(avg('milliseconds'))

# COMMAND ----------

# MAGIC %md ###### Loading Data into S3

# COMMAND ----------

df_drivers_laptimes.write.csv("s3://cld-gr5069/processed/in_class_workshop/drivers_laptimes.csv")
