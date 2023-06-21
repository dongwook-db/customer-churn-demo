# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ${catalog};
# MAGIC use schema ${schema};

# COMMAND ----------

cloud_storage_path = dbutils.widgets.get("cloud_storage_path")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Silver data: anonimized table, date cleaned
# MAGIC
# MAGIC <img width="700px" style="float:right" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-2.png"/>
# MAGIC
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

(spark.readStream 
        .table("churn_users_bronze")
        .withColumnRenamed("id", "user_id")
        .withColumn("email", sha1(col("email")))
        .withColumn("creation_date", to_timestamp(col("creation_date"), "MM-dd-yyyy H:mm:ss"))
        .withColumn("last_activity_date", to_timestamp(col("last_activity_date"), "MM-dd-yyyy HH:mm:ss"))
        .withColumn("firstname", initcap(col("firstname")))
        .withColumn("lastname", initcap(col("lastname")))
        .withColumn("age_group", col("age_group").cast('int'))
        .withColumn("gender", col("gender").cast('int'))
        .withColumn("churn", col("churn").cast('int'))
        .drop(col("_rescued_data"))
     .writeStream
        .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/users")
        .trigger(once=True)
        .table("churn_users").awaitTermination())

# COMMAND ----------

# MAGIC %sql select * from churn_users;

# COMMAND ----------

(spark.readStream 
        .table("churn_orders_bronze")
        .withColumnRenamed("id", "order_id")
        .withColumn("amount", col("amount").cast('int'))
        .withColumn("item_count", col("item_count").cast('int'))
        .drop(col("_rescued_data"))
     .writeStream
        .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/orders")
        .trigger(once=True)
        .table("churn_orders").awaitTermination())

# COMMAND ----------

# MAGIC %sql select * from churn_orders_bronze;
