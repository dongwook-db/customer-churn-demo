# Databricks notebook source
# MAGIC %run ../../_resources/00-setup $reset_all_data=true $db_prefix=retail

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Explore the dataset
# MAGIC
# MAGIC Let's review the files being received

# COMMAND ----------

# MAGIC %fs ls /demos/retail/churn/users

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalog};
# MAGIC use schema ${schema};

# COMMAND ----------

cloud_storage_path = dbutils.widgets.get("cloud_storage_path")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="700px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-1.png"/>
# MAGIC </div>
# MAGIC   
# MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC
# MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
# MAGIC
# MAGIC Let's use it to [create our pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/95f28631-1884-425e-af69-05c3f397dd90) and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/retail/churn/...`. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS user_bronze;
# MAGIC -- Note: tables are automatically created during  .writeStream.table("user_bronze") operation, but we can also use plain SQL to create them:
# MAGIC CREATE TABLE IF NOT EXISTS user_bronze (
# MAGIC      id                 STRING,
# MAGIC      email              STRING,
# MAGIC      creation_date      TIMESTAMP,
# MAGIC      last_activity_date TIMESTAMP,
# MAGIC      firstname          STRING,
# MAGIC      lastname           STRING,
# MAGIC      address            STRING,
# MAGIC      age_group          INT,
# MAGIC      canal              STRING,
# MAGIC      churn              INT,
# MAGIC      country            STRING,
# MAGIC      gender             INT
# MAGIC      
# MAGIC   ) using delta tblproperties (
# MAGIC      delta.autooptimize.optimizewrite = TRUE,
# MAGIC      delta.autooptimize.autocompact   = TRUE ); 
# MAGIC -- With these 2 last options, Databricks engine will solve small files & optimize write out of the box!

# COMMAND ----------

def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                              .format("cloudFiles")
                              .option("cloudFiles.format", data_format)
                              .option("cloudFiles.inferColumnTypes", "true")
                              .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                              .load(folder))

  return (bronze_products.writeStream
                    .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
                    .option("mergeSchema", "true") #merge any new column dynamically
                    .trigger(once = True) #Remove for real time streaming
                    .table(table)) #Table will be created if we haven't specified the schema first
  
ingest_folder('/demos/retail/churn/orders', 'json', 'churn_orders_bronze')
ingest_folder('/demos/retail/churn/events', 'csv', 'churn_app_events')
ingest_folder('/demos/retail/churn/users', 'json',  'churn_users_bronze').awaitTermination()

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it'll be stored here
# MAGIC select * from churn_users_bronze;
