# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ${catalog};
# MAGIC use schema ${schema};

# COMMAND ----------

cloud_storage_path = dbutils.widgets.get("cloud_storage_path")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ Aggregate and join data to create our ML features
# MAGIC
# MAGIC <img width="700px" style="float:right" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-3.png"/>
# MAGIC
# MAGIC
# MAGIC We're now ready to create the features required for our Churn prediction.
# MAGIC
# MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, sucj as:
# MAGIC
# MAGIC * last command date
# MAGIC * number of item bought
# MAGIC * number of actions in our website
# MAGIC * device used (ios/iphone)
# MAGIC * ...

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE churn_features AS
      WITH 
          churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item
            FROM churn_orders GROUP BY user_id),  
          churn_app_events_stats as (
            SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
              FROM churn_app_events GROUP BY user_id)
        SELECT *, 
           datediff(now(), creation_date) as days_since_creation,
           datediff(now(), last_activity_date) as days_since_last_activity,
           datediff(now(), last_event) as days_last_event
           FROM churn_users
             INNER JOIN churn_orders_stats using (user_id)
             INNER JOIN churn_app_events_stats using (user_id)""")
     
display(spark.table("churn_features"))
