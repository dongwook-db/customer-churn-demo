# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## 5/ Enriching the gold data with a ML model
# MAGIC
# MAGIC <img width="700px" style="float:right" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-5.png"/>
# MAGIC
# MAGIC Our Data scientist team has build a churn prediction model using Auto ML and saved it into Databricks Model registry. 
# MAGIC
# MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline. 
# MAGIC
# MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalog};
# MAGIC use schema ${schema};

# COMMAND ----------

# DBTITLE 1,Load the model as SQL function
# MAGIC %python
# MAGIC import mlflow
# MAGIC #                                                                              Stage/version    output
# MAGIC #                                                                 Model name         |            |
# MAGIC #                                                                     |              |            |
# MAGIC predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/dongwook_demos_customer_churn/Production", "int")

# COMMAND ----------

# DBTITLE 1,Call our model and predict churn in our pipeline
model_features = predict_churn_udf.metadata.get_input_schema().input_names()
predictions = spark.table('churn_features').withColumn('churn_prediction', predict_churn_udf(*model_features)).write.format("delta").mode("overwrite").saveAsTable("churn_prediction")
