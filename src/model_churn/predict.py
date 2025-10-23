# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering 

# COMMAND ----------

# MAGIC %pip install mlflow feature_engine

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")
model_uri = "models:/feature_store.upsell.model_churn/2"
model = mlflow.sklearn.load_model(model_uri)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient,FeatureLookup
import pandas as pd
pd.set_option('display.max_rows', 500)



lookups = [
    FeatureLookup(table_name="feature_store.upsell.fs_geral",lookup_key=['dtRef','IdCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_pontos",lookup_key=['dtRef','IdCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_transacoes",lookup_key=['dtRef','IdCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_dia_horario",lookup_key=['dtRef','IdCliente'])
]

query = """
        SELECT dtRef,
            IdCliente 
        FROM feature_store.upsell.fs_geral 
        WHERE dtRef = (SELECT MAX(dtRef) FROM feature_store.upsell.fs_geral)
        """

df = spark.sql(query)

fe = FeatureEngineeringClient()

predict_set = fe.create_training_set(df=df, feature_lookups=lookups, label=None)

df_predict = predict_set.load_df().toPandas()

df_predict.head()

# COMMAND ----------

features = model.feature_names_in_

# COMMAND ----------

proba_churn = model.predict_proba(df_predict[features])[:,1]
df_predict['prob_churn'] = proba_churn
df_predict[['dtRef','IdCliente','prob_churn']]

