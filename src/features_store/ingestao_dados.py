# Databricks notebook source
# MAGIC %pip install databricks feature-engineering

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


from databricks.feature_engineering import FeatureEngineeringClient
from tqdm import tqdm

fe = FeatureEngineeringClient()

def import_query(path):
    with open(path) as open_file:
        return open_file.read()

# COMMAND ----------

query = import_query("fs_geral.sql")
df = spark.sql(query.format(dt_ref='2024-07-30'))
fe.write_table(name="feature_store.upsell.fs_geral"
               ,df=df
               ,mode="merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM feature_store.upsell.fs_geral

# COMMAND ----------

def table_existe(catalog,database,tables):
    count = (spark.sql("SHOW TABLES FROM feature_store.upsell").filter("tableName == 'fs_geral'").count())
    return count > 0

table_existe("feature_store","upsell","fs_geral")


# COMMAND ----------

def table_existe(catalog,database,tables):
    count = (spark.sql("SHOW TABLES FROM feature_store.upsell").filter("tableName == 'fs_geral'").count())
    return count > 0
 
query = import_query("fs_geral.sql")

dates = ["2024-02-01"
         ,"2024-03-01"
         ,"2024-04-01"
         ,"2024-05-01"
         ,"2024-06-01"
         ,"2024-07-01"]


if not table_existe("feature_store", "upsell", "fs_geral"):
    
    print("Criando Tabela....")

    df = spark.sql(query.format(dt_ref=dates.pop[0]))

    fe.create_table(name="feature_store.upsell.fs_geral"
                    ,primary_keys=["dtRef","IdCliente"]
                    ,df=df,partition_columns=["dtRef"]
                    ,schema=df.schema)
    
for d in tqdm(dates):
    df = spark.sql(query.format(dt_ref=d))
    fe.write_table(name="feature_store.upsell.fs_geral"
               ,df=df
               ,mode="merge")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dtRef,
# MAGIC count(*),
# MAGIC count(DISTINCT IdCliente)
# MAGIC FROM feature_store.upsell.fs_geral
# MAGIC GROUP BY all
