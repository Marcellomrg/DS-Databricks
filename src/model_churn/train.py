# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient,FeatureLookup

def import_query(path):
    with open(path) as open_file:
        return open_file.read()

lookups = [
    FeatureLookup(table_name="feature_store.upsell.fs_geral",lookup_key=['dtRef','IdCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_pontos",lookup_key=['dtRef','IdCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_transacoes",lookup_key=['dtRef','IdCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_dia_horario",lookup_key=['dtRef','IdCliente'])
]

query = import_query('fl_churn.sql')
df = spark.sql(query)


fe = FeatureEngineeringClient()
training_set = fe.create_training_set(df=df, feature_lookups=lookups, label="flChurn")

df_train = training_set.load_df()

# COMMAND ----------

df_train_pandas  = df_train.toPandas()
df_train_pandas.head()

# COMMAND ----------

# DBTITLE 1,SAMPLE
from sklearn import model_selection

# Sepando minha base entre treino e teste

features = df_train_pandas.columns.tolist()[2:-1]

target = 'flChurn'

X = df_train_pandas[features]
y = df_train_pandas[target]

X_train, X_test, y_train, y_test = model_selection.train_test_split(X,y,test_size=0.3,random_state=42)

X_train.head()
