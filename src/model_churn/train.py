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

import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 500)

# Sepando minha base entre treino e teste

features = df_train_pandas.columns.tolist()[2:-1]

target = 'flChurn'

X = df_train_pandas[features]
y = df_train_pandas[target]


X_train, X_test, y_train, y_test = model_selection.train_test_split(X,y,test_size=0.3,random_state=42)

X_train.head()

x_train, x_test, y_train, y_test = model_selection.train_test_split(X,y,test_size=0.3,random_state=42)

x_train.head()

# COMMAND ----------

# DBTITLE 1,EXPLORE
# Explorar os dados

# Explorando features do tipo object
cat_features = x_train.columns[x_train.dtypes == 'objetc']


# COMMAND ----------

# DBTITLE 1,EXPLORE
# Explorando dados nulos
x_train.isna().sum()

#nrqtdepontosnegativosvida                  --->      1329
#nrqtdepontospositivosd7                    --->       777
#nrqtdepontosnegativosd7                    --->      1411
#nrqtdepontosnegativosd28                   --->      1362
#nrqtdepontosnegativosd56                   --->      1342
#nrPctTransacoesChurn_5pp                   --->      1419
#nrPctTransacoesChurn_10pp                  --->      1419
#nrPctTransacoesChurn_2pp                   --->      1419
#nrPctTransacoesAirflowLover                --->      1419
#nrPctTransacoesDailyLoot                   --->      1419
#nrPctTransacoesListaPresenca               --->      1419
#nrPctTransacoesPresencaStreak              --->      1419
#nrPctTransacoesRLover                      --->      1419
#nrPctTransacoesResgatarPonei               --->      1419
#nrPctTransacoesTrocaPontosStreamElements   --->      1419
#nrPctTransacoesitemVenda                   --->      1419
#nrTransacaoDiaD7                           --->       777
#nrqtdeTransacaoManha                       --->       220
#nrqtdeTransacaoTarde                       --->      1295
#nrqtdeTransacaoNoite                       --->       985
#nrqntpontosminuto                          --->       595
#nrqnttransacaominuto                       --->       595
#nrqntmenssagensminuto                      --->       595

# COMMAND ----------

# DBTITLE 1,EXPLORE

# Fazendo uma analise Descritiva de cada feature em Relacao a minha variavel target
df_describe = x_train.copy()
df_describe[target] = y_train.copy()

describe = df_describe.groupby(target)[features].mean().T

describe[0] = describe[0].replace(0,np.nan)

describe["ratio"] = describe[1] / describe[0]

describe


# COMMAND ----------

# DBTITLE 1,MODIFY

