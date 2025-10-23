# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

# MAGIC %pip install feature_engine 

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient,FeatureLookup
from sklearn import model_selection
from feature_engine import imputation
import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 500)
from sklearn import ensemble
from sklearn import pipeline
from sklearn import metrics
from tqdm import tqdm
import mlflow
mlflow.set_experiment(experiment_id=1014072875431853)

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

# Como essas Algumas variaveis estao no formato de object mas sao numericas,vou fazer a conversao para float

#'nrTransacaoDiaD7',
#'nrTransacaoDiaD28',
#'nrTransacaoDiaD56',
#'nrTransacaoDiaVida'

df_train_pandas_cat = df_train_pandas.columns[df_train_pandas.dtypes == 'object'][2:].to_list()

df_train_pandas[df_train_pandas_cat] = df_train_pandas[df_train_pandas_cat].astype(float)
df_train_pandas.head()

# COMMAND ----------

# DBTITLE 1,SAMPLE

# Sepando minha base entre treino e teste

features = df_train_pandas.columns.tolist()[2:-1]

target = 'flChurn'

X = df_train_pandas[features]
y = df_train_pandas[target]


X_train, X_test, y_train, y_test = model_selection.train_test_split(X,y,test_size=0.3,random_state=42)

X_train.head()


# COMMAND ----------

# DBTITLE 1,EXPLORE
# Explorar os dados

# Explorando features do tipo object
cat_features = X_train.columns[X_train.dtypes == 'object'].to_list()
cat_features


# COMMAND ----------

# DBTITLE 1,EXPLORE
# Explorando dados nulos
X_train.isna().sum()

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
df_describe = X_train.copy()
df_describe[target] = y_train.copy()

describe = df_describe.groupby(target)[features].mean().T

describe[0] = describe[0].replace(0,np.nan)

describe["ratio"] = describe[1] / describe[0]

describe


# COMMAND ----------

# DBTITLE 1,MODIFY
# Modificando meus dados 

# Fazendo o input de zero nas features onde contem dados nulos

features_input_zeros = [

'nrqtdepontosnegativosvida',                
'nrqtdepontospositivosd7',                 
'nrqtdepontosnegativosd7' ,    
'nrqtdepontosnegativosd28' ,    
'nrqtdepontosnegativosd56' ,    
'nrPctTransacoesChurn_5pp' ,    
'nrPctTransacoesChurn_10pp' ,    
'nrPctTransacoesChurn_2pp' ,    
'nrPctTransacoesAirflowLover' ,    
'nrPctTransacoesDailyLoot'  ,    
'nrPctTransacoesListaPresenca' ,    
'nrPctTransacoesPresencaStreak' ,    
'nrPctTransacoesRLover',    
'nrPctTransacoesResgatarPonei',    
'nrPctTransacoesTrocaPontosStreamElements',    
'nrPctTransacoesitemVenda',    
'nrTransacaoDiaD7',    
'nrqtdeTransacaoManha',    
'nrqtdeTransacaoTarde',    
'nrqtdeTransacaoNoite',    
'nrqntpontosminuto',    
'nrqnttransacaominuto',
'nrqntmenssagensminuto'

]

input_zeros = imputation.ArbitraryNumberImputer(variables=features_input_zeros
                                  ,arbitrary_number=0)




# COMMAND ----------

# DBTITLE 1,MODEL
# FAZENDO A MODELAGEM DO MEU MODELO 
model = ensemble.RandomForestClassifier(n_estimators=500,min_samples_leaf=100,random_state=42,n_jobs=-1)

params = {"min_samples_leaf": [50,100,200,300,500],
          "n_estimators": [100,200,300,400,500],
          }

grid = model_selection.GridSearchCV(param_grid=params
                                    ,cv=3
                                    ,verbose=4
                                    ,estimator=model
                                    ,n_jobs=1
                                    ,scoring='roc_auc')

model_pipeline = pipeline.Pipeline(steps=[('input_zeros',input_zeros),
                                          ('grid',grid)])


# COMMAND ----------

# DBTITLE 1,ASSESS

with tqdm(mlflow.start_run()) :

    mlflow.sklearn.autolog()

    model_pipeline.fit(X_train,y_train)

    # CALCULANDO MEUS PREDICTS
    predict_train = model_pipeline.predict(X_train)

    predict_test = model_pipeline.predict(X_test)

    pridict_proba_train = model_pipeline.predict_proba(X_train)[:,1]

    pridict_proba_test = model_pipeline.predict_proba(X_test)[:,1]

    acc_train = metrics.accuracy_score(y_train,predict_train)
    acc_test = metrics.accuracy_score(y_test,predict_test)
  
    auc_train = metrics.roc_auc_score(y_train,pridict_proba_train)
    auc_test = metrics.roc_auc_score(y_test,pridict_proba_test)

    mlflow.log_metrics({
        'acc_train':acc_train,
        'acc_test':acc_test,
        'auc_train':auc_train,
        'auc_test':auc_test})
    
