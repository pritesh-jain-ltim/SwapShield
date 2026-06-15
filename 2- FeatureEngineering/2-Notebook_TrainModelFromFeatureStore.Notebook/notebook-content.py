# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "10d8e90a-037d-44ab-b9d2-dff3d5c3b7e3",
# META       "default_lakehouse_name": "IQ_Ontology_lh_26019883a4f241dda9453a5f88a6f367",
# META       "default_lakehouse_workspace_id": "403ac573-7599-48cb-ac00-cddcaad06fa9",
# META       "known_lakehouses": [
# META         {
# META           "id": "10d8e90a-037d-44ab-b9d2-dff3d5c3b7e3"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "984afcaa-23e9-b9cd-4767-36fc47c468cb",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook 2 - Train Model from Feature Store Tables
# 
# Purpose:
# 1. Read curated feature store tables already prepared in the Lakehouse.
# 2. Train and track an insider-detection model using MLflow.
# 3. Persist scored output for investigations / Power BI.
# 
# Expected source table:
# - `feature_store_trade_ml` (from feature engineering process)
# 
# **Metric	Meaning**
# - Precision	“When ML flags fraud, how often is it correct?”
# - Recall	“How many fraud trades did we catch?”
# - Confusion Matrix	Breakdown of correct/incorrect predictions
# 


# CELL ********************

FEATURE_TABLE = 'feature_store_trade_ml'
SCORED_TABLE = 'trade_scores_from_feature_store'
EXPERIMENT_NAME = 'swap_surveillance_feature_store_experiment'
MODEL_NAME = 'insider_detection_from_feature_store'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %pip install imblearn scikit-learn==1.6.1 mlflow==2.12.2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
import pandas as pd
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

feature_df = spark.table(FEATURE_TABLE)
display(feature_df.orderBy('trade_id'))
pdf = feature_df.toPandas()

target_col = 'label_insider'
id_cols = ['trade_id','client_id','risk_flag','underlying']
categorical_cols = ['direction','desk','legal_entity','portfolio','jurisdiction_risk','risk_rating','sector','trade_status','product_type','currency']
  
exclude_cols = set(id_cols + [target_col,'trade_date','booking_date','maturity_date','client_name','trader_id','instrument_id','country','isin'])
feature_cols = [c for c in pdf.columns if c not in exclude_cols]
numeric_cols = [c for c in feature_cols if c not in categorical_cols]


pdf[categorical_cols] = pdf[categorical_cols].fillna('UNKNOWN')
X = pdf[feature_cols]
y = pdf[target_col]

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_cols),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
    ],
    remainder='drop'
)

pipeline = ImbPipeline(steps=[
    ('prep', preprocessor),
     #removed ('smote', SMOTE(random_state=42, k_neighbors=1)),
    ('model', RandomForestClassifier(n_estimators=300, max_depth=8, random_state=42, class_weight='balanced'))
])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42, stratify=y)
mlflow.set_experiment(EXPERIMENT_NAME)
with mlflow.start_run(run_name='random_forest_from_feature_store'):
    pipeline.fit(X_train, y_train)
    preds = pipeline.predict(X_test)
    proba = pipeline.predict_proba(X_test)[:,1] if hasattr(pipeline, 'predict_proba') else None

    precision = precision_score(y_test, preds, zero_division=0)
    recall = recall_score(y_test, preds, zero_division=0)
    f1 = f1_score(y_test, preds, zero_division=0)
    roc_auc = roc_auc_score(y_test, proba) if proba is not None and len(set(y_test)) > 1 else None

    mlflow.log_param('model_type', 'RandomForestClassifier')
    mlflow.log_param('source_mode', 'feature_store_table')
    mlflow.log_metric('precision', precision)
    mlflow.log_metric('recall', recall)
    mlflow.log_metric('f1', f1)
    if roc_auc is not None:
        mlflow.log_metric('roc_auc', roc_auc)
    mlflow.sklearn.log_model(pipeline, 'model')

    print('Classification report:')
    print(classification_report(y_test, preds, zero_division=0))
    print('Confusion matrix:')
    print(confusion_matrix(y_test, preds))

full_pred = pipeline.predict(X)
full_proba = pipeline.predict_proba(X)[:,1] if hasattr(pipeline, 'predict_proba') else [None] * len(X)
scored = pdf[id_cols].copy()
scored['predicted_label'] = full_pred
scored['predicted_probability'] = full_proba
spark.createDataFrame(scored).write.mode('overwrite').format('delta').option("overwriteSchema", "true").saveAsTable(SCORED_TABLE)
display(spark.table(SCORED_TABLE).orderBy(F.desc('predicted_probability')))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
