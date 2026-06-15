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

# # Notebook 1 - CSV Landing to ML Training
# 
# Purpose:
# 1. Read newly landed CSV files from the attached Lakehouse `Files/` area.
# 2. Persist raw Delta/Lakehouse tables.
# 3. Build a model-ready feature set in Spark.
# 4. Train an insider-detection model and save scored output back to the Lakehouse.
# 
# Expected landed files (after SharePoint -> Fabric ingestion):
# - `Files/trade_surveillance/landing/trades/*.csv`
# - `Files/trade_surveillance/landing/clients/*.csv`
# - `Files/trade_surveillance/landing/events/*.csv`
# - `Files/trade_surveillance/landing/instruments/*.csv`
# - `Files/trade_surveillance/landing/relationships/*.csv`
# 
# invoke it from a Fabric pipeline Notebook activity after ingestion step.

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/trade_surveillance/landing/trades/trades.csv")
# df now is a Spark DataFrame containing CSV data from "Files/trade_surveillance/landing/trades/trades.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters / configurable paths
EXPERIMENT_NAME = 'swap_surveillance_csv_landing_experiment'
MODEL_NAME = 'insider_detection_from_csv_landing'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#%pip install imblearn scikit-learn==1.6.1 mlflow==2.12.2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
import mlflow.sklearn
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
print('Libraries loaded successfully')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Default paths 
TRADES_DIR = 'Files/trade_surveillance/landing/trades'  # directory, not wildcard
CLIENTS_DIR = 'Files/trade_surveillance/landing/clients'
EVENTS_DIR = 'Files/trade_surveillance/landing/events'
INSTRUMENTS_DIR = 'Files/trade_surveillance/landing/instruments'
RELATIONSHIPS_DIR = 'Files/trade_surveillance/landing/relationships'



def incremental_load_from_dir(src_dir: str,
                              raw_table: str,
                              processed_meta_table: str,
                              date_cast_columns=None,
                              numeric_cast_columns=None):
    """Generic helper to load only new CSV files from a directory into a Delta table.

    - src_dir: Files/<...> directory path
    - raw_table: target Delta table name (e.g. 'trades_raw')
    - processed_meta_table: metadata table name (e.g. 'processed_trade_files')
    - date_cast_columns: list of column names to cast to date
    - numeric_cast_columns: list of column names to cast to double
    """
    if date_cast_columns is None:
        date_cast_columns = []
    if numeric_cast_columns is None:
        numeric_cast_columns = []

    # 1) List all CSV files currently in the landing folder
    files_info = notebookutils.fs.ls(src_dir)
    file_paths = [f.path for f in files_info if f.path.lower().endswith('.csv')]

    if not file_paths:
        print(f"No CSV files found in {src_dir}.")
        return None

    all_files_df = spark.createDataFrame(file_paths, 'string').toDF('file_path')

    # 2) Work out which files are new using metadata table
    if spark.catalog.tableExists(processed_meta_table):
        processed_df = spark.table(processed_meta_table).select('file_path').distinct()
        new_files_df = all_files_df.join(processed_df, 'file_path', 'left_anti')
    else:
        new_files_df = all_files_df

    new_file_paths = [r.file_path for r in new_files_df.collect()]

    if not new_file_paths:
        print(f"No new files to process for {raw_table} from {src_dir}.")
        return None

    print(f"Found {len(new_file_paths)} new file(s) to process for {raw_table} from {src_dir}.")

    # 3) Read only the new CSV files as the current batch
    batch_df = spark.read.option('header', True).csv(new_file_paths)

    # Basic type casting on the NEW batch
    for col_name in date_cast_columns:
        if col_name in batch_df.columns:
            batch_df = batch_df.withColumn(col_name, F.to_date(col_name))

    for col_name in numeric_cast_columns:
        if col_name in batch_df.columns:
            batch_df = batch_df.withColumn(col_name, F.col(col_name).cast('double'))

    # 4) Append the new batch into the persistent raw table
    write_mode = 'append' if spark.catalog.tableExists(raw_table) else 'overwrite'
    batch_df.write.mode(write_mode).format('delta').saveAsTable(raw_table)

    # 5) Update metadata table of processed files
    processed_to_add = new_files_df.withColumn('load_ts', F.current_timestamp())
    processed_write_mode = 'append' if spark.catalog.tableExists(processed_meta_table) else 'overwrite'
    processed_to_add.write.mode(processed_write_mode).format('delta').saveAsTable(processed_meta_table)

    return batch_df

# --- Trades ---
trades_batch_df = incremental_load_from_dir(
    src_dir=TRADES_DIR,
    raw_table='trades_raw',
    processed_meta_table='processed_trade_files',
    date_cast_columns=['trade_date', 'booking_date', 'maturity_date'],
    numeric_cast_columns=['notional', 'quantity', 'price', 'dividend_rate', 'financing_spread']
)

# 6) Load the full trades_raw table (all history) for downstream logic
if spark.catalog.tableExists('trades_raw'):
    trades_df = spark.table('trades_raw')
else:
    trades_df = spark.createDataFrame([], schema=None)

# --- Clients ---
clients_batch_df = incremental_load_from_dir(
    src_dir=CLIENTS_DIR,
    raw_table='clients_raw',
    processed_meta_table='processed_client_files'
)
if spark.catalog.tableExists('clients_raw'):
    clients_df = spark.table('clients_raw')
else:
    clients_df = spark.createDataFrame([], schema=None)

# --- Events ---
events_batch_df = incremental_load_from_dir(
    src_dir=EVENTS_DIR,
    raw_table='events_raw',
    processed_meta_table='processed_event_files',
    date_cast_columns=['event_date']
)
if spark.catalog.tableExists('events_raw'):
    events_df = spark.table('events_raw')
else:
    events_df = spark.createDataFrame([], schema=None)

# --- Instruments ---
instruments_batch_df = incremental_load_from_dir(
    src_dir=INSTRUMENTS_DIR,
    raw_table='instruments_raw',
    processed_meta_table='processed_instrument_files'
)
if spark.catalog.tableExists('instruments_raw'):
    instruments_df = spark.table('instruments_raw')
else:
    instruments_df = spark.createDataFrame([], schema=None)

# --- Relationships ---
relationships_batch_df = incremental_load_from_dir(
    src_dir=RELATIONSHIPS_DIR,
    raw_table='relationships_raw',
    processed_meta_table='processed_relationship_files'
)
if spark.catalog.tableExists('relationships_raw'):
    relationships_df = spark.table('relationships_raw')
else:
    relationships_df = spark.createDataFrame([], schema=None)

print('Raw tables written incrementally for trades, clients, events, instruments, and relationships.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) Build graph metrics
out_deg = relationships_df.groupBy(F.col('from_client').alias('client_id')).agg(F.count('*').alias('out_degree'))
in_deg = relationships_df.groupBy(F.col('to_client').alias('client_id')).agg(F.count('*').alias('in_degree'))
neighbors_1hop = relationships_df.groupBy(F.col('from_client').alias('client_id')).agg(F.countDistinct('to_client').alias('distinct_neighbors_1hop'))

a = relationships_df.alias('a')
b = relationships_df.alias('b')
c = relationships_df.alias('c')
hop3 = (a.join(b, F.col('a.to_client') == F.col('b.from_client'))
         .join(c, F.col('b.to_client') == F.col('c.from_client'))
         .select(F.col('a.from_client').alias('client_id'),
                 F.col('a.to_client').alias('hop1_client'),
                 F.col('b.to_client').alias('hop2_client'),
                 F.col('c.to_client').alias('hop3_client'))
         .withColumn('in_three_hop_loop', F.when(F.col('client_id') == F.col('hop3_client'), F.lit(1)).otherwise(F.lit(0))))
hop3_agg = hop3.groupBy('client_id').agg(F.count('*').alias('hop3_path_count'),
                                      F.sum('in_three_hop_loop').alias('three_hop_loop_count'),
                                      F.max('in_three_hop_loop').alias('in_three_hop_loop'))

client_graph = (clients_df
    .join(out_deg, 'client_id', 'left')
    .join(in_deg, 'client_id', 'left')
    .join(neighbors_1hop, 'client_id', 'left')
    .join(hop3_agg, 'client_id', 'left')
    .fillna({'out_degree':0,'in_degree':0,'distinct_neighbors_1hop':0,'hop3_path_count':0,'three_hop_loop_count':0,'in_three_hop_loop':0})
    .withColumn('has_1hop_link', F.when(F.col('distinct_neighbors_1hop') >= 1, F.lit(1)).otherwise(F.lit(0)))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4) Event proximity features
trade_events = (trades_df.alias('t')
    .join(events_df.alias('e'), F.col('t.underlying') == F.col('e.underlying'), 'left')
    .withColumn('days_to_event', F.datediff(F.col('e.event_date'), F.col('t.trade_date')))
    .filter((F.col('days_to_event') >= 0) & (F.col('days_to_event') <= 30) | F.col('days_to_event').isNull())
)
event_features = (trade_events.groupBy('trade_id')
    .agg(F.coalesce(F.min('days_to_event'), F.lit(9999)).alias('days_to_next_event'),
         F.max(F.when((F.col('days_to_event') >= 0) & (F.col('days_to_event') <= 7), 1).otherwise(0)).alias('event_within_7d'),
         F.max(F.when((F.col('days_to_event') >= 0) & (F.col('days_to_event') <= 3), 1).otherwise(0)).alias('event_within_3d'),
         F.max(F.when(F.upper(F.col('importance')) == 'HIGH', 1).otherwise(0)).alias('has_high_importance_event'),
         F.max(F.when(F.upper(F.col('event_type')).contains('EARNING'), 1).otherwise(0)).alias('has_earnings_event'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5) Build final feature store from CSV landing path
feature_store = (trades_df
    .join(clients_df.select('client_id','country','risk_rating'), 'client_id', 'left')
    .join(instruments_df.select('underlying','isin','instrument_id','sector'), ['underlying','isin'], 'left')
    .join(client_graph.select('client_id','out_degree','in_degree','distinct_neighbors_1hop','has_1hop_link','hop3_path_count','three_hop_loop_count','in_three_hop_loop'), 'client_id', 'left')
    .join(event_features, 'trade_id', 'left')
    .fillna({'out_degree':0,'in_degree':0,'distinct_neighbors_1hop':0,'has_1hop_link':0,'hop3_path_count':0,'three_hop_loop_count':0,'in_three_hop_loop':0, 'days_to_next_event':9999, 'event_within_7d':0, 'event_within_3d':0, 'has_high_importance_event':0, 'has_earnings_event':0})
    .withColumn('jurisdiction_risk_score', F.when(F.upper(F.col('jurisdiction_risk')) == 'HIGH', 3).when(F.upper(F.col('jurisdiction_risk')) == 'MEDIUM', 2).otherwise(1))
    .withColumn('kyc_risk_score', F.when(F.upper(F.col('risk_rating')) == 'HIGH', 3).when(F.upper(F.col('risk_rating')) == 'MEDIUM', 2).otherwise(1))
    .withColumn('notional_ge_1bn', F.when(F.col('notional') >= 1_000_000_000, 1).otherwise(0))
    .withColumn('notional_ge_500m', F.when(F.col('notional') >= 500_000_000, 1).otherwise(0))
    .withColumn('label_insider', F.when(F.upper(F.col('risk_flag')) == 'INSIDER', 1).otherwise(0))
)

feature_store.write.mode('overwrite').format('delta').saveAsTable('feature_store_trade_ml_csv_pipeline')
display(feature_store.orderBy('trade_id'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6) Convert to pandas and train model
pdf = feature_store.toPandas()

# Basic sanity check: if we have fewer than 2 classes or too few minority samples,
# skip SMOTE-like resampling to avoid errors like:
# "Expected n_neighbors <= n_samples_fit".

# Build target/feature sets safely
if 'label_insider' not in pdf.columns:
    raise ValueError("Expected 'label_insider' column in feature_store for modeling.")

target_col = 'label_insider'
feature_cols = [
    'notional','quantity','price','dividend_rate','financing_spread','out_degree','in_degree','distinct_neighbors_1hop',
    'has_1hop_link','hop3_path_count','three_hop_loop_count','in_three_hop_loop',
    'days_to_next_event','event_within_7d','event_within_3d','has_high_importance_event','has_earnings_event',
    'jurisdiction_risk_score','kyc_risk_score','notional_ge_1bn','notional_ge_500m',
    'underlying','direction','desk','legal_entity','portfolio','jurisdiction_risk','risk_rating','sector'
]

# Keep only the needed columns and handle missing categorical values
pdf = pdf[feature_cols + [target_col]].copy()
pdf = pdf.fillna({
    'sector':'UNKNOWN',
    'risk_rating':'UNKNOWN',
    'jurisdiction_risk':'UNKNOWN',
    'desk':'UNKNOWN',
    'legal_entity':'UNKNOWN',
    'portfolio':'UNKNOWN',
    'direction':'UNKNOWN',
    'underlying':'UNKNOWN'
})

X = pdf[feature_cols]
y = pdf[target_col]

# Identify categorical vs numeric columns
categorical_cols = ['underlying','direction','desk','legal_entity','portfolio','jurisdiction_risk','risk_rating','sector']
numeric_cols = [c for c in feature_cols if c not in categorical_cols]

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_cols),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
    ]
)

# Use RandomForest with class_weight instead of SMOTE to avoid small-sample issues
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=6,
    random_state=42,
    class_weight='balanced'  # handle class imbalance without SMOTE
)

pipeline = Pipeline(steps=[
    ('prep', preprocessor),
    ('model', model)
])

# Guard: need at least 2 classes to train a classifier
unique_labels = y.unique()
if len(unique_labels) < 2:
    raise ValueError(f"Need at least 2 classes to train classifier; found classes: {unique_labels}")

# Stratified split may still fail if the minority class has only 1 sample; handle gracefully
try:
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.4,
        random_state=42,
        stratify=y
    )
except ValueError:
    # Fallback: no stratification if class counts are too small
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.4,
        random_state=42,
        stratify=None
    )

mlflow.set_experiment(EXPERIMENT_NAME)
with mlflow.start_run(run_name='random_forest_from_csv_landing'):
    pipeline.fit(X_train, y_train)
    preds = pipeline.predict(X_test)
    proba = pipeline.predict_proba(X_test)[:,1] if hasattr(pipeline, 'predict_proba') else None

    precision = precision_score(y_test, preds, zero_division=0)
    recall = recall_score(y_test, preds, zero_division=0)
    f1 = f1_score(y_test, preds, zero_division=0)
    roc_auc = roc_auc_score(y_test, proba) if proba is not None and len(set(y_test)) > 1 else None

    mlflow.log_param('model_type', 'RandomForestClassifier')
    mlflow.log_param('source_mode', 'csv_landing')
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

# Score full dataset
full_pred = pipeline.predict(X)
full_proba = pipeline.predict_proba(X)[:,1] if hasattr(pipeline, 'predict_proba') else [None] * len(X)
scored = feature_store.toPandas()[['trade_id','client_id','underlying','risk_flag']].copy()
scored['predicted_label'] = full_pred
scored['predicted_probability'] = full_proba
spark.createDataFrame(scored).write.mode('overwrite').format('delta').saveAsTable('trade_scores_from_csv_landing')

display(spark.table('trade_scores_from_csv_landing').orderBy(F.desc('predicted_probability')))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
