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

# CELL ********************

from pyspark.sql.functions import current_timestamp, col

# 1) Load high‑risk trades directly as Spark DF
high_risk_df = spark.sql("""
SELECT 
    trade_id,
    client_id,
    underlying,
    risk_flag,
    predicted_probability
FROM dbo.trade_scores_from_feature_store
WHERE predicted_probability > 0.80
""")

# 2) Build alert payload (Spark)
alerts_df = (
    high_risk_df
    .withColumn("risk_score", col("predicted_probability").cast("double"))
    .drop("predicted_probability")
    .withColumn("timestamp", current_timestamp())
)

# 3) Incremental append into iq_agent_input:
#    - First run: table does not exist → write all alerts
#    - Later runs: append only alerts whose trade_id is not yet present

if spark.catalog.tableExists("iq_agent_input"):
    existing_ids = spark.table("iq_agent_input").select("trade_id").distinct()
    new_alerts_df = alerts_df.join(existing_ids, on="trade_id", how="left_anti")
    write_df = new_alerts_df
else:
    write_df = alerts_df

# 4) Append only new alerts
write_df.write.mode("append").saveAsTable("iq_agent_input")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
