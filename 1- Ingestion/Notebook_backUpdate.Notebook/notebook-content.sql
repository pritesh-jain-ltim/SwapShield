-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "10d8e90a-037d-44ab-b9d2-dff3d5c3b7e3",
-- META       "default_lakehouse_name": "IQ_Ontology_lh_26019883a4f241dda9453a5f88a6f367",
-- META       "default_lakehouse_workspace_id": "403ac573-7599-48cb-ac00-cddcaad06fa9",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "10d8e90a-037d-44ab-b9d2-dff3d5c3b7e3"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "a9bfa83d-d87d-4f5c-acf9-be5b120f319d",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "a9bfa83d-d87d-4f5c-acf9-be5b120f319d",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

UPDATE dbo.instruments_raw set isin='INE009A01021' where underlying='INFY';
UPDATE dbo.instruments_raw set isin='INE759A01021' where underlying='MASTEK';

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
