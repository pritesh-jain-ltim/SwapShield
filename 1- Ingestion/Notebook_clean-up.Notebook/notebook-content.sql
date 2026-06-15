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
-- META     "environment": {
-- META       "environmentId": "984afcaa-23e9-b9cd-4767-36fc47c468cb",
-- META       "workspaceId": "00000000-0000-0000-0000-000000000000"
-- META     },
-- META     "warehouse": {}
-- META   }
-- META }

-- CELL ********************

delete from processed_trade_files;
delete from processed_event_files;
delete from processed_client_files;
delete from processed_instrument_files;
delete from processed_relationship_files;

delete from relationships_raw;
delete from instruments_raw;
delete from events_raw;
delete from clients_raw;
delete from trades_raw;

delete from feature_trade_base;
delete from feature_client_hop1;
delete from feature_client_hop3;
delete from feature_event_proximity;
delete from feature_store_trade_ml;
delete from feature_store_trade_ml_csv_pipeline;


delete from trade_scores_from_csv_landing;
delete from trade_scores_from_feature_store;

delete from iq_clients;
delete from iq_events;
delete from iq_instruments;
delete from iq_trades;
delete from iq_agent_input;





-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
