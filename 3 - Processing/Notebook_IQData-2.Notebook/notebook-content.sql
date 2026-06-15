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
-- META     "warehouse": {
-- META       "known_warehouses": []
-- META     }
-- META   }
-- META }

-- CELL ********************

-- 1) Initial creation: only if table does NOT exist yet
CREATE TABLE IF NOT EXISTS dbo.iq_trades
USING DELTA
AS
SELECT
    -- identifiers
    trade_id,
    client_id,
    underlying,   
    trade_date,   
    direction,  

    -- trade economics
    notional,    
    notional1,   
    CAST(notional AS FLOAT) / AVG(CAST(notional AS FLOAT)) OVER (
        PARTITION BY client_id
    ) AS notional_vs_avg,

    CASE 
        WHEN COUNT(*) OVER (
            PARTITION BY client_id, underlying
        ) = 1 THEN 1 
        ELSE 0 
    END AS new_exposure_flag,

    CASE 
        WHEN notional > 200000000 THEN 1 
        ELSE 0 
    END AS large_trade_flag,

    COUNT(*) OVER (
        PARTITION BY client_id
        ORDER BY trade_date
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS rolling_frequency,

    -- labels
    label_insider AS is_insider,

    -- event-related
    days_to_next_event,
    event_within_7d,
    event_within_3d,
    
    -- risk features
    jurisdiction_risk,
    jurisdiction_risk_score,
    risk_rating,
    kyc_risk_score,
    notional_ge_1bn,
    notional_ge_500m,

    -- anomaly score as a single metric for ontology/UI
    (
        CASE WHEN days_to_next_event <= 2 THEN 1 ELSE 0 END +
        CASE WHEN notional1 > 200000000 THEN 1 ELSE 0 END +
        COALESCE(jurisdiction_risk_score, 0)
    ) AS anomaly_score
FROM dbo.feature_store_trade_ml;

-- 2) Incremental append: only insert trades not yet present in dbo.iq_trades
INSERT INTO dbo.iq_trades
SELECT
    -- identifiers
    t.trade_id,
    t.client_id,
    t.underlying,   
    t.trade_date,   
    t.direction,  

    -- trade economics
    t.notional,    
    t.notional1,   
    CAST(t.notional AS FLOAT) / AVG(CAST(t.notional AS FLOAT)) OVER (
        PARTITION BY t.client_id
    ) AS notional_vs_avg,

    CASE 
        WHEN COUNT(*) OVER (
            PARTITION BY t.client_id, t.underlying
        ) = 1 THEN 1 
        ELSE 0 
    END AS new_exposure_flag,

    CASE 
        WHEN t.notional > 200000000 THEN 1 
        ELSE 0 
    END AS large_trade_flag,

    COUNT(*) OVER (
        PARTITION BY t.client_id
        ORDER BY t.trade_date
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS rolling_frequency,

    -- labels
    t.label_insider AS is_insider,

    -- event-related
    t.days_to_next_event,
    t.event_within_7d,
    t.event_within_3d,
    
    -- risk features
    t.jurisdiction_risk,
    t.jurisdiction_risk_score,
    t.risk_rating,
    t.kyc_risk_score,
    t.notional_ge_1bn,
    t.notional_ge_500m,

    -- anomaly score as a single metric for ontology/UI
    (
        CASE WHEN t.days_to_next_event <= 2 THEN 1 ELSE 0 END +
        CASE WHEN t.notional1 > 200000000 THEN 1 ELSE 0 END +
        COALESCE(t.jurisdiction_risk_score, 0)
    ) AS anomaly_score
FROM dbo.feature_store_trade_ml t
LEFT ANTI JOIN dbo.iq_trades q
    ON t.trade_id = q.trade_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM IQ_Ontology_lh_26019883a4f241dda9453a5f88a6f367.dbo.iq_events LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- 1) Initial creation
CREATE TABLE IF NOT EXISTS dbo.iq_events
USING DELTA
AS
SELECT
    ep.trade_id,
    e.event_id,
    e.underlying,  
    e.event_type,
    e.event_date,
    ep.days_to_next_event,
    ep.event_within_7d,
    ep.event_within_3d,
    ep.has_high_importance_event,
    ep.has_earnings_event
FROM dbo.events_raw e
LEFT JOIN dbo.feature_trade_base t
    ON t.underlying = e.underlying 
LEFT JOIN dbo.feature_event_proximity ep
    ON t.trade_id = ep.trade_id;

-- 2) Incremental append – only new (trade_id, event_id) pairs
INSERT INTO dbo.iq_events
SELECT
    ep.trade_id,
    e.event_id,
    e.underlying,  
    e.event_type,
    e.event_date,
    ep.days_to_next_event,
    ep.event_within_7d,
    ep.event_within_3d,
    ep.has_high_importance_event,
    ep.has_earnings_event
FROM dbo.events_raw e
LEFT JOIN dbo.feature_trade_base t
    ON t.underlying = e.underlying 
LEFT JOIN dbo.feature_event_proximity ep
    ON t.trade_id = ep.trade_id
LEFT ANTI JOIN dbo.iq_events q
    ON ep.trade_id = q.trade_id
   AND e.event_id = q.event_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- PARAMETERS CELL ********************

DROP VIEW IF EXISTS dbo.v_client_network;
 --CLIENT NETWORK VIEW
CREATE VIEW dbo.v_client_network AS
     SELECT DISTINCT
         -- Root client (investigated entity)
         r.from_client AS root_client,
     
         -- Direct linked client (1-hop)
         r.to_client AS connected_client,
     
         -- Relationship details
         r.relationship,         
     
         -- Trade info of connected client
         t.trade_id,
         t.underlying,
         t.trade_date,
         t.notional,
         t.kyc_risk_score,
     
         -- Event context
         e.event_date,
         e.event_type,     
         -- Event proximity
         t.days_to_next_event
            
     FROM dbo.relationships_raw r
     
     -- Join linked client trades
     JOIN iq_trades t
         ON r.to_client = t.client_id
     
     -- Join events
     left JOIN dbo.iq_events e
         ON t.underlying = e.underlying;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- 1) Initial creation
CREATE TABLE IF NOT EXISTS dbo.iq_clients
USING DELTA
AS
SELECT         
    c.client_id,
    v.connected_client,              
    v.relationship,  
    t.trade_id     
FROM dbo.clients_raw c        
LEFT OUTER JOIN dbo.v_client_network v
    ON c.client_id = v.root_client
LEFT OUTER JOIN dbo.iq_trades t
    ON t.client_id = c.client_id;

-- 2) Incremental append – only new combinations
-- Key: (client_id, connected_client, relationship, trade_id)
INSERT INTO dbo.iq_clients
SELECT         
    c.client_id,
    v.connected_client,              
    v.relationship,  
    t.trade_id     
FROM dbo.clients_raw c        
LEFT OUTER JOIN dbo.v_client_network v
    ON c.client_id = v.root_client
LEFT OUTER JOIN dbo.iq_trades t
    ON t.client_id = c.client_id
LEFT ANTI JOIN dbo.iq_clients q
    ON  c.client_id       = q.client_id
    AND v.connected_client = q.connected_client
    AND v.relationship     = q.relationship
    AND t.trade_id         = q.trade_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- 1) Initial creation
CREATE TABLE IF NOT EXISTS dbo.iq_instruments
USING DELTA
AS
SELECT DISTINCT 
    i.underlying,  
    i.isin,
    i.sector,
    i.instrument_id
FROM dbo.instruments_raw i
LEFT JOIN dbo.feature_trade_base t
    ON t.underlying = i.underlying;

-- 2) Incremental append – only new instruments
INSERT INTO dbo.iq_instruments
SELECT DISTINCT 
    i.underlying,  
    i.isin,
    i.sector,
    i.instrument_id
FROM dbo.instruments_raw i
LEFT JOIN dbo.feature_trade_base t
    ON t.underlying = i.underlying
LEFT ANTI JOIN dbo.iq_instruments q
    ON  i.instrument_id = q.instrument_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
