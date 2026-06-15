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

-- ---------- 1) Base enriched trade feature table ----------
--Initial creation (one-time)
CREATE TABLE IF NOT EXISTS dbo.feature_trade_base
USING DELTA
AS
SELECT
    t.trade_id,
    CAST(t.trade_date AS date)               AS trade_date,
    CAST(t.booking_date AS date)             AS booking_date,
    t.trade_status,
    t.product_type,
    t.underlying,
    t.isin,
    t.currency,
    CAST(t.notional AS decimal(20,2))        AS notional,
    t.notional       AS notional1,
    CAST(t.quantity AS decimal(20,4))        AS quantity,
    CAST(t.price AS decimal(20,4))           AS price,
    t.direction,
    t.client_id,
    t.client_name,
    t.trader_id,
    t.desk,
    t.legal_entity,
    t.portfolio,
    CAST(t.dividend_rate AS decimal(18,6))   AS dividend_rate,
    CAST(t.financing_spread AS decimal(18,6))AS financing_spread,
    CAST(t.maturity_date AS date)            AS maturity_date,
    t.risk_flag,
    t.jurisdiction_risk,
    c.country,
    c.risk_rating,
    i.instrument_id,
    i.sector,   
    CASE WHEN UPPER(t.risk_flag) = 'INSIDER' THEN 1 ELSE 0 END AS label_insider
FROM dbo.trades_raw t
LEFT JOIN dbo.clients_raw c
    ON t.client_id = c.client_id
LEFT JOIN dbo.instruments_raw i
    ON t.underlying = i.underlying
   AND t.isin = i.isin;

-- 2. Incremental append: only pick trades not yet in feature_trade_base
INSERT INTO dbo.feature_trade_base  
SELECT
    t.trade_id,
    CAST(t.trade_date AS date)               AS trade_date,
    CAST(t.booking_date AS date)             AS booking_date,
    t.trade_status,
    t.product_type,
    t.underlying,
    t.isin,
    t.currency,
    CAST(t.notional AS decimal(20,2))        AS notional,
    t.notional       AS notional1,
    CAST(t.quantity AS decimal(20,4))        AS quantity,
    CAST(t.price AS decimal(20,4))           AS price,
    t.direction,
    t.client_id,
    t.client_name,
    t.trader_id,
    t.desk,
    t.legal_entity,
    t.portfolio,
    CAST(t.dividend_rate AS decimal(18,6))   AS dividend_rate,
    CAST(t.financing_spread AS decimal(18,6))AS financing_spread,
    CAST(t.maturity_date AS date)            AS maturity_date,
    t.risk_flag,
    t.jurisdiction_risk,
    c.country,
    c.risk_rating,
    i.instrument_id,
    i.sector,   
    CASE WHEN UPPER(t.risk_flag) = 'INSIDER' THEN 1 ELSE 0 END AS label_insider
FROM dbo.trades_raw t
LEFT JOIN dbo.clients_raw c
    ON t.client_id = c.client_id
LEFT JOIN dbo.instruments_raw i
    ON t.underlying = i.underlying
   AND t.isin = i.isin
LEFT ANTI JOIN dbo.feature_trade_base f
    ON t.trade_id = f.trade_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ---------- 2) One-hop graph metrics ----------
CREATE OR REPLACE TABLE dbo.feature_client_hop1
USING DELTA
AS
WITH out_deg AS (
    SELECT from_client AS client_id, COUNT(*) AS out_degree
    FROM dbo.relationships_raw
    GROUP BY from_client
),
in_deg AS (
    SELECT to_client AS client_id, COUNT(*) AS in_degree
    FROM dbo.relationships_raw
    GROUP BY to_client
),
neighbors AS (
    SELECT from_client AS client_id, COUNT(DISTINCT to_client) AS distinct_neighbors_1hop
    FROM dbo.relationships_raw
    GROUP BY from_client
)
SELECT
    c.client_id,
    COALESCE(o.out_degree, 0) AS out_degree,
    COALESCE(i.in_degree, 0) AS in_degree,
    COALESCE(n.distinct_neighbors_1hop, 0) AS distinct_neighbors_1hop,
    CASE WHEN COALESCE(n.distinct_neighbors_1hop, 0) >= 1 THEN 1 ELSE 0 END AS has_1hop_link
FROM dbo.clients_raw c
LEFT JOIN out_deg o ON c.client_id = o.client_id
LEFT JOIN in_deg  i ON c.client_id = i.client_id
LEFT JOIN neighbors n ON c.client_id = n.client_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ---------- 3) Three-hop loop / cluster metrics ----------
CREATE OR REPLACE TABLE dbo.feature_client_hop3
USING DELTA
AS
WITH hop3 AS (
    SELECT
        a.from_client AS root_client,
        a.to_client   AS hop1_client,
        b.to_client   AS hop2_client,
        c.to_client   AS hop3_client,
        CASE WHEN c.to_client = a.from_client THEN 1 ELSE 0 END AS is_three_hop_loop
    FROM dbo.relationships_raw a
    INNER JOIN dbo.relationships_raw b
        ON a.to_client = b.from_client
    INNER JOIN dbo.relationships_raw c
        ON b.to_client = c.from_client
),
agg AS (
    SELECT
        root_client AS client_id,
        COUNT(*) AS hop3_path_count,
        SUM(is_three_hop_loop) AS three_hop_loop_count,
        MAX(is_three_hop_loop) AS in_three_hop_loop
    FROM hop3
    GROUP BY root_client
)
SELECT
    c.client_id,
    COALESCE(a.hop3_path_count, 0) AS hop3_path_count,
    COALESCE(a.three_hop_loop_count, 0) AS three_hop_loop_count,
    COALESCE(a.in_three_hop_loop, 0) AS in_three_hop_loop
FROM dbo.clients_raw c
LEFT JOIN agg a
    ON c.client_id = a.client_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ---------- 4) Event proximity features ----------

CREATE OR REPLACE TABLE  dbo.feature_event_proximity
USING DELTA
AS
WITH event_match AS (
    SELECT
        t.trade_id,
        MIN(DATEDIFF(DAY, CAST(t.trade_date AS date), CAST(e.event_date AS date))) AS days_to_next_event,
        MIN(CASE WHEN DATEDIFF(DAY, CAST(t.trade_date AS date), CAST(e.event_date AS date)) BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS tmp_flag,
        MAX(CASE WHEN DATEDIFF(DAY, CAST(t.trade_date AS date), CAST(e.event_date AS date)) BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS event_within_7d,
        MAX(CASE WHEN DATEDIFF(DAY, CAST(t.trade_date AS date), CAST(e.event_date AS date)) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) AS event_within_3d,
        MAX(CASE WHEN UPPER(e.importance) = 'HIGH' THEN 1 ELSE 0 END) AS has_high_importance_event,
        MAX(CASE WHEN UPPER(e.event_type) LIKE '%EARNING%' THEN 1 ELSE 0 END) AS has_earnings_event
    FROM dbo.trades_raw t
    LEFT JOIN dbo.events_raw e
        ON t.underlying = e.underlying
       AND DATEDIFF(DAY, CAST(t.trade_date AS date), CAST(e.event_date AS date)) BETWEEN 0 AND 30
    GROUP BY t.trade_id
)
SELECT
    trade_id,
    COALESCE(days_to_next_event, 9999) AS days_to_next_event,
    COALESCE(event_within_7d, 0) AS event_within_7d,
    COALESCE(event_within_3d, 0) AS event_within_3d,
    COALESCE(has_high_importance_event, 0) AS has_high_importance_event,
    COALESCE(has_earnings_event, 0) AS has_earnings_event
FROM event_match;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ---------- 5) Final ML feature store table (incremental) ----------

-- 1. Initial creation (runs only if the table does NOT exist)
CREATE TABLE IF NOT EXISTS dbo.feature_store_trade_ml
USING DELTA
AS
SELECT
    b.trade_id,
    b.trade_date,
    b.booking_date,
    b.trade_status,
    b.product_type,
    b.underlying,
    b.isin,
    b.currency,
    b.notional,
    b.notional1,
    b.quantity,
    b.price,
    b.direction,
    b.client_id,
    b.client_name,
    b.trader_id,
    b.desk,
    b.legal_entity,
    b.portfolio,
    b.dividend_rate,
    b.financing_spread,
    b.maturity_date,
    b.risk_flag,
    b.jurisdiction_risk,
    b.country,
    b.risk_rating,
    b.instrument_id,
    b.sector,
    h1.out_degree,
    h1.in_degree,
    h1.distinct_neighbors_1hop,
    h1.has_1hop_link,
    h3.hop3_path_count,
    h3.three_hop_loop_count,
    h3.in_three_hop_loop,
    ep.days_to_next_event,
    ep.event_within_7d,
    ep.event_within_3d,
    ep.has_high_importance_event,
    ep.has_earnings_event,
    CASE WHEN UPPER(b.jurisdiction_risk) = 'HIGH' THEN 3
         WHEN UPPER(b.jurisdiction_risk) = 'MEDIUM' THEN 2
         ELSE 1 END AS jurisdiction_risk_score,
    CASE WHEN UPPER(b.risk_rating) = 'HIGH' THEN 3
         WHEN UPPER(b.risk_rating) = 'MEDIUM' THEN 2
         ELSE 1 END AS kyc_risk_score,
    CASE WHEN b.notional >= 1000000000 THEN 1 ELSE 0 END AS notional_ge_1bn,
    CASE WHEN b.notional >= 500000000 THEN 1 ELSE 0 END AS notional_ge_500m,
    b.label_insider
FROM dbo.feature_trade_base b
LEFT JOIN dbo.feature_client_hop1 h1
    ON b.client_id = h1.client_id
LEFT JOIN dbo.feature_client_hop3 h3
    ON b.client_id = h3.client_id
LEFT JOIN dbo.feature_event_proximity ep
    ON b.trade_id = ep.trade_id;

-- 2. Incremental append: only pick trades not yet in feature_store_trade_ml
INSERT INTO dbo.feature_store_trade_ml
SELECT
    b.trade_id,
    b.trade_date,
    b.booking_date,
    b.trade_status,
    b.product_type,
    b.underlying,
    b.isin,
    b.currency,
    b.notional,
    b.notional1,
    b.quantity,
    b.price,
    b.direction,
    b.client_id,
    b.client_name,
    b.trader_id,
    b.desk,
    b.legal_entity,
    b.portfolio,
    b.dividend_rate,
    b.financing_spread,
    b.maturity_date,
    b.risk_flag,
    b.jurisdiction_risk,
    b.country,
    b.risk_rating,
    b.instrument_id,
    b.sector,
    h1.out_degree,
    h1.in_degree,
    h1.distinct_neighbors_1hop,
    h1.has_1hop_link,
    h3.hop3_path_count,
    h3.three_hop_loop_count,
    h3.in_three_hop_loop,
    ep.days_to_next_event,
    ep.event_within_7d,
    ep.event_within_3d,
    ep.has_high_importance_event,
    ep.has_earnings_event,
    CASE WHEN UPPER(b.jurisdiction_risk) = 'HIGH' THEN 3
         WHEN UPPER(b.jurisdiction_risk) = 'MEDIUM' THEN 2
         ELSE 1 END AS jurisdiction_risk_score,
    CASE WHEN UPPER(b.risk_rating) = 'HIGH' THEN 3
         WHEN UPPER(b.risk_rating) = 'MEDIUM' THEN 2
         ELSE 1 END AS kyc_risk_score,
    CASE WHEN b.notional >= 1000000000 THEN 1 ELSE 0 END AS notional_ge_1bn,
    CASE WHEN b.notional >= 500000000 THEN 1 ELSE 0 END AS notional_ge_500m,
    b.label_insider
FROM dbo.feature_trade_base b
LEFT JOIN dbo.feature_client_hop1 h1
    ON b.client_id = h1.client_id
LEFT JOIN dbo.feature_client_hop3 h3
    ON b.client_id = h3.client_id
LEFT JOIN dbo.feature_event_proximity ep
    ON b.trade_id = ep.trade_id
LEFT ANTI JOIN dbo.feature_store_trade_ml f
    ON b.trade_id = f.trade_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ---------- 6) Optional investigator-friendly view ----------

DROP VIEW IF EXISTS dbo.v_trade_surveillance_investigation;

CREATE VIEW dbo.v_trade_surveillance_investigation
AS
SELECT
    trade_id,
    trade_date,
    client_id,
    client_name,
    underlying,
    notional,
    notional1,
    jurisdiction_risk,
    risk_rating,
    days_to_next_event,
    has_1hop_link,
    in_three_hop_loop,
    event_within_7d,
    label_insider,
    0 AS label_other,
    (
        COALESCE(notional_ge_1bn,0) * 25 +
        COALESCE(event_within_7d,0) * 20 +
        COALESCE(has_1hop_link,0) * 15 +
        COALESCE(in_three_hop_loop,0) * 20 +
        COALESCE(jurisdiction_risk_score,0) * 5 +
        COALESCE(kyc_risk_score,0) * 5
    ) AS heuristic_surveillance_score
FROM dbo.feature_store_trade_ml;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT  * FROM dbo.v_trade_surveillance_investigation;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
