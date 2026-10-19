-- Sample queries over the online store for the demo users (make cases first).
-- Run: make cases-cql   or   docker compose exec scylla cqlsh -f /schema/queries.cql
-- Amounts are minor units (paise): 125000 = Rs.1,250.
-- Every query is a single-partition read (WHERE user_id = ...), the same access path the API uses.

-- 1. Everything stored for one user: D/M/Y buckets + profile (static columns repeat on each row)
SELECT grain, bucket, txn_cnt, txn_amt, ord_cnt, ord_amt, segment, kyc_level
FROM fs.user_agg WHERE user_id = 'demo-steady';

-- 2. Seasonality: monthly buckets, newest first. Oct-Dec last year stand out.
SELECT bucket AS month, txn_cnt, txn_amt
FROM fs.user_agg WHERE user_id = 'demo-seasonal' AND grain = 'M';

-- 3. Year-to-date and month-to-date for the seasonal shopper
SELECT grain, bucket, txn_cnt, txn_amt
FROM fs.user_agg WHERE user_id = 'demo-seasonal' AND grain IN ('Y', 'M') LIMIT 3;

-- 4. Fraud velocity: hourly buckets for the burst user (last 24 h)
SELECT hour, txn_cnt, txn_amt
FROM fs.user_agg_hourly WHERE user_id = 'demo-burst' LIMIT 24;

-- 5. Latest day bucket vs. that user's history (12 large txns today vs. small weekly spend)
SELECT grain, bucket, txn_cnt, txn_amt
FROM fs.user_agg WHERE user_id = 'demo-burst' AND grain = 'D';

-- 6. Refunds reduce the amount but not the count; cancels reduce both. Both land in the bucket of
--    the purchase they reverse, so no day goes negative.
SELECT grain, bucket, txn_cnt, txn_amt, ord_cnt, ord_amt
FROM fs.user_agg WHERE user_id = 'demo-refunds' AND grain = 'D' LIMIT 3;

-- 7. Duplicate deliveries counted once (5 txns, not 15)
SELECT grain, bucket, txn_cnt, txn_amt
FROM fs.user_agg WHERE user_id = 'demo-dupes';

-- 8. When each bucket expires (TTL, seconds) and which version won (USING TIMESTAMP, micros)
SELECT grain, bucket, txn_amt, TTL(txn_amt) AS ttl_seconds, WRITETIME(txn_amt) AS version
FROM fs.user_agg WHERE user_id = 'demo-steady' AND grain IN ('D', 'Y');

-- 9. Profile only (slow features as static columns)
SELECT DISTINCT user_id, segment, kyc_level, signup_date, profile_ts
FROM fs.user_agg WHERE user_id IN ('demo-steady', 'demo-seasonal', 'demo-burst', 'demo-refunds', 'demo-new', 'demo-dupes');

-- 10. A user with no events has no partition at all (the API serves zeros)
SELECT * FROM fs.user_agg WHERE user_id = 'demo-unknown';

-- 11. Which keyspace the API is serving from (blue/green switch)
SELECT * FROM fs_meta.serving;
