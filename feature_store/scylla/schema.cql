-- Online store schema. Keyspace name "fs" is replaced by tools/reload.py when
-- building a blue/green copy (e.g. fs_v2), so keep it written as "fs." below.
-- Local: RF=1. Production: NetworkTopologyStrategy with RF=3 per DC, racks = AZs.

CREATE KEYSPACE IF NOT EXISTS fs
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

-- One partition per user: day/month/year buckets as clustering rows,
-- profile (slow features) as static columns -> one single-partition read serves the API.
CREATE TABLE IF NOT EXISTS fs.user_agg (
    user_id      text,
    grain        text,        -- 'D' | 'M' | 'Y'
    bucket       text,        -- '2026-10-19' | '2026-10' | '2026' (user's local calendar)
    txn_cnt      bigint,
    txn_amt      bigint,      -- minor units (paise / cents)
    ord_cnt      bigint,
    ord_amt      bigint,
    updated_at   timestamp,
    segment      text      STATIC,
    kyc_level    text      STATIC,
    signup_date  date      STATIC,
    profile_ts   timestamp STATIC,
    PRIMARY KEY ((user_id), grain, bucket)
) WITH CLUSTERING ORDER BY (grain ASC, bucket DESC)
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 172800;

-- Serving config shared by every API worker/instance: which keyspace is live (blue/green).
-- No row = the API's FS_KEYSPACE default.
CREATE KEYSPACE IF NOT EXISTS fs_meta
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS fs_meta.serving (
    name        text PRIMARY KEY,   -- 'online'
    active_ks   text,
    updated_at  timestamp
);

-- Hourly buckets for fraud velocity features. Separate table because rows expire
-- every hour; TWCS drops whole expired SSTables instead of accumulating tombstones.
CREATE TABLE IF NOT EXISTS fs.user_agg_hourly (
    user_id     text,
    hour        timestamp,
    txn_cnt     bigint,
    txn_amt     bigint,
    ord_cnt     bigint,
    ord_amt     bigint,
    updated_at  timestamp,
    PRIMARY KEY ((user_id), hour)
) WITH CLUSTERING ORDER BY (hour DESC)
  AND default_time_to_live = 172800
  AND gc_grace_seconds = 3600
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS',
                    'compaction_window_size': 6};
