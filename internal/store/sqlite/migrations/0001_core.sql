CREATE TABLE probe_runs (
    run_id BLOB PRIMARY KEY CHECK(length(run_id) = 8),
    started_at_us INTEGER NOT NULL,
    boot_id TEXT NOT NULL DEFAULT '',
    build_version TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE targets (
    id INTEGER PRIMARY KEY,
    stable_id TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    configured_address TEXT NOT NULL,
    address_family TEXT NOT NULL CHECK(address_family IN ('auto', '4', '6')),
    interval_ns INTEGER NOT NULL CHECK(interval_ns > 0),
    timeout_ns INTEGER NOT NULL CHECK(timeout_ns > 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL
);

CREATE TABLE endpoints (
    id INTEGER PRIMARY KEY,
    target_id INTEGER NOT NULL REFERENCES targets(id),
    family INTEGER NOT NULL CHECK(family IN (4, 6)),
    address TEXT NOT NULL,
    zone TEXT NOT NULL DEFAULT '',
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    UNIQUE(target_id, family, address, zone)
);

CREATE TABLE target_events (
    id INTEGER PRIMARY KEY,
    target_id INTEGER NOT NULL REFERENCES targets(id),
    at_us INTEGER NOT NULL,
    kind TEXT NOT NULL,
    old_endpoint_id INTEGER REFERENCES endpoints(id),
    new_endpoint_id INTEGER REFERENCES endpoints(id),
    detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX target_events_target_time ON target_events(target_id, at_us);

CREATE TABLE ping_samples (
    run_id BLOB NOT NULL CHECK(length(run_id) = 8),
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    target_id INTEGER NOT NULL REFERENCES targets(id),
    endpoint_id INTEGER NOT NULL REFERENCES endpoints(id),
    scheduled_at_us INTEGER NOT NULL,
    sent_at_us INTEGER NOT NULL,
    timeout_ns INTEGER NOT NULL CHECK(timeout_ns > 0),
    local_outcome TEXT CHECK(local_outcome IS NULL OR local_outcome = 'send_error'),
    send_error_code TEXT,
    send_error_message TEXT,
    reply_at_us INTEGER,
    rtt_ns INTEGER CHECK(rtt_ns IS NULL OR rtt_ns >= 0),
    reply_class TEXT CHECK(reply_class IS NULL OR reply_class IN ('on_time', 'late')),
    responder_address TEXT,
    icmp_type INTEGER,
    icmp_code INTEGER,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(run_id, sequence)
) WITHOUT ROWID;

CREATE INDEX ping_samples_target_time ON ping_samples(target_id, scheduled_at_us);
CREATE INDEX ping_samples_time ON ping_samples(scheduled_at_us);

CREATE TABLE scheduler_gaps (
    id INTEGER PRIMARY KEY,
    target_id INTEGER NOT NULL REFERENCES targets(id),
    first_scheduled_at_us INTEGER NOT NULL,
    interval_ns INTEGER NOT NULL CHECK(interval_ns > 0),
    missed_count INTEGER NOT NULL CHECK(missed_count > 0)
);

CREATE INDEX scheduler_gaps_target_time ON scheduler_gaps(target_id, first_scheduled_at_us);

CREATE TABLE ping_rollups (
    target_id INTEGER NOT NULL REFERENCES targets(id),
    resolution_s INTEGER NOT NULL CHECK(resolution_s IN (60, 3600)),
    bucket_start_us INTEGER NOT NULL,
    scheduled_count INTEGER NOT NULL,
    attempted_count INTEGER NOT NULL,
    sent_count INTEGER NOT NULL,
    on_time_count INTEGER NOT NULL,
    late_count INTEGER NOT NULL,
    unanswered_count INTEGER NOT NULL,
    send_error_count INTEGER NOT NULL,
    scheduler_missed_count INTEGER NOT NULL,
    rtt_count INTEGER NOT NULL,
    rtt_sum_ns INTEGER NOT NULL,
    rtt_min_ns INTEGER,
    rtt_max_ns INTEGER,
    histogram BLOB NOT NULL,
    updated_at_us INTEGER NOT NULL,
    PRIMARY KEY(target_id, resolution_s, bucket_start_us)
) WITHOUT ROWID;

CREATE TABLE dirty_rollups (
    kind TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    resolution_s INTEGER NOT NULL,
    bucket_start_us INTEGER NOT NULL,
    PRIMARY KEY(kind, entity_id, resolution_s, bucket_start_us)
) WITHOUT ROWID;

CREATE TABLE diagnostics (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
) WITHOUT ROWID;
