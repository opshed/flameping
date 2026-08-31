CREATE TABLE trace_runs (
    id INTEGER PRIMARY KEY,
    target_id INTEGER NOT NULL REFERENCES targets(id),
    endpoint_id INTEGER NOT NULL REFERENCES endpoints(id),
    method TEXT NOT NULL,
    flow_id TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER NOT NULL,
    status TEXT NOT NULL,
    reached INTEGER NOT NULL CHECK(reached IN (0, 1)),
    reached_hop INTEGER NOT NULL,
    signature TEXT NOT NULL
);

CREATE INDEX trace_runs_target_time ON trace_runs(target_id, started_at_us);

CREATE TABLE trace_probes (
    run_id INTEGER NOT NULL REFERENCES trace_runs(id) ON DELETE CASCADE,
    ttl INTEGER NOT NULL,
    probe_index INTEGER NOT NULL,
    token INTEGER NOT NULL,
    sent_at_us INTEGER NOT NULL,
    reply_at_us INTEGER,
    responder_address TEXT,
    rtt_ns INTEGER,
    icmp_type INTEGER,
    icmp_code INTEGER,
    PRIMARY KEY(run_id, ttl, probe_index)
) WITHOUT ROWID;

CREATE TABLE route_candidates (
    target_id INTEGER PRIMARY KEY REFERENCES targets(id),
    endpoint_id INTEGER NOT NULL REFERENCES endpoints(id),
    current_trace_id INTEGER REFERENCES trace_runs(id) ON DELETE SET NULL,
    candidate_trace_id INTEGER REFERENCES trace_runs(id) ON DELETE SET NULL,
    candidate_signature TEXT,
    consecutive_count INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

CREATE TABLE route_changes (
    id INTEGER PRIMARY KEY,
    target_id INTEGER NOT NULL REFERENCES targets(id),
    endpoint_id INTEGER NOT NULL REFERENCES endpoints(id),
    old_trace_id INTEGER REFERENCES trace_runs(id) ON DELETE SET NULL,
    candidate_trace_id INTEGER REFERENCES trace_runs(id) ON DELETE SET NULL,
    confirming_trace_id INTEGER REFERENCES trace_runs(id) ON DELETE SET NULL,
    old_signature TEXT NOT NULL,
    new_signature TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL,
    confirmed_at_us INTEGER NOT NULL
);

CREATE INDEX route_changes_target_time ON route_changes(target_id, confirmed_at_us);
