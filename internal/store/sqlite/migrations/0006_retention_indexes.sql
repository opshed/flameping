CREATE INDEX ping_rollups_retention ON ping_rollups(resolution_s, bucket_start_us, target_id);
CREATE INDEX interface_rollups_retention ON interface_rollups(resolution_s, bucket_start_us, generation_id);
CREATE INDEX trace_runs_retention ON trace_runs(ended_at_us, id);
CREATE INDEX route_changes_retention ON route_changes(confirmed_at_us, id);
CREATE INDEX dirty_rollups_work ON dirty_rollups(kind, resolution_s, bucket_start_us, entity_id);
CREATE INDEX scheduler_gaps_retention ON scheduler_gaps(
    (first_scheduled_at_us + ((missed_count-1)*(interval_ns/1000))), id
);
CREATE INDEX scheduler_gaps_target_end ON scheduler_gaps(
    target_id, (first_scheduled_at_us + ((missed_count-1)*(interval_ns/1000))) DESC, id
);
CREATE INDEX route_changes_old_trace ON route_changes(old_trace_id);
CREATE INDEX route_changes_candidate_trace ON route_changes(candidate_trace_id);
CREATE INDEX route_changes_confirming_trace ON route_changes(confirming_trace_id);

CREATE TABLE retention_state (
    name TEXT PRIMARY KEY,
    value_us INTEGER NOT NULL
) WITHOUT ROWID;
INSERT INTO retention_state(name,value_us) VALUES('ping_pruned_through',0);
