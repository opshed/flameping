ALTER TABLE trace_runs ADD COLUMN error_detail TEXT NOT NULL DEFAULT '';

ALTER TABLE route_candidates ADD COLUMN candidate_reached_hop INTEGER;

ALTER TABLE route_changes ADD COLUMN old_reached_hop INTEGER NOT NULL DEFAULT 0;
ALTER TABLE route_changes ADD COLUMN new_reached_hop INTEGER NOT NULL DEFAULT 0;

UPDATE route_candidates SET candidate_reached_hop=(
    SELECT reached_hop FROM trace_runs WHERE id=candidate_trace_id
) WHERE candidate_trace_id IS NOT NULL;

UPDATE route_changes SET
    old_reached_hop=COALESCE((SELECT reached_hop FROM trace_runs WHERE id=old_trace_id),0),
    new_reached_hop=COALESCE((SELECT reached_hop FROM trace_runs WHERE id=confirming_trace_id),
                            (SELECT reached_hop FROM trace_runs WHERE id=candidate_trace_id),0);

CREATE TABLE configured_interfaces (
    name TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    last_seen_us INTEGER NOT NULL
) WITHOUT ROWID;
