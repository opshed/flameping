ALTER TABLE ping_rollups ADD COLUMN timeout_min_ns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ping_rollups ADD COLUMN timeout_max_ns INTEGER NOT NULL DEFAULT 0;

CREATE TABLE interface_resets (
    id INTEGER PRIMARY KEY,
    generation_id INTEGER REFERENCES interface_generations(id),
    name TEXT NOT NULL,
    at_us INTEGER NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX interface_resets_name_time ON interface_resets(name, at_us);

ALTER TABLE route_candidates ADD COLUMN current_signature TEXT;
ALTER TABLE route_candidates ADD COLUMN current_reached_hop INTEGER;
ALTER TABLE route_candidates ADD COLUMN current_method TEXT;
ALTER TABLE route_candidates ADD COLUMN current_flow_id TEXT;

UPDATE route_candidates SET
    current_signature=(SELECT signature FROM trace_runs WHERE id=current_trace_id),
    current_reached_hop=(SELECT reached_hop FROM trace_runs WHERE id=current_trace_id),
    current_method=(SELECT method FROM trace_runs WHERE id=current_trace_id),
    current_flow_id=(SELECT flow_id FROM trace_runs WHERE id=current_trace_id)
WHERE current_trace_id IS NOT NULL;
