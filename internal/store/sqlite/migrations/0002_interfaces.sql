CREATE TABLE interface_generations (
    id INTEGER PRIMARY KEY,
    boot_id TEXT NOT NULL,
    ifindex INTEGER NOT NULL,
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    mac TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER
);

CREATE INDEX interface_generations_name_time ON interface_generations(name, started_at_us);

CREATE TABLE interface_samples (
    generation_id INTEGER NOT NULL REFERENCES interface_generations(id),
    sampled_at_us INTEGER NOT NULL,
    rx_bytes INTEGER NOT NULL,
    tx_bytes INTEGER NOT NULL,
    rx_packets INTEGER NOT NULL,
    tx_packets INTEGER NOT NULL,
    rx_errors INTEGER NOT NULL,
    tx_errors INTEGER NOT NULL,
    rx_dropped INTEGER NOT NULL,
    tx_dropped INTEGER NOT NULL,
    rx_missed INTEGER NOT NULL,
    rx_fifo INTEGER NOT NULL,
    tx_fifo INTEGER NOT NULL,
    rx_crc INTEGER NOT NULL,
    rx_frame INTEGER NOT NULL,
    tx_carrier INTEGER NOT NULL,
    collisions INTEGER NOT NULL,
    PRIMARY KEY(generation_id, sampled_at_us)
) WITHOUT ROWID;

CREATE INDEX interface_samples_time ON interface_samples(sampled_at_us);

CREATE TABLE interface_rollups (
    generation_id INTEGER NOT NULL REFERENCES interface_generations(id),
    resolution_s INTEGER NOT NULL CHECK(resolution_s IN (60, 3600)),
    bucket_start_us INTEGER NOT NULL,
    elapsed_ns INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    reset_count INTEGER NOT NULL,
    rx_bytes_delta INTEGER NOT NULL,
    tx_bytes_delta INTEGER NOT NULL,
    rx_packets_delta INTEGER NOT NULL,
    tx_packets_delta INTEGER NOT NULL,
    rx_errors_delta INTEGER NOT NULL,
    tx_errors_delta INTEGER NOT NULL,
    rx_dropped_delta INTEGER NOT NULL,
    tx_dropped_delta INTEGER NOT NULL,
    rx_missed_delta INTEGER NOT NULL,
    rx_fifo_delta INTEGER NOT NULL,
    tx_fifo_delta INTEGER NOT NULL,
    rx_crc_delta INTEGER NOT NULL,
    rx_frame_delta INTEGER NOT NULL,
    tx_carrier_delta INTEGER NOT NULL,
    collisions_delta INTEGER NOT NULL,
    peak_rx_bytes_per_s INTEGER NOT NULL,
    peak_tx_bytes_per_s INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL,
    PRIMARY KEY(generation_id, resolution_s, bucket_start_us)
) WITHOUT ROWID;
