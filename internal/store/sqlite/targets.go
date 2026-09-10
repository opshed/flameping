package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/netip"
	"time"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
)

func (d *DB) LastEndpoint(ctx context.Context, targetID int64) (netip.Addr, error) {
	var address string
	err := d.readers.QueryRowContext(ctx, `SELECT address FROM endpoints WHERE target_id=? ORDER BY last_seen_us DESC,id DESC LIMIT 1`, targetID).Scan(&address)
	if err != nil {
		return netip.Addr{}, err
	}
	return netip.ParseAddr(address)
}

type Target struct {
	ID                int64
	StableID          string
	DisplayName       string
	ConfiguredAddress string
	Family            string
	Interval          time.Duration
	Timeout           time.Duration
	Trace             bool
}

func (d *DB) SyncTargets(ctx context.Context, cfg config.Config) ([]Target, error) {
	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().UnixMicro()
	if _, err := tx.ExecContext(ctx, `UPDATE targets SET active = 0, last_seen_us = ?`, now); err != nil {
		return nil, err
	}
	result := make([]Target, 0, len(cfg.Targets))
	for _, item := range cfg.Targets {
		name := item.Name
		if name == "" {
			name = item.ID
		}
		family := item.Family
		if family == "" {
			family = "auto"
		}
		interval, timeout := item.EffectiveInterval(cfg), item.EffectiveTimeout(cfg)
		var oldName, oldAddress, oldFamily string
		var oldInterval, oldTimeout int64
		oldErr := tx.QueryRowContext(ctx, `SELECT display_name, configured_address, address_family, interval_ns, timeout_ns FROM targets WHERE stable_id=?`, item.ID).Scan(&oldName, &oldAddress, &oldFamily, &oldInterval, &oldTimeout)
		_, err := tx.ExecContext(ctx, `INSERT INTO targets(
            stable_id, display_name, configured_address, address_family,
            interval_ns, timeout_ns, active, first_seen_us, last_seen_us
        ) VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(stable_id) DO UPDATE SET
            display_name=excluded.display_name,
            configured_address=excluded.configured_address,
            address_family=excluded.address_family,
            interval_ns=excluded.interval_ns,
            timeout_ns=excluded.timeout_ns,
            active=1,
            last_seen_us=excluded.last_seen_us`,
			item.ID, name, item.Address, family, interval.Nanoseconds(), timeout.Nanoseconds(), now, now)
		if err != nil {
			return nil, fmt.Errorf("sync target %s: %w", item.ID, err)
		}
		var id int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM targets WHERE stable_id = ?`, item.ID).Scan(&id); err != nil {
			return nil, err
		}
		if oldErr == nil && (oldName != name || oldAddress != item.Address || oldFamily != family || oldInterval != interval.Nanoseconds() || oldTimeout != timeout.Nanoseconds()) {
			detail, _ := json.Marshal(map[string]any{"old": map[string]any{"name": oldName, "address": oldAddress, "family": oldFamily, "interval_ns": oldInterval, "timeout_ns": oldTimeout}, "new": map[string]any{"name": name, "address": item.Address, "family": family, "interval_ns": interval.Nanoseconds(), "timeout_ns": timeout.Nanoseconds()}})
			if _, err := tx.ExecContext(ctx, `INSERT INTO target_events(target_id,at_us,kind,detail_json) VALUES(?,?,'config_changed',?)`, id, now, string(detail)); err != nil {
				return nil, err
			}
		} else if oldErr != nil && oldErr != sql.ErrNoRows {
			return nil, oldErr
		}
		result = append(result, Target{ID: id, StableID: item.ID, DisplayName: name, ConfiguredAddress: item.Address, Family: family, Interval: interval, Timeout: timeout, Trace: item.TraceEnabled(cfg.Traceroute.Enabled)})
	}
	if _, err := tx.ExecContext(ctx, `UPDATE configured_interfaces SET active=0,last_seen_us=?`, now); err != nil {
		return nil, err
	}
	for _, item := range cfg.Interfaces {
		display := item.DisplayName
		if display == "" {
			display = item.Name
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO configured_interfaces(name,display_name,active,last_seen_us)
			VALUES(?,?,1,?) ON CONFLICT(name) DO UPDATE SET display_name=excluded.display_name,active=1,last_seen_us=excluded.last_seen_us`, item.Name, display, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *DB) StartRun(ctx context.Context, runID model.RunID, bootID, version string) error {
	_, err := d.writer.ExecContext(ctx, `INSERT INTO probe_runs(run_id, started_at_us, boot_id, build_version) VALUES(?, ?, ?, ?)`, runID.Bytes(), time.Now().UnixMicro(), bootID, version)
	return err
}

func ensureEndpoint(ctx context.Context, tx *sql.Tx, targetID int64, address string, family int, zone string, now int64) (int64, error) {
	_, err := tx.ExecContext(ctx, `INSERT INTO endpoints(target_id, family, address, zone, first_seen_us, last_seen_us)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(target_id, family, address, zone) DO UPDATE SET last_seen_us=excluded.last_seen_us`, targetID, family, address, zone, now, now)
	if err != nil {
		return 0, err
	}
	var id int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM endpoints WHERE target_id=? AND family=? AND address=? AND zone=?`, targetID, family, address, zone).Scan(&id)
	return id, err
}
