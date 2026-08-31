package sqlite

import (
	"context"
	"fmt"
	"time"
)

const pruneBatch = 10_000

// prune removes only data whose replacement rollup exists and is no longer
// dirty. Deletes are deliberately bounded so regular ingestion is never held
// behind a large retention transaction.
func (d *DB) prune(ctx context.Context, now time.Time) error {
	return d.pruneWithRaw(ctx, now, d.config.RawRetention.Value())
}

func (d *DB) pruneWithRaw(ctx context.Context, now time.Time, rawRetention time.Duration) error {
	rawCutoff := now.Add(-rawRetention).Truncate(time.Minute).UnixMicro()
	minuteCutoff := now.Add(-d.config.MinuteRetention.Value()).Truncate(time.Hour).UnixMicro()
	hourCutoff := now.Add(-d.config.HourRetention.Value()).Truncate(time.Hour).UnixMicro()
	traceCutoff := now.Add(-d.config.TraceRetention.Value()).UnixMicro()

	statements := []struct {
		name string
		sql  string
		arg  int64
	}{
		{"ping raw", `DELETE FROM ping_samples WHERE (run_id, sequence) IN (
			SELECT p.run_id, p.sequence FROM ping_samples p
			WHERE p.scheduled_at_us < ?1 AND p.sent_at_us < ?1
			  AND EXISTS (SELECT 1 FROM ping_rollups r WHERE r.target_id=p.target_id AND r.resolution_s=60
				AND r.bucket_start_us=p.scheduled_at_us-(p.scheduled_at_us%60000000))
			  AND NOT EXISTS (SELECT 1 FROM dirty_rollups d WHERE d.kind='ping' AND d.entity_id=p.target_id
				AND d.resolution_s=60 AND d.bucket_start_us=p.scheduled_at_us-(p.scheduled_at_us%60000000))
			ORDER BY p.scheduled_at_us,p.run_id,p.sequence LIMIT 10000)`, rawCutoff},
		{"scheduler gaps", `DELETE FROM scheduler_gaps WHERE id IN (
			SELECT g.id FROM scheduler_gaps g WHERE g.first_scheduled_at_us + ((g.missed_count-1)*(g.interval_ns/1000)) < ?
			AND NOT EXISTS (SELECT 1 FROM dirty_rollups d WHERE d.kind='ping' AND d.entity_id=g.target_id AND d.resolution_s=60
				AND d.bucket_start_us BETWEEN g.first_scheduled_at_us-(g.first_scheduled_at_us%60000000)
				AND (g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000)))-((g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000)))%60000000))
			ORDER BY g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000)),g.id LIMIT 10000)`, rawCutoff},
		{"interface raw", `DELETE FROM interface_samples WHERE (generation_id, sampled_at_us) IN (
			SELECT s.generation_id, s.sampled_at_us FROM interface_samples s WHERE s.sampled_at_us < ?
			AND EXISTS (SELECT 1 FROM interface_rollups r WHERE r.generation_id=s.generation_id AND r.resolution_s=60
				AND r.bucket_start_us=s.sampled_at_us-(s.sampled_at_us%60000000))
			AND NOT EXISTS (SELECT 1 FROM dirty_rollups d WHERE d.kind='interface' AND d.entity_id=s.generation_id AND d.resolution_s=60
				AND d.bucket_start_us=s.sampled_at_us-(s.sampled_at_us%60000000)) ORDER BY s.sampled_at_us,s.generation_id LIMIT 10000)`, rawCutoff},
		{"ping minute", `DELETE FROM ping_rollups WHERE (target_id, resolution_s, bucket_start_us) IN (
			SELECT m.target_id, m.resolution_s, m.bucket_start_us FROM ping_rollups m
			WHERE m.resolution_s=60 AND m.bucket_start_us < ?
			AND EXISTS (SELECT 1 FROM ping_rollups h WHERE h.target_id=m.target_id AND h.resolution_s=3600
				AND h.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000))
			AND NOT EXISTS (SELECT 1 FROM dirty_rollups d WHERE d.kind='ping' AND d.entity_id=m.target_id AND d.resolution_s=3600
				AND d.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000)) ORDER BY m.bucket_start_us,m.target_id LIMIT 10000)`, minuteCutoff},
		{"interface minute", `DELETE FROM interface_rollups WHERE (generation_id, resolution_s, bucket_start_us) IN (
			SELECT m.generation_id, m.resolution_s, m.bucket_start_us FROM interface_rollups m
			WHERE m.resolution_s=60 AND m.bucket_start_us < ?
			AND EXISTS (SELECT 1 FROM interface_rollups h WHERE h.generation_id=m.generation_id AND h.resolution_s=3600
				AND h.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000))
			AND NOT EXISTS (SELECT 1 FROM dirty_rollups d WHERE d.kind='interface' AND d.entity_id=m.generation_id AND d.resolution_s=3600
				AND d.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000)) ORDER BY m.bucket_start_us,m.generation_id LIMIT 10000)`, minuteCutoff},
		{"ping hour", `DELETE FROM ping_rollups WHERE (target_id, resolution_s, bucket_start_us) IN
			(SELECT target_id, resolution_s, bucket_start_us FROM ping_rollups WHERE resolution_s=3600 AND bucket_start_us<? ORDER BY bucket_start_us,target_id LIMIT 10000)`, hourCutoff},
		{"interface hour", `DELETE FROM interface_rollups WHERE (generation_id, resolution_s, bucket_start_us) IN
			(SELECT generation_id, resolution_s, bucket_start_us FROM interface_rollups WHERE resolution_s=3600 AND bucket_start_us<? ORDER BY bucket_start_us,generation_id LIMIT 10000)`, hourCutoff},
		{"trace", `DELETE FROM trace_runs WHERE id IN (SELECT tr.id FROM trace_runs tr WHERE tr.ended_at_us<?
			AND NOT EXISTS (SELECT 1 FROM route_candidates rc WHERE rc.current_trace_id=tr.id OR rc.candidate_trace_id=tr.id)
			AND NOT EXISTS (SELECT 1 FROM route_changes rc WHERE rc.old_trace_id=tr.id OR rc.candidate_trace_id=tr.id OR rc.confirming_trace_id=tr.id)
			ORDER BY tr.ended_at_us,tr.id LIMIT 10000)`, traceCutoff},
		{"route changes", `DELETE FROM route_changes WHERE id IN (
			SELECT id FROM route_changes WHERE confirmed_at_us<? ORDER BY confirmed_at_us,id LIMIT 10000)`, hourCutoff},
	}
	for _, statement := range statements {
		if _, err := d.writer.ExecContext(ctx, statement.sql, statement.arg); err != nil {
			return fmt.Errorf("prune %s: %w", statement.name, err)
		}
	}
	return d.advancePrunedThrough(ctx, rawCutoff)
}

func (d *DB) advancePrunedThrough(ctx context.Context, cutoff int64) error {
	var remains int
	if err := d.writer.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ping_samples WHERE sent_at_us<?)`, cutoff).Scan(&remains); err != nil {
		return err
	}
	if remains != 0 {
		return nil
	}
	if _, err := d.writer.ExecContext(ctx, `UPDATE retention_state SET value_us=MAX(value_us,?) WHERE name='ping_pruned_through'`, cutoff); err != nil {
		return err
	}
	if cutoff > d.prunedThrough.Load() {
		d.prunedThrough.Store(cutoff)
	}
	return nil
}

const emergencyPruneStages = 9

func (d *DB) emergencyRawCutoff(ctx context.Context, now time.Time) (int64, error) {
	rawFloor := 24 * time.Hour
	var maxTimeout int64
	if err := d.writer.QueryRowContext(ctx, `SELECT COALESCE(MAX(timeout_ns),0) FROM targets`).Scan(&maxTimeout); err != nil {
		return 0, err
	}
	if candidate := time.Duration(maxTimeout) + time.Minute; candidate > rawFloor {
		rawFloor = candidate
	}
	return now.Add(-rawFloor).Truncate(time.Minute).UnixMicro(), nil
}

func (d *DB) emergencyPruneStage(ctx context.Context, now time.Time, stage int) (int64, error) {
	rawCutoff, err := d.emergencyRawCutoff(ctx, now)
	if err != nil {
		return 0, err
	}
	month := now.Add(-30 * 24 * time.Hour).Truncate(time.Hour).UnixMicro()
	day := now.Add(-24 * time.Hour).UnixMicro()
	statements := []struct {
		name, sql string
		cutoff    int64
	}{
		{"ping raw", `DELETE FROM ping_samples WHERE (run_id,sequence) IN (SELECT p.run_id,p.sequence FROM ping_samples p
			WHERE p.scheduled_at_us<?1 AND p.sent_at_us<?1 AND EXISTS(SELECT 1 FROM ping_rollups r WHERE r.target_id=p.target_id AND r.resolution_s=60 AND r.bucket_start_us=p.scheduled_at_us-(p.scheduled_at_us%60000000))
			AND NOT EXISTS(SELECT 1 FROM dirty_rollups d WHERE d.kind='ping' AND d.entity_id=p.target_id AND d.resolution_s=60 AND d.bucket_start_us=p.scheduled_at_us-(p.scheduled_at_us%60000000)) ORDER BY p.scheduled_at_us,p.run_id,p.sequence LIMIT 10000)`, rawCutoff},
		{"scheduler gaps", `DELETE FROM scheduler_gaps WHERE id IN (SELECT g.id FROM scheduler_gaps g WHERE g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000))<?
			AND NOT EXISTS(SELECT 1 FROM dirty_rollups d WHERE d.kind='ping' AND d.entity_id=g.target_id AND d.resolution_s=60 AND d.bucket_start_us BETWEEN g.first_scheduled_at_us-(g.first_scheduled_at_us%60000000) AND (g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000)))-((g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000)))%60000000)) ORDER BY g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000)),g.id LIMIT 10000)`, rawCutoff},
		{"interface raw", `DELETE FROM interface_samples WHERE (generation_id,sampled_at_us) IN (SELECT s.generation_id,s.sampled_at_us FROM interface_samples s WHERE s.sampled_at_us<?
			AND EXISTS(SELECT 1 FROM interface_rollups r WHERE r.generation_id=s.generation_id AND r.resolution_s=60 AND r.bucket_start_us=s.sampled_at_us-(s.sampled_at_us%60000000))
			AND NOT EXISTS(SELECT 1 FROM dirty_rollups d WHERE d.kind='interface' AND d.entity_id=s.generation_id AND d.resolution_s=60 AND d.bucket_start_us=s.sampled_at_us-(s.sampled_at_us%60000000)) ORDER BY s.sampled_at_us,s.generation_id LIMIT 10000)`, rawCutoff},
		{"old traces", `DELETE FROM trace_runs WHERE id IN (SELECT tr.id FROM trace_runs tr WHERE tr.ended_at_us<?
			AND NOT EXISTS(SELECT 1 FROM route_candidates rc WHERE rc.current_trace_id=tr.id OR rc.candidate_trace_id=tr.id)
			AND NOT EXISTS(SELECT 1 FROM route_changes rc WHERE rc.old_trace_id=tr.id OR rc.candidate_trace_id=tr.id OR rc.confirming_trace_id=tr.id)
			AND EXISTS(SELECT 1 FROM trace_runs keep WHERE keep.target_id=tr.target_id AND keep.id<tr.id AND keep.started_at_us-(keep.started_at_us%86400000000)=tr.started_at_us-(tr.started_at_us%86400000000)) ORDER BY tr.ended_at_us,tr.id LIMIT 10000)`, day},
		{"ping minute pressure", `DELETE FROM ping_rollups WHERE (target_id,resolution_s,bucket_start_us) IN (SELECT m.target_id,m.resolution_s,m.bucket_start_us FROM ping_rollups m WHERE m.resolution_s=60 AND m.bucket_start_us<? AND EXISTS(SELECT 1 FROM ping_rollups h WHERE h.target_id=m.target_id AND h.resolution_s=3600 AND h.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000)) AND NOT EXISTS(SELECT 1 FROM dirty_rollups d WHERE d.kind='ping' AND d.entity_id=m.target_id AND d.resolution_s=3600 AND d.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000)) ORDER BY m.bucket_start_us,m.target_id LIMIT 10000)`, month},
		{"interface minute pressure", `DELETE FROM interface_rollups WHERE (generation_id,resolution_s,bucket_start_us) IN (SELECT m.generation_id,m.resolution_s,m.bucket_start_us FROM interface_rollups m WHERE m.resolution_s=60 AND m.bucket_start_us<? AND EXISTS(SELECT 1 FROM interface_rollups h WHERE h.generation_id=m.generation_id AND h.resolution_s=3600 AND h.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000)) AND NOT EXISTS(SELECT 1 FROM dirty_rollups d WHERE d.kind='interface' AND d.entity_id=m.generation_id AND d.resolution_s=3600 AND d.bucket_start_us=m.bucket_start_us-(m.bucket_start_us%3600000000)) ORDER BY m.bucket_start_us,m.generation_id LIMIT 10000)`, month},
		{"ping hour pressure", `DELETE FROM ping_rollups WHERE (target_id,resolution_s,bucket_start_us) IN (SELECT target_id,resolution_s,bucket_start_us FROM ping_rollups WHERE resolution_s=3600 AND bucket_start_us<? ORDER BY bucket_start_us,target_id LIMIT 10000)`, month},
		{"interface hour pressure", `DELETE FROM interface_rollups WHERE (generation_id,resolution_s,bucket_start_us) IN (SELECT generation_id,resolution_s,bucket_start_us FROM interface_rollups WHERE resolution_s=3600 AND bucket_start_us<? ORDER BY bucket_start_us,generation_id LIMIT 10000)`, month},
		{"route changes pressure", `DELETE FROM route_changes WHERE id IN (SELECT id FROM route_changes WHERE confirmed_at_us<? ORDER BY confirmed_at_us,id LIMIT 10000)`, month},
	}
	if stage < 0 || stage >= len(statements) {
		return 0, fmt.Errorf("invalid emergency prune stage %d", stage)
	}
	statement := statements[stage]
	result, err := d.writer.ExecContext(ctx, statement.sql, statement.cutoff)
	if err != nil {
		return 0, fmt.Errorf("emergency prune %s: %w", statement.name, err)
	}
	removed, err := result.RowsAffected()
	if err == nil && stage == 0 {
		err = d.advancePrunedThrough(ctx, rawCutoff)
	}
	return removed, err
}

func (d *DB) emergencyPrune(ctx context.Context, now time.Time) error {
	for stage := 0; stage < emergencyPruneStages; stage++ {
		if _, err := d.emergencyPruneStage(ctx, now, stage); err != nil {
			return err
		}
	}
	return nil
}
