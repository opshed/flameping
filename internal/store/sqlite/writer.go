package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"time"

	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/opshed/flameping/internal/eventbus"
	"github.com/opshed/flameping/internal/model"
)

const (
	maxBatchSize        = 512
	maxBatchDelay       = 250 * time.Millisecond
	maintenancePeriod   = 5 * time.Second
	maxOrphanReplies    = 4096
	orphanReplyLifetime = 5 * time.Second
	capacityRetryPeriod = time.Second
	capacityRecoveryMax = 5 * time.Second
)

var errCheckpointBusy = errors.New("SQLite WAL checkpoint busy")

type orphanReply struct {
	event   model.ProbeEvent
	addedAt time.Time
}

func (d *DB) RunWriter(ctx context.Context, bus *eventbus.Bus) error {
	orphans := make(map[model.ProbeKey]orphanReply)
	nextMaintenance := time.Now().Add(maintenancePeriod)
	for {
		waitUntil := nextMaintenance
		waitCtx, cancel := context.WithDeadline(ctx, waitUntil)
		first, err := bus.Next(waitCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				if err := d.maintenance(ctx); err != nil {
					return d.failWriter(err)
				}
				nextMaintenance = time.Now().Add(maintenancePeriod)
				continue
			}
			if errors.Is(err, eventbus.ErrClosed) {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return d.failWriter(err)
		}

		batch := []model.Event{first}
		batchDeadline := time.Now().Add(maxBatchDelay)
		for len(batch) < maxBatchSize {
			collectCtx, collectCancel := context.WithDeadline(ctx, batchDeadline)
			event, err := bus.Next(collectCtx)
			collectCancel()
			if err == nil {
				batch = append(batch, event)
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, eventbus.ErrClosed) || ctx.Err() != nil {
				break
			}
			return d.failWriter(err)
		}
		if err := d.applyBatchRecovering(ctx, batch, &orphans); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return d.failWriter(err)
		}
		if err := d.expireOrphans(ctx, orphans, time.Now()); err != nil {
			return d.failWriter(err)
		}
		if !time.Now().Before(nextMaintenance) {
			if err := d.maintenance(ctx); err != nil {
				return d.failWriter(err)
			}
			nextMaintenance = time.Now().Add(maintenancePeriod)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (d *DB) applyBatchRecovering(ctx context.Context, events []model.Event, orphans *map[model.ProbeKey]orphanReply) error {
	for {
		err := d.applyRetainedBatch(ctx, events, orphans)
		if err == nil || !isSQLiteFull(err) {
			return err
		}
		if err = d.recoverFullOnce(ctx, events, orphans); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isCapacityRecoveryBlocker(err) {
			return err
		}
		d.pauseForPressure(fmt.Errorf("SQLite capacity exhausted; measurement batch retained while safe checkpoint/pruning recovery waits: %w", err))
		timer := time.NewTimer(capacityRetryPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (d *DB) applyRetainedBatch(ctx context.Context, events []model.Event, orphans *map[model.ProbeKey]orphanReply) error {
	before := cloneOrphans(*orphans)
	if err := d.applyBatch(ctx, events, *orphans); err != nil {
		*orphans = before
		return err
	}
	return nil
}

func cloneOrphans(source map[model.ProbeKey]orphanReply) map[model.ProbeKey]orphanReply {
	result := make(map[model.ProbeKey]orphanReply, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (d *DB) recoverFullOnce(ctx context.Context, events []model.Event, orphans *map[model.ProbeKey]orphanReply) error {
	recoveryCtx, cancel := context.WithTimeout(ctx, capacityRecoveryMax)
	defer cancel()
	if err := d.checkpointWithRetry(recoveryCtx, "TRUNCATE", capacityRecoveryMax); err != nil {
		return err
	}
	lastErr := d.applyRetainedBatch(recoveryCtx, events, orphans)
	if lastErr == nil || !isSQLiteFull(lastErr) {
		return lastErr
	}
	for stage := 0; stage < emergencyPruneStages; stage++ {
		if _, err := d.emergencyPruneStage(recoveryCtx, time.Now(), stage); err != nil {
			return err
		}
		if err := d.checkpointWithRetry(recoveryCtx, "TRUNCATE", capacityRecoveryMax); err != nil {
			return err
		}
		lastErr = d.applyRetainedBatch(recoveryCtx, events, orphans)
		if lastErr == nil || !isSQLiteFull(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

func isSQLiteFull(err error) bool {
	var sqliteErr *modernsqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqlite3.SQLITE_FULL
}

func isCapacityRecoveryBlocker(err error) bool {
	return isSQLiteFull(err) || errors.Is(err, errCheckpointBusy) || errors.Is(err, context.DeadlineExceeded)
}

func (d *DB) failWriter(err error) error {
	wrapped := fmt.Errorf("SQLite writer: %w", err)
	d.writerFailed.Store(&writerFailure{err: wrapped})
	d.ready.Store(false)
	return wrapped
}

func (d *DB) applyBatch(ctx context.Context, events []model.Event, orphans map[model.ProbeKey]orphanReply) error {
	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, event := range events {
		switch event.Kind {
		case model.EventProbeSent, model.EventProbeSendError:
			if err := insertProbe(ctx, tx, event, orphans); err != nil {
				return err
			}
		case model.EventProbeReply:
			found, err := applyReply(ctx, tx, event.Probe)
			if err != nil {
				return err
			}
			if !found {
				if !event.Probe.SentAt.IsZero() && event.Probe.SentAt.UnixMicro() <= d.prunedThrough.Load() {
					if err := incrementDiagnostic(ctx, tx, "late_reply_evicted"); err != nil {
						return err
					}
					continue
				}
				if len(orphans) >= maxOrphanReplies {
					return fmt.Errorf("reply-before-row buffer exceeded %d entries", maxOrphanReplies)
				}
				orphans[event.Probe.Key] = orphanReply{event: event.Probe, addedAt: time.Now()}
			}
		case model.EventSchedulerGap:
			if err := insertGap(ctx, tx, event.Gap); err != nil {
				return err
			}
		case model.EventInterfaceSnapshot, model.EventInterfaceReset:
			if err := insertInterfaceEvent(ctx, tx, event); err != nil {
				return err
			}
		case model.EventTraceCompleted:
			if err := insertTrace(ctx, tx, event.Trace); err != nil {
				return err
			}
		case model.EventEndpointChanged:
			if err := insertEndpointChange(ctx, tx, event.Endpoint); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported event kind %d", event.Kind)
		}
	}
	return tx.Commit()
}

func incrementDiagnostic(ctx context.Context, tx *sql.Tx, name string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO diagnostics(name, value, updated_at_us)
		VALUES(?, 1, ?) ON CONFLICT(name) DO UPDATE SET value=value+1, updated_at_us=excluded.updated_at_us`, name, time.Now().UnixMicro())
	return err
}

func insertProbe(ctx context.Context, tx *sql.Tx, event model.Event, orphans map[model.ProbeKey]orphanReply) error {
	p := event.Probe
	if p.Key.Sequence > math.MaxInt64 {
		return fmt.Errorf("probe sequence %d exceeds SQLite signed range", p.Key.Sequence)
	}
	if !p.Endpoint.IsValid() {
		return errors.New("probe endpoint is invalid")
	}
	family := 6
	if p.Endpoint.Is4() {
		family = 4
	}
	now := p.SentAt.UnixMicro()
	endpointID, err := ensureEndpoint(ctx, tx, p.TargetID, addrText(p.Endpoint), family, p.Endpoint.Zone(), now)
	if err != nil {
		return err
	}
	var localOutcome, errorCode, errorMessage any
	if event.Kind == model.EventProbeSendError {
		localOutcome = "send_error"
		errorCode = p.SendErrorCode
		errorMessage = p.SendErrorMessage
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO ping_samples(
        run_id, sequence, target_id, endpoint_id, scheduled_at_us, sent_at_us,
        timeout_ns, local_outcome, send_error_code, send_error_message
    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, p.Key.RunID.Bytes(), int64(p.Key.Sequence), p.TargetID, endpointID,
		p.ScheduledAt.UnixMicro(), p.SentAt.UnixMicro(), p.Timeout.Nanoseconds(), localOutcome, errorCode, errorMessage)
	if err != nil {
		return err
	}
	if err := markDirty(ctx, tx, "ping", p.TargetID, 60, minuteBucket(p.ScheduledAt)); err != nil {
		return err
	}
	if orphan, ok := orphans[p.Key]; ok {
		found, err := applyReply(ctx, tx, orphan.event)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("buffered reply still has no row for %s", p.Key)
		}
		delete(orphans, p.Key)
	}
	return nil
}

func applyReply(ctx context.Context, tx *sql.Tx, p model.ProbeEvent) (bool, error) {
	responder := addrText(p.Responder)
	result, err := tx.ExecContext(ctx, `UPDATE ping_samples SET
        reply_at_us=?, rtt_ns=?, reply_class=?, responder_address=?, icmp_type=?, icmp_code=?
		WHERE run_id=? AND sequence=? AND reply_at_us IS NULL
		  AND EXISTS (SELECT 1 FROM endpoints e WHERE e.id=ping_samples.endpoint_id AND e.address=?)`,
		p.ReplyAt.UnixMicro(), p.RTT.Nanoseconds(), string(p.ReplyClass), responder, p.ICMPType, p.ICMPCode,
		p.Key.RunID.Bytes(), int64(p.Key.Sequence), responder)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed > 0 {
		var targetID, scheduledAt int64
		if err := tx.QueryRowContext(ctx, `SELECT target_id, scheduled_at_us FROM ping_samples WHERE run_id=? AND sequence=?`, p.Key.RunID.Bytes(), int64(p.Key.Sequence)).Scan(&targetID, &scheduledAt); err != nil {
			return false, err
		}
		return true, markDirty(ctx, tx, "ping", targetID, 60, bucketUnixMicro(scheduledAt, 60))
	}
	var exists int
	var expectedAddress string
	var priorReply sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT 1, e.address, ps.reply_at_us
		FROM ping_samples ps JOIN endpoints e ON e.id=ps.endpoint_id
		WHERE ps.run_id=? AND ps.sequence=?`, p.Key.RunID.Bytes(), int64(p.Key.Sequence)).Scan(&exists, &expectedAddress, &priorReply)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if expectedAddress != responder {
		_, err = tx.ExecContext(ctx, `INSERT INTO diagnostics(name, value, updated_at_us)
			VALUES('reply_source_mismatch', 1, ?) ON CONFLICT(name) DO UPDATE SET
			value=value+1, updated_at_us=excluded.updated_at_us`, time.Now().UnixMicro())
		return true, err
	}
	if !priorReply.Valid {
		return false, fmt.Errorf("reply row exists but could not be updated for %s", p.Key)
	}
	_, err = tx.ExecContext(ctx, `UPDATE ping_samples SET duplicate_count=duplicate_count+1 WHERE run_id=? AND sequence=?`, p.Key.RunID.Bytes(), int64(p.Key.Sequence))
	return true, err
}

func insertGap(ctx context.Context, tx *sql.Tx, gap model.SchedulerGap) error {
	if gap.MissedCount == 0 || gap.Interval <= 0 {
		return errors.New("invalid scheduler gap")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO scheduler_gaps(target_id, first_scheduled_at_us, interval_ns, missed_count) VALUES(?, ?, ?, ?)`,
		gap.TargetID, gap.FirstScheduled.UnixMicro(), gap.Interval.Nanoseconds(), gap.MissedCount)
	if err != nil {
		return err
	}
	last := gap.FirstScheduled.Add(time.Duration(gap.MissedCount-1) * gap.Interval)
	for bucket := minuteBucket(gap.FirstScheduled); !bucket.After(minuteBucket(last)); bucket = bucket.Add(time.Minute) {
		if err := markDirty(ctx, tx, "ping", gap.TargetID, 60, bucket); err != nil {
			return err
		}
	}
	return nil
}

func markDirty(ctx context.Context, tx *sql.Tx, kind string, entityID int64, resolution int, bucket time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO dirty_rollups(kind, entity_id, resolution_s, bucket_start_us)
        VALUES(?, ?, ?, ?) ON CONFLICT DO NOTHING`, kind, entityID, resolution, bucket.UnixMicro())
	return err
}

func minuteBucket(t time.Time) time.Time { return t.Truncate(time.Minute) }

func bucketUnixMicro(us int64, resolution int64) time.Time {
	width := resolution * int64(time.Second/time.Microsecond)
	return time.UnixMicro(us - us%width)
}

func (d *DB) expireOrphans(ctx context.Context, orphans map[model.ProbeKey]orphanReply, now time.Time) error {
	for key, orphan := range orphans {
		if now.Sub(orphan.addedAt) > orphanReplyLifetime {
			if !orphan.event.SentAt.IsZero() && orphan.event.SentAt.UnixMicro() <= d.prunedThrough.Load() {
				if _, err := d.writer.ExecContext(ctx, `INSERT INTO diagnostics(name,value,updated_at_us) VALUES('late_reply_evicted',1,?)
					ON CONFLICT(name) DO UPDATE SET value=value+1,updated_at_us=excluded.updated_at_us`, now.UnixMicro()); err != nil {
					return err
				}
				delete(orphans, key)
				continue
			}
			return fmt.Errorf("reply-before-row entry %s expired", key)
		}
	}
	return nil
}

func addrText(addr netip.Addr) string {
	if addr.Is6() {
		return addr.WithZone("").String()
	}
	return addr.String()
}

func (d *DB) maintenance(ctx context.Context) error {
	now := time.Now()
	if err := d.recomputeDirty(ctx, 1024, now); err != nil {
		return err
	}
	if err := d.prune(ctx, now); err != nil {
		return err
	}
	if err := d.checkpoint(ctx, "PASSIVE"); err != nil {
		return err
	}
	pressure, err := d.pressure(ctx, d.writer)
	if err != nil {
		return err
	}
	if d.reserveLow(pressure) {
		if err := d.checkpointWithRetry(ctx, "TRUNCATE", capacityRecoveryMax); err != nil {
			return err
		}
		pressure, err = d.pressure(ctx, d.writer)
		if err != nil {
			return err
		}
		if d.reserveLow(pressure) {
			d.pauseForPressure(fmt.Errorf("filesystem reserve pressure persists after checkpoint: %d bytes available, %d reserved", pressure.available, pressure.reserve))
			return nil
		}
	}
	if d.budgetHigh(pressure) {
		if err := d.checkpointWithRetry(ctx, "TRUNCATE", capacityRecoveryMax); err != nil {
			return err
		}
		pressure, err = d.pressure(ctx, d.writer)
		if err != nil {
			return err
		}
	}
	for stage := 0; d.budgetHigh(pressure) && stage < emergencyPruneStages; stage++ {
		removed, pruneErr := d.emergencyPruneStage(ctx, now, stage)
		if pruneErr != nil {
			return pruneErr
		}
		if err := d.checkpointWithRetry(ctx, "TRUNCATE", capacityRecoveryMax); err != nil {
			return err
		}
		pressure, err = d.pressure(ctx, d.writer)
		if err != nil {
			return err
		}
		if removed >= pruneBatch {
			return nil
		}
	}
	if d.budgetHigh(pressure) {
		d.pauseForPressure(fmt.Errorf("storage live-data pressure remains high after exhausting safe retention stages: %d live/WAL/SHM bytes of %d", pressure.live+pressure.wal+pressure.shm, d.config.MaxBytes.Int64()))
		return nil
	}
	if err := d.pressureCritical(pressure); err != nil {
		if checkpointErr := d.checkpointWithRetry(ctx, "TRUNCATE", capacityRecoveryMax); checkpointErr != nil {
			return checkpointErr
		}
		pressure, pressureErr := d.pressure(ctx, d.writer)
		if pressureErr != nil {
			return pressureErr
		}
		if err = d.pressureCritical(pressure); err != nil {
			d.pauseForPressure(err)
		}
	}
	return nil
}

func (d *DB) checkpoint(ctx context.Context, mode string) error {
	var busy, logFrames, checkpointed int
	if err := d.writer.QueryRowContext(ctx, `PRAGMA wal_checkpoint(`+mode+`)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 && mode != "PASSIVE" {
		return fmt.Errorf("%w: %s checkpoint (%d frames, %d checkpointed)", errCheckpointBusy, mode, logFrames, checkpointed)
	}
	return nil
}

func (d *DB) checkpointWithRetry(ctx context.Context, mode string, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	for {
		err := d.checkpoint(ctx, mode)
		if err == nil || !errors.Is(err, errCheckpointBusy) {
			return err
		}
		if !time.Now().Before(deadline) {
			return err
		}
		timer := time.NewTimer(min(100*time.Millisecond, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
