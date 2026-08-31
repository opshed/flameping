package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"flameping/internal/histogram"
)

type dirtyKey struct {
	kind       string
	entityID   int64
	resolution int
	bucketUS   int64
}

type pingAggregate struct {
	scheduled, attempted, sent, onTime, late, unanswered, sendError, schedulerMissed int64
	rttCount, rttSum, rttMin, rttMax                                                 int64
	timeoutMin, timeoutMax                                                           int64
	hist                                                                             *histogram.Histogram
}

func (d *DB) recomputeDirty(ctx context.Context, limit int, asOf time.Time) error {
	if limit < 4 {
		limit = 4
	}
	quota := (limit + 3) / 4
	queries := []struct {
		kind       string
		resolution int
		eligible   string
	}{
		{"ping", 60, `d.bucket_start_us+60000000<=? AND NOT EXISTS(SELECT 1 FROM ping_samples p
			WHERE p.target_id=d.entity_id AND p.scheduled_at_us>=d.bucket_start_us AND p.scheduled_at_us<d.bucket_start_us+60000000
			AND p.local_outcome IS NULL AND p.reply_at_us IS NULL AND p.sent_at_us+(p.timeout_ns/1000)>?)`},
		{"ping", 3600, `d.bucket_start_us+3600000000<=? AND NOT EXISTS(SELECT 1 FROM dirty_rollups m WHERE
			m.kind='ping' AND m.entity_id=d.entity_id AND m.resolution_s=60 AND m.bucket_start_us>=d.bucket_start_us AND m.bucket_start_us<d.bucket_start_us+3600000000)`},
		{"interface", 60, `d.bucket_start_us+60000000<=? AND (EXISTS(SELECT 1 FROM interface_samples s
			WHERE s.generation_id=d.entity_id AND s.sampled_at_us>=d.bucket_start_us+60000000) OR EXISTS(SELECT 1 FROM interface_generations g
			WHERE g.id=d.entity_id AND g.ended_at_us IS NOT NULL))`},
		{"interface", 3600, `d.bucket_start_us+3600000000<=? AND NOT EXISTS(SELECT 1 FROM dirty_rollups m WHERE
			m.kind='interface' AND m.entity_id=d.entity_id AND m.resolution_s=60 AND m.bucket_start_us>=d.bucket_start_us AND m.bucket_start_us<d.bucket_start_us+3600000000)`},
	}
	keys := make([]dirtyKey, 0, limit)
	for _, query := range queries {
		statement := `SELECT d.kind,d.entity_id,d.resolution_s,d.bucket_start_us FROM dirty_rollups d WHERE d.kind=? AND d.resolution_s=? AND ` + query.eligible + ` ORDER BY d.bucket_start_us LIMIT ?`
		args := []any{query.kind, query.resolution, asOf.UnixMicro()}
		if query.kind == "ping" && query.resolution == 60 {
			args = append(args, asOf.UnixMicro())
		}
		args = append(args, quota)
		rows, err := d.writer.QueryContext(ctx, statement, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var key dirtyKey
			if err := rows.Scan(&key.kind, &key.entityID, &key.resolution, &key.bucketUS); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	for _, key := range keys {
		var err error
		switch key.kind {
		case "ping":
			switch key.resolution {
			case 60:
				err = d.recomputePingMinute(ctx, key, asOf)
			case 3600:
				err = d.recomputePingHour(ctx, key, asOf)
			default:
				err = fmt.Errorf("unknown ping rollup resolution %d", key.resolution)
			}
		case "interface":
			switch key.resolution {
			case 60:
				err = d.recomputeInterfaceMinute(ctx, key, asOf)
			case 3600:
				err = d.recomputeInterfaceHour(ctx, key, asOf)
			default:
				err = fmt.Errorf("unknown interface rollup resolution %d", key.resolution)
			}
		default:
			err = fmt.Errorf("unknown dirty rollup kind %q", key.kind)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type interfaceRow struct {
	at       int64
	counters [15]int64
}

type interfaceAggregate struct {
	elapsed, samples, resets int64
	delta                    [15]int64
	peakRXBytes, peakTXBytes int64
}

func (d *DB) recomputeInterfaceMinute(ctx context.Context, key dirtyKey, asOf time.Time) error {
	start := time.UnixMicro(key.bucketUS)
	end := start.Add(time.Minute)
	if asOf.Before(end) {
		return nil
	}
	var after int
	if err := d.writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM interface_samples
		WHERE generation_id=? AND sampled_at_us>=?`, key.entityID, end.UnixMicro()).Scan(&after); err != nil {
		return err
	}
	if after == 0 {
		var ended sql.NullInt64
		if err := d.writer.QueryRowContext(ctx, `SELECT ended_at_us FROM interface_generations WHERE id=?`, key.entityID).Scan(&ended); err != nil {
			return err
		}
		if !ended.Valid {
			return nil
		}
	}
	rows, err := d.loadInterfaceRows(ctx, key.entityID, start, end)
	if err != nil {
		return err
	}
	agg, err := aggregateInterfaceRows(rows)
	if err != nil {
		return err
	}
	if err := d.writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM interface_resets
		WHERE generation_id=? AND at_us>=? AND at_us<?`, key.entityID, start.UnixMicro(), end.UnixMicro()).Scan(&agg.resets); err != nil {
		return err
	}
	return d.storeInterfaceAggregate(ctx, key, agg, asOf, true)
}

func (d *DB) loadInterfaceRows(ctx context.Context, generationID int64, start, end time.Time) ([]interfaceRow, error) {
	var baseline int64
	err := d.writer.QueryRowContext(ctx, `SELECT sampled_at_us FROM interface_samples
		WHERE generation_id=? AND sampled_at_us<? ORDER BY sampled_at_us DESC LIMIT 1`, generationID, start.UnixMicro()).Scan(&baseline)
	if err == sql.ErrNoRows {
		baseline = start.UnixMicro()
	} else if err != nil {
		return nil, err
	}
	rows, err := d.writer.QueryContext(ctx, `SELECT sampled_at_us,
        rx_bytes, tx_bytes, rx_packets, tx_packets, rx_errors, tx_errors,
        rx_dropped, tx_dropped, rx_missed, rx_fifo, tx_fifo, rx_crc, rx_frame,
        tx_carrier, collisions
		FROM interface_samples WHERE generation_id=? AND sampled_at_us>=? AND sampled_at_us<?
        ORDER BY sampled_at_us`, generationID, baseline, end.UnixMicro())
	if err != nil {
		return nil, err
	}
	var result []interfaceRow
	for rows.Next() {
		var row interfaceRow
		args := []any{&row.at}
		for i := range row.counters {
			args = append(args, &row.counters[i])
		}
		if err := rows.Scan(args...); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Close()
}

func aggregateInterfaceRows(rows []interfaceRow) (interfaceAggregate, error) {
	var agg interfaceAggregate
	if len(rows) < 2 {
		agg.samples = int64(len(rows))
		return agg, nil
	}
	agg.samples = int64(len(rows))
	for i := 1; i < len(rows); i++ {
		elapsedUS := rows[i].at - rows[i-1].at
		if elapsedUS <= 0 {
			return agg, fmt.Errorf("interface samples are not strictly ordered")
		}
		agg.elapsed += elapsedUS * 1000
		for j := range agg.delta {
			if rows[i].counters[j] < rows[i-1].counters[j] {
				return agg, fmt.Errorf("interface counter decreased within one generation")
			}
			agg.delta[j] += rows[i].counters[j] - rows[i-1].counters[j]
		}
		rxRate := interfaceRate(rows[i].counters[0]-rows[i-1].counters[0], elapsedUS)
		txRate := interfaceRate(rows[i].counters[1]-rows[i-1].counters[1], elapsedUS)
		if rxRate > agg.peakRXBytes {
			agg.peakRXBytes = rxRate
		}
		if txRate > agg.peakTXBytes {
			agg.peakTXBytes = txRate
		}
	}
	return agg, nil
}

func interfaceRate(delta, elapsedUS int64) int64 {
	if delta <= 0 || elapsedUS <= 0 {
		return 0
	}
	rate := float64(delta) * 1_000_000 / float64(elapsedUS)
	if rate >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(rate)
}

func (d *DB) recomputeInterfaceHour(ctx context.Context, key dirtyKey, asOf time.Time) error {
	start := time.UnixMicro(key.bucketUS)
	end := start.Add(time.Hour)
	if asOf.Before(end) {
		return nil
	}
	var dirtyMinutes int
	if err := d.writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM dirty_rollups
        WHERE kind='interface' AND entity_id=? AND resolution_s=60 AND bucket_start_us>=? AND bucket_start_us<?`, key.entityID, start.UnixMicro(), end.UnixMicro()).Scan(&dirtyMinutes); err != nil {
		return err
	}
	if dirtyMinutes > 0 {
		return nil
	}
	rows, err := d.writer.QueryContext(ctx, `SELECT elapsed_ns, sample_count, reset_count,
        rx_bytes_delta, tx_bytes_delta, rx_packets_delta, tx_packets_delta,
        rx_errors_delta, tx_errors_delta, rx_dropped_delta, tx_dropped_delta,
        rx_missed_delta, rx_fifo_delta, tx_fifo_delta, rx_crc_delta, rx_frame_delta,
        tx_carrier_delta, collisions_delta, peak_rx_bytes_per_s, peak_tx_bytes_per_s
        FROM interface_rollups WHERE generation_id=? AND resolution_s=60
          AND bucket_start_us>=? AND bucket_start_us<?`, key.entityID, start.UnixMicro(), end.UnixMicro())
	if err != nil {
		return err
	}
	var agg interfaceAggregate
	for rows.Next() {
		var delta [15]int64
		var peakRX, peakTX int64
		args := []any{&agg.elapsed, &agg.samples, &agg.resets}
		for i := range delta {
			args = append(args, &delta[i])
		}
		args = append(args, &peakRX, &peakTX)
		var elapsed, samples, resets int64
		args[0], args[1], args[2] = &elapsed, &samples, &resets
		if err := rows.Scan(args...); err != nil {
			rows.Close()
			return err
		}
		agg.elapsed += elapsed
		agg.samples += samples
		agg.resets += resets
		for i := range delta {
			agg.delta[i] += delta[i]
		}
		if peakRX > agg.peakRXBytes {
			agg.peakRXBytes = peakRX
		}
		if peakTX > agg.peakTXBytes {
			agg.peakTXBytes = peakTX
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return d.storeInterfaceAggregate(ctx, key, agg, asOf, false)
}

func (d *DB) storeInterfaceAggregate(ctx context.Context, key dirtyKey, agg interfaceAggregate, asOf time.Time, dirtyHour bool) error {
	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	args := []any{key.entityID, key.resolution, key.bucketUS, agg.elapsed, agg.samples, agg.resets}
	for _, value := range agg.delta {
		args = append(args, value)
	}
	args = append(args, agg.peakRXBytes, agg.peakTXBytes, asOf.UnixMicro())
	_, err = tx.ExecContext(ctx, `INSERT INTO interface_rollups(
        generation_id, resolution_s, bucket_start_us, elapsed_ns, sample_count, reset_count,
        rx_bytes_delta, tx_bytes_delta, rx_packets_delta, tx_packets_delta,
        rx_errors_delta, tx_errors_delta, rx_dropped_delta, tx_dropped_delta,
        rx_missed_delta, rx_fifo_delta, tx_fifo_delta, rx_crc_delta, rx_frame_delta,
        tx_carrier_delta, collisions_delta, peak_rx_bytes_per_s, peak_tx_bytes_per_s, updated_at_us
    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(generation_id, resolution_s, bucket_start_us) DO UPDATE SET
        elapsed_ns=excluded.elapsed_ns, sample_count=excluded.sample_count, reset_count=excluded.reset_count,
        rx_bytes_delta=excluded.rx_bytes_delta, tx_bytes_delta=excluded.tx_bytes_delta,
        rx_packets_delta=excluded.rx_packets_delta, tx_packets_delta=excluded.tx_packets_delta,
        rx_errors_delta=excluded.rx_errors_delta, tx_errors_delta=excluded.tx_errors_delta,
        rx_dropped_delta=excluded.rx_dropped_delta, tx_dropped_delta=excluded.tx_dropped_delta,
        rx_missed_delta=excluded.rx_missed_delta, rx_fifo_delta=excluded.rx_fifo_delta,
        tx_fifo_delta=excluded.tx_fifo_delta, rx_crc_delta=excluded.rx_crc_delta,
        rx_frame_delta=excluded.rx_frame_delta, tx_carrier_delta=excluded.tx_carrier_delta,
        collisions_delta=excluded.collisions_delta, peak_rx_bytes_per_s=excluded.peak_rx_bytes_per_s,
        peak_tx_bytes_per_s=excluded.peak_tx_bytes_per_s, updated_at_us=excluded.updated_at_us`, args...)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dirty_rollups WHERE kind=? AND entity_id=? AND resolution_s=? AND bucket_start_us=?`, key.kind, key.entityID, key.resolution, key.bucketUS); err != nil {
		return err
	}
	if dirtyHour {
		if err := markDirty(ctx, tx, "interface", key.entityID, 3600, time.UnixMicro(key.bucketUS).Truncate(time.Hour)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) recomputePingMinute(ctx context.Context, key dirtyKey, asOf time.Time) error {
	start := time.UnixMicro(key.bucketUS)
	end := start.Add(time.Minute)
	if asOf.Before(end) {
		return nil
	}
	var pending int64
	if err := d.writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM ping_samples
        WHERE target_id=? AND scheduled_at_us>=? AND scheduled_at_us<?
          AND local_outcome IS NULL AND reply_at_us IS NULL
          AND sent_at_us + (timeout_ns / 1000) > ?`, key.entityID, start.UnixMicro(), end.UnixMicro(), asOf.UnixMicro()).Scan(&pending); err != nil {
		return err
	}
	if pending > 0 {
		return nil
	}
	agg, err := d.aggregateRawMinute(ctx, key.entityID, start, end)
	if err != nil {
		return err
	}
	return d.storePingAggregate(ctx, key, agg, asOf, true)
}

func (d *DB) aggregateRawMinute(ctx context.Context, targetID int64, start, end time.Time) (pingAggregate, error) {
	agg := pingAggregate{hist: histogram.New(), rttMin: math.MaxInt64}
	rows, err := d.writer.QueryContext(ctx, `SELECT timeout_ns, local_outcome, reply_at_us, rtt_ns, reply_class
        FROM ping_samples WHERE target_id=? AND scheduled_at_us>=? AND scheduled_at_us<?`, targetID, start.UnixMicro(), end.UnixMicro())
	if err != nil {
		return agg, err
	}
	for rows.Next() {
		var timeout int64
		var local, replyClass sql.NullString
		var replyAt, rtt sql.NullInt64
		if err := rows.Scan(&timeout, &local, &replyAt, &rtt, &replyClass); err != nil {
			rows.Close()
			return agg, err
		}
		agg.attempted++
		observeTimeout(&agg, timeout)
		if local.Valid {
			agg.sendError++
			continue
		}
		agg.sent++
		if !replyAt.Valid {
			agg.unanswered++
			continue
		}
		if replyClass.String == "late" {
			agg.late++
		} else {
			agg.onTime++
		}
		if rtt.Valid {
			agg.rttCount++
			agg.rttSum += rtt.Int64
			if rtt.Int64 < agg.rttMin {
				agg.rttMin = rtt.Int64
			}
			if rtt.Int64 > agg.rttMax {
				agg.rttMax = rtt.Int64
			}
			agg.hist.Observe(uint64(rtt.Int64))
		}
	}
	if err := rows.Close(); err != nil {
		return agg, err
	}
	gapRows, err := d.writer.QueryContext(ctx, `SELECT first_scheduled_at_us, interval_ns, missed_count
        FROM scheduler_gaps WHERE target_id=?
          AND first_scheduled_at_us < ?
          AND first_scheduled_at_us + ((missed_count - 1) * (interval_ns / 1000)) >= ?`, targetID, end.UnixMicro(), start.UnixMicro())
	if err != nil {
		return agg, err
	}
	for gapRows.Next() {
		var firstUS, intervalNS, count int64
		if err := gapRows.Scan(&firstUS, &intervalNS, &count); err != nil {
			gapRows.Close()
			return agg, err
		}
		agg.schedulerMissed += countPeriodic(firstUS, intervalNS/1000, count, start.UnixMicro(), end.UnixMicro())
	}
	if err := gapRows.Close(); err != nil {
		return agg, err
	}
	agg.scheduled = agg.attempted + agg.schedulerMissed
	if agg.rttCount == 0 {
		agg.rttMin = 0
	}
	return agg, nil
}

func countPeriodic(first, step, count, from, to int64) int64 {
	if step <= 0 || count <= 0 || from >= to {
		return 0
	}
	low := int64(0)
	if from > first {
		low = (from - first + step - 1) / step
	}
	high := (to - 1 - first) / step
	if high >= count {
		high = count - 1
	}
	if low < 0 {
		low = 0
	}
	if high < low {
		return 0
	}
	return high - low + 1
}

func (d *DB) recomputePingHour(ctx context.Context, key dirtyKey, asOf time.Time) error {
	start := time.UnixMicro(key.bucketUS)
	end := start.Add(time.Hour)
	if asOf.Before(end) {
		return nil
	}
	var dirtyMinutes int
	if err := d.writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM dirty_rollups
        WHERE kind='ping' AND entity_id=? AND resolution_s=60 AND bucket_start_us>=? AND bucket_start_us<?`, key.entityID, start.UnixMicro(), end.UnixMicro()).Scan(&dirtyMinutes); err != nil {
		return err
	}
	if dirtyMinutes > 0 {
		return nil
	}
	agg := pingAggregate{hist: histogram.New(), rttMin: math.MaxInt64}
	rows, err := d.writer.QueryContext(ctx, `SELECT scheduled_count, attempted_count, sent_count,
		on_time_count, late_count, unanswered_count, send_error_count, scheduler_missed_count,
		rtt_count, rtt_sum_ns, rtt_min_ns, rtt_max_ns, histogram, timeout_min_ns, timeout_max_ns
        FROM ping_rollups WHERE target_id=? AND resolution_s=60 AND bucket_start_us>=? AND bucket_start_us<?`, key.entityID, start.UnixMicro(), end.UnixMicro())
	if err != nil {
		return err
	}
	for rows.Next() {
		var min, max sql.NullInt64
		var blob []byte
		var scheduled, attempted, sent, onTime, late, unanswered, sendError, missed, rttCount, rttSum int64
		var timeoutMin, timeoutMax int64
		if err := rows.Scan(&scheduled, &attempted, &sent, &onTime, &late, &unanswered, &sendError, &missed, &rttCount, &rttSum, &min, &max, &blob, &timeoutMin, &timeoutMax); err != nil {
			rows.Close()
			return err
		}
		agg.scheduled += scheduled
		agg.attempted += attempted
		agg.sent += sent
		agg.onTime += onTime
		agg.late += late
		agg.unanswered += unanswered
		agg.sendError += sendError
		agg.schedulerMissed += missed
		agg.rttCount += rttCount
		agg.rttSum += rttSum
		if timeoutMin > 0 {
			observeTimeout(&agg, timeoutMin)
		}
		if timeoutMax > 0 {
			observeTimeout(&agg, timeoutMax)
		}
		if min.Valid && min.Int64 < agg.rttMin {
			agg.rttMin = min.Int64
		}
		if max.Valid && max.Int64 > agg.rttMax {
			agg.rttMax = max.Int64
		}
		h, err := histogram.UnmarshalBinary(blob)
		if err != nil {
			rows.Close()
			return err
		}
		agg.hist.Merge(h)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if agg.rttCount == 0 {
		agg.rttMin = 0
	}
	return d.storePingAggregate(ctx, key, agg, asOf, false)
}

func (d *DB) storePingAggregate(ctx context.Context, key dirtyKey, agg pingAggregate, asOf time.Time, dirtyHour bool) error {
	blob, err := agg.hist.MarshalBinary()
	if err != nil {
		return err
	}
	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var min, max any
	if agg.rttCount > 0 {
		min, max = agg.rttMin, agg.rttMax
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO ping_rollups(
        target_id, resolution_s, bucket_start_us, scheduled_count, attempted_count,
        sent_count, on_time_count, late_count, unanswered_count, send_error_count,
        scheduler_missed_count, rtt_count, rtt_sum_ns, rtt_min_ns, rtt_max_ns,
		histogram, updated_at_us, timeout_min_ns, timeout_max_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(target_id, resolution_s, bucket_start_us) DO UPDATE SET
        scheduled_count=excluded.scheduled_count, attempted_count=excluded.attempted_count,
        sent_count=excluded.sent_count, on_time_count=excluded.on_time_count,
        late_count=excluded.late_count, unanswered_count=excluded.unanswered_count,
        send_error_count=excluded.send_error_count, scheduler_missed_count=excluded.scheduler_missed_count,
        rtt_count=excluded.rtt_count, rtt_sum_ns=excluded.rtt_sum_ns,
        rtt_min_ns=excluded.rtt_min_ns, rtt_max_ns=excluded.rtt_max_ns,
		histogram=excluded.histogram, updated_at_us=excluded.updated_at_us,
		timeout_min_ns=excluded.timeout_min_ns, timeout_max_ns=excluded.timeout_max_ns`,
		key.entityID, key.resolution, key.bucketUS, agg.scheduled, agg.attempted,
		agg.sent, agg.onTime, agg.late, agg.unanswered, agg.sendError, agg.schedulerMissed,
		agg.rttCount, agg.rttSum, min, max, blob, asOf.UnixMicro(), agg.timeoutMin, agg.timeoutMax)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dirty_rollups WHERE kind=? AND entity_id=? AND resolution_s=? AND bucket_start_us=?`, key.kind, key.entityID, key.resolution, key.bucketUS); err != nil {
		return err
	}
	if dirtyHour {
		hour := time.UnixMicro(key.bucketUS).Truncate(time.Hour)
		if err := markDirty(ctx, tx, "ping", key.entityID, 3600, hour); err != nil {
			return err
		}
	}
	return tx.Commit()
}
