package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"flameping/internal/histogram"
)

type TargetSummary struct {
	ID                int64    `json:"-"`
	StableID          string   `json:"id"`
	Name              string   `json:"name"`
	Address           string   `json:"address"`
	Endpoint          string   `json:"endpoint,omitempty"`
	IntervalMS        float64  `json:"interval_ms"`
	TimeoutMS         float64  `json:"timeout_ms"`
	LastScheduledAtMS *int64   `json:"last_scheduled_at_ms,omitempty"`
	LastRTTMS         *float64 `json:"last_rtt_ms,omitempty"`
	State             string   `json:"state"`
}

type PingPoint struct {
	TimeMS          int64    `json:"time_ms"`
	Scheduled       int64    `json:"scheduled"`
	Attempted       int64    `json:"attempted"`
	Sent            int64    `json:"sent"`
	OnTime          int64    `json:"on_time"`
	Late            int64    `json:"late"`
	Unanswered      int64    `json:"unanswered"`
	SendErrors      int64    `json:"send_errors"`
	SchedulerMissed int64    `json:"scheduler_missed"`
	MinMS           *float64 `json:"min_ms,omitempty"`
	P50MS           *float64 `json:"p50_ms,omitempty"`
	P95MS           *float64 `json:"p95_ms,omitempty"`
	P99MS           *float64 `json:"p99_ms,omitempty"`
	MaxMS           *float64 `json:"max_ms,omitempty"`
	TimeoutMinMS    *float64 `json:"timeout_min_ms,omitempty"`
	TimeoutMaxMS    *float64 `json:"timeout_max_ms,omitempty"`
	DeadlineMissPct float64  `json:"deadline_miss_pct"`
	NoReplyPct      float64  `json:"no_reply_pct"`
	Partial         bool     `json:"partial,omitempty"`
}

type PingSeries struct {
	TargetID    string      `json:"target_id"`
	AsOfMS      int64       `json:"as_of_ms"`
	ResolutionS int64       `json:"resolution_s"`
	Points      []PingPoint `json:"points"`
}

type queryAggregate struct {
	pingAggregate
	partial bool
}

func (d *DB) Targets(ctx context.Context, asOf time.Time) ([]TargetSummary, error) {
	rows, err := d.readers.QueryContext(ctx, `
	SELECT t.id, t.stable_id, t.display_name, t.configured_address, t.interval_ns, t.timeout_ns,
		l.scheduled_at_us, l.sent_at_us, l.timeout_ns, l.reply_at_us, l.rtt_ns, l.local_outcome, e.address,
		g.first_scheduled_at_us+((g.missed_count-1)*(g.interval_ns/1000))
	FROM targets t LEFT JOIN ping_samples l ON (l.run_id,l.sequence)=(
		SELECT p.run_id,p.sequence FROM ping_samples p WHERE p.target_id=t.id ORDER BY p.scheduled_at_us DESC LIMIT 1)
	LEFT JOIN endpoints e ON e.id=l.endpoint_id
	LEFT JOIN scheduler_gaps g ON g.id=(SELECT gap.id FROM scheduler_gaps gap WHERE gap.target_id=t.id
		ORDER BY gap.first_scheduled_at_us+((gap.missed_count-1)*(gap.interval_ns/1000)) DESC LIMIT 1)
	WHERE t.active=1 ORDER BY t.display_name, t.stable_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make([]TargetSummary, 0)
	for rows.Next() {
		var item TargetSummary
		var intervalNS, timeoutNS int64
		var scheduled, sent, sampleTimeout, reply, rtt sql.NullInt64
		var gapLast sql.NullInt64
		var local, endpoint sql.NullString
		if err := rows.Scan(&item.ID, &item.StableID, &item.Name, &item.Address, &intervalNS, &timeoutNS,
			&scheduled, &sent, &sampleTimeout, &reply, &rtt, &local, &endpoint, &gapLast); err != nil {
			return nil, err
		}
		item.IntervalMS = float64(intervalNS) / 1e6
		item.TimeoutMS = float64(timeoutNS) / 1e6
		if endpoint.Valid {
			item.Endpoint = endpoint.String
		}
		if gapLast.Valid && (!scheduled.Valid || gapLast.Int64 > scheduled.Int64) {
			ms := gapLast.Int64 / 1000
			item.LastScheduledAtMS = &ms
			item.State = "scheduler_gap"
		} else if !scheduled.Valid {
			item.State = "unknown"
		} else {
			ms := scheduled.Int64 / 1000
			item.LastScheduledAtMS = &ms
			if asOf.UnixMicro()-scheduled.Int64 > max(3*intervalNS, timeoutNS+intervalNS)/1000 {
				item.State = "stale"
				targets = append(targets, item)
				continue
			}
			switch {
			case local.Valid:
				item.State = "send_error"
			case reply.Valid:
				value := float64(rtt.Int64) / 1e6
				item.LastRTTMS = &value
				if rtt.Int64 > sampleTimeout.Int64 {
					item.State = "late"
				} else {
					item.State = "up"
				}
			case asOf.UnixMicro() >= sent.Int64+sampleTimeout.Int64/1000:
				item.State = "down"
			default:
				item.State = "pending"
			}
		}
		targets = append(targets, item)
	}
	return targets, rows.Err()
}

func (d *DB) PingSeries(ctx context.Context, stableID string, from, to time.Time, maxPoints int) (PingSeries, error) {
	if !from.Before(to) || maxPoints < 1 || maxPoints > 20000 {
		return PingSeries{}, errors.New("invalid series range or max_points")
	}
	tx, err := d.readers.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PingSeries{}, err
	}
	defer tx.Rollback()
	asOf := time.Now()
	if to.After(asOf) {
		to = asOf
	}
	if !from.Before(to) {
		return PingSeries{}, errors.New("series range is entirely in the future")
	}
	var targetID, intervalNS int64
	if err := tx.QueryRowContext(ctx, `SELECT id, interval_ns FROM targets WHERE stable_id=?`, stableID).Scan(&targetID, &intervalNS); err != nil {
		return PingSeries{}, err
	}
	desired := (to.Sub(from) + time.Duration(maxPoints) - 1) / time.Duration(maxPoints)
	if desired < time.Duration(intervalNS) {
		desired = time.Duration(intervalNS)
	}
	sourceResolution := int64(0)
	pointWidth := desired
	var rawStart, minuteStart, hourStart sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(scheduled_at_us) FROM ping_samples WHERE target_id=?`, targetID).Scan(&rawStart); err != nil {
		return PingSeries{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT MIN(bucket_start_us) FROM ping_rollups WHERE target_id=? AND resolution_s=60`, targetID).Scan(&minuteStart); err != nil {
		return PingSeries{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT MIN(bucket_start_us) FROM ping_rollups WHERE target_id=? AND resolution_s=3600`, targetID).Scan(&hourStart); err != nil {
		return PingSeries{}, err
	}
	rawCovers := rawStart.Valid && rawStart.Int64 <= from.UnixMicro()
	minuteCovers := minuteStart.Valid && minuteStart.Int64 <= from.UnixMicro()
	if (desired >= time.Hour && hourStart.Valid) || (!rawCovers && !minuteCovers && hourStart.Valid) {
		sourceResolution, pointWidth = 3600, roundWidth(desired, time.Hour)
	} else if (desired >= time.Minute && minuteStart.Valid) || (!rawCovers && minuteStart.Valid) {
		sourceResolution, pointWidth = 60, roundWidth(desired, time.Minute)
	}
	var aggregates map[int64]*queryAggregate
	if sourceResolution == 0 {
		aggregates, err = queryRawAggregates(ctx, tx, targetID, from, to, asOf, pointWidth, nil)
	} else {
		aggregates, err = queryRollupAggregates(ctx, tx, targetID, from, to, sourceResolution, pointWidth, nil, nil, 0)
		if err == nil {
			err = overlayDirty(ctx, tx, targetID, from, to, asOf, sourceResolution, pointWidth, aggregates)
		}
	}
	if err != nil {
		return PingSeries{}, err
	}
	points := aggregatesToPoints(aggregates)
	if len(points) > maxPoints+2 {
		return PingSeries{}, fmt.Errorf("query planner produced %d points over cap %d", len(points), maxPoints)
	}
	if err := tx.Commit(); err != nil {
		return PingSeries{}, err
	}
	return PingSeries{TargetID: stableID, AsOfMS: asOf.UnixMilli(), ResolutionS: max(1, int64(pointWidth/time.Second)), Points: points}, nil
}

func roundWidth(w, base time.Duration) time.Duration {
	units := (w + base - 1) / base
	return units * base
}

func queryRollupAggregates(ctx context.Context, q *sql.Tx, targetID int64, from, to time.Time, sourceResolution int64, pointWidth time.Duration, skip, onlyParent map[int64]struct{}, parentWidth time.Duration) (map[int64]*queryAggregate, error) {
	result := make(map[int64]*queryAggregate)
	rows, err := q.QueryContext(ctx, `SELECT bucket_start_us, scheduled_count, attempted_count,
        sent_count, on_time_count, late_count, unanswered_count, send_error_count,
		scheduler_missed_count, rtt_count, rtt_sum_ns, rtt_min_ns, rtt_max_ns, histogram, timeout_min_ns, timeout_max_ns
        FROM ping_rollups WHERE target_id=? AND resolution_s=? AND bucket_start_us>=? AND bucket_start_us<?
        ORDER BY bucket_start_us`, targetID, sourceResolution, from.Truncate(time.Duration(sourceResolution)*time.Second).UnixMicro(), to.UnixMicro())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	widthUS := pointWidth.Microseconds()
	for rows.Next() {
		var bucketUS int64
		var minRTT, maxRTT sql.NullInt64
		var blob []byte
		var a pingAggregate
		if err := rows.Scan(&bucketUS, &a.scheduled, &a.attempted, &a.sent, &a.onTime, &a.late, &a.unanswered,
			&a.sendError, &a.schedulerMissed, &a.rttCount, &a.rttSum, &minRTT, &maxRTT, &blob, &a.timeoutMin, &a.timeoutMax); err != nil {
			return nil, err
		}
		if _, excluded := skip[bucketUS]; excluded {
			continue
		}
		if onlyParent != nil {
			parent := bucketUS - bucketUS%parentWidth.Microseconds()
			if _, included := onlyParent[parent]; !included {
				continue
			}
		}
		a.hist, err = histogram.UnmarshalBinary(blob)
		if err != nil {
			return nil, err
		}
		if minRTT.Valid {
			a.rttMin, a.rttMax = minRTT.Int64, maxRTT.Int64
		}
		key := bucketUS - bucketUS%widthUS
		partial := bucketUS < from.UnixMicro() || bucketUS+sourceResolution*int64(time.Second/time.Microsecond) > to.UnixMicro()
		mergeQueryAggregate(result, key, a, partial)
	}
	return result, rows.Err()
}

func overlayDirty(ctx context.Context, q *sql.Tx, targetID int64, from, to, asOf time.Time, sourceResolution int64, pointWidth time.Duration, result map[int64]*queryAggregate) error {
	replace := make(map[int64]struct{})
	dirtyMinutes := make(map[int64]struct{})
	sourceWidth := time.Duration(sourceResolution) * time.Second
	current := asOf.Truncate(sourceWidth).UnixMicro()
	if current >= from.UnixMicro() && current < to.UnixMicro() {
		replace[current] = struct{}{}
	}
	currentMinute := asOf.Truncate(time.Minute).UnixMicro()
	if currentMinute >= from.Add(-pointWidth).UnixMicro() && currentMinute < to.UnixMicro() {
		dirtyMinutes[currentMinute] = struct{}{}
	}
	rows, err := q.QueryContext(ctx, `SELECT resolution_s, bucket_start_us FROM dirty_rollups
        WHERE kind='ping' AND entity_id=? AND bucket_start_us<?`, targetID, to.UnixMicro())
	if err != nil {
		return err
	}
	for rows.Next() {
		var resolution, bucket int64
		if err := rows.Scan(&resolution, &bucket); err != nil {
			rows.Close()
			return err
		}
		if resolution > sourceResolution {
			continue
		}
		if resolution == 60 {
			dirtyMinutes[bucket] = struct{}{}
		}
		if sourceResolution == 3600 {
			bucket = time.UnixMicro(bucket).Truncate(time.Hour).UnixMicro()
		}
		if bucket >= from.Add(-sourceWidth).UnixMicro() && bucket < to.UnixMicro() {
			replace[bucket] = struct{}{}
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(replace) == 0 {
		return nil
	}
	affectedPoints := make(map[int64]struct{})
	for bucket := range replace {
		pointKey := bucket - bucket%pointWidth.Microseconds()
		affectedPoints[pointKey] = struct{}{}
		delete(result, pointKey)
	}
	minUS, maxUS := int64(math.MaxInt64), int64(math.MinInt64)
	for point := range affectedPoints {
		if point < minUS {
			minUS = point
		}
		if point > maxUS {
			maxUS = point
		}
	}
	windowFrom, windowTo := time.UnixMicro(minUS), time.UnixMicro(maxUS).Add(pointWidth)
	// Rebuild affected rendered points from clean source rollups first. This is
	// essential after raw retention has removed clean sibling buckets.
	rebuilt, err := queryRollupAggregates(ctx, q, targetID, windowFrom, windowTo, sourceResolution, pointWidth, replace, nil, 0)
	if err != nil {
		return err
	}
	if sourceResolution == 3600 {
		minutes, minuteErr := queryRollupAggregates(ctx, q, targetID, windowFrom, windowTo, 60, pointWidth, dirtyMinutes, replace, time.Hour)
		if minuteErr != nil {
			return minuteErr
		}
		for key, value := range minutes {
			mergeQueryAggregate(rebuilt, key, value.pingAggregate, value.partial)
		}
	}
	if len(dirtyMinutes) > 0 {
		raw, rawErr := queryRawAggregates(ctx, q, targetID, windowFrom, windowTo, asOf, pointWidth, dirtyMinutes)
		if rawErr != nil {
			return rawErr
		}
		for key, value := range raw {
			mergeQueryAggregate(rebuilt, key, value.pingAggregate, value.partial)
		}
	}
	for key := range affectedPoints {
		if value := rebuilt[key]; value != nil {
			result[key] = value
		}
	}
	return nil
}

func queryRawAggregates(ctx context.Context, q *sql.Tx, targetID int64, from, to, asOf time.Time, width time.Duration, allowedSource map[int64]struct{}) (map[int64]*queryAggregate, error) {
	result := make(map[int64]*queryAggregate)
	widthUS := width.Microseconds()
	rows, err := q.QueryContext(ctx, `SELECT scheduled_at_us, sent_at_us, timeout_ns,
        local_outcome, reply_at_us, rtt_ns, reply_class
        FROM ping_samples WHERE target_id=? AND scheduled_at_us>=? AND scheduled_at_us<?
        ORDER BY scheduled_at_us`, targetID, from.UnixMicro(), to.UnixMicro())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var scheduled, sent, timeout int64
		var local, class sql.NullString
		var reply, rtt sql.NullInt64
		if err := rows.Scan(&scheduled, &sent, &timeout, &local, &reply, &rtt, &class); err != nil {
			rows.Close()
			return nil, err
		}
		sourceBucket := scheduled - scheduled%(int64(time.Minute/time.Microsecond))
		if allowedSource != nil {
			if _, ok := allowedSource[sourceBucket]; !ok {
				hourBucket := time.UnixMicro(scheduled).Truncate(time.Hour).UnixMicro()
				if _, ok := allowedSource[hourBucket]; !ok {
					continue
				}
			}
		}
		key := scheduled - scheduled%widthUS
		entry := ensureQueryAggregate(result, key)
		entry.attempted++
		entry.scheduled++
		observeTimeout(&entry.pingAggregate, timeout)
		if local.Valid {
			entry.sendError++
			continue
		}
		entry.sent++
		if !reply.Valid {
			if sent+timeout/1000 <= asOf.UnixMicro() {
				entry.unanswered++
			} else {
				entry.partial = true
			}
			continue
		}
		if class.String == "late" {
			entry.late++
		} else {
			entry.onTime++
		}
		if rtt.Valid {
			observeRTT(&entry.pingAggregate, rtt.Int64)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	gapRows, err := q.QueryContext(ctx, `SELECT first_scheduled_at_us, interval_ns, missed_count
        FROM scheduler_gaps WHERE target_id=? AND first_scheduled_at_us<?
          AND first_scheduled_at_us + ((missed_count-1)*(interval_ns/1000))>=?`, targetID, to.UnixMicro(), from.UnixMicro())
	if err != nil {
		return nil, err
	}
	for gapRows.Next() {
		var first, intervalNS, count int64
		if err := gapRows.Scan(&first, &intervalNS, &count); err != nil {
			gapRows.Close()
			return nil, err
		}
		step := intervalNS / 1000
		firstBucket := max(first, from.UnixMicro())
		firstBucket -= firstBucket % widthUS
		for bucket := firstBucket; bucket < to.UnixMicro(); bucket += widthUS {
			bucketEnd := min(bucket+widthUS, to.UnixMicro())
			missed := countPeriodic(first, step, count, max(bucket, from.UnixMicro()), bucketEnd)
			if missed == 0 {
				continue
			}
			sourceBucket := bucket - bucket%(int64(time.Minute/time.Microsecond))
			if allowedSource != nil {
				if _, ok := allowedSource[sourceBucket]; !ok {
					hourBucket := time.UnixMicro(bucket).Truncate(time.Hour).UnixMicro()
					if _, ok := allowedSource[hourBucket]; !ok {
						continue
					}
				}
			}
			entry := ensureQueryAggregate(result, bucket)
			entry.schedulerMissed += missed
			entry.scheduled += missed
		}
	}
	return result, gapRows.Close()
}

func ensureQueryAggregate(result map[int64]*queryAggregate, key int64) *queryAggregate {
	entry := result[key]
	if entry == nil {
		entry = &queryAggregate{pingAggregate: pingAggregate{hist: histogram.New(), rttMin: math.MaxInt64}}
		result[key] = entry
	}
	return entry
}

func observeRTT(a *pingAggregate, rtt int64) {
	a.rttCount++
	a.rttSum += rtt
	if rtt < a.rttMin {
		a.rttMin = rtt
	}
	if rtt > a.rttMax {
		a.rttMax = rtt
	}
	a.hist.Observe(uint64(rtt))
}

func observeTimeout(a *pingAggregate, timeout int64) {
	if timeout <= 0 {
		return
	}
	if a.timeoutMin == 0 || timeout < a.timeoutMin {
		a.timeoutMin = timeout
	}
	if timeout > a.timeoutMax {
		a.timeoutMax = timeout
	}
}

func mergeQueryAggregate(result map[int64]*queryAggregate, key int64, source pingAggregate, partial bool) {
	dest := ensureQueryAggregate(result, key)
	dest.scheduled += source.scheduled
	dest.attempted += source.attempted
	dest.sent += source.sent
	dest.onTime += source.onTime
	dest.late += source.late
	dest.unanswered += source.unanswered
	dest.sendError += source.sendError
	dest.schedulerMissed += source.schedulerMissed
	dest.rttCount += source.rttCount
	dest.rttSum += source.rttSum
	if source.timeoutMin > 0 {
		observeTimeout(&dest.pingAggregate, source.timeoutMin)
	}
	if source.timeoutMax > 0 {
		observeTimeout(&dest.pingAggregate, source.timeoutMax)
	}
	if source.rttCount > 0 {
		if source.rttMin < dest.rttMin {
			dest.rttMin = source.rttMin
		}
		if source.rttMax > dest.rttMax {
			dest.rttMax = source.rttMax
		}
		dest.hist.Merge(source.hist)
	}
	dest.partial = dest.partial || partial
}

func aggregatesToPoints(aggregates map[int64]*queryAggregate) []PingPoint {
	keys := make([]int64, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	points := make([]PingPoint, 0, len(keys))
	for _, key := range keys {
		a := aggregates[key]
		point := PingPoint{TimeMS: key / 1000, Scheduled: a.scheduled, Attempted: a.attempted, Sent: a.sent,
			OnTime: a.onTime, Late: a.late, Unanswered: a.unanswered, SendErrors: a.sendError,
			SchedulerMissed: a.schedulerMissed, Partial: a.partial}
		if a.rttCount > 0 {
			minMS, p50MS := float64(a.rttMin)/1e6, float64(a.hist.Quantile(0.50))/1e6
			p95MS, p99MS, maxMS := float64(a.hist.Quantile(0.95))/1e6, float64(a.hist.Quantile(0.99))/1e6, float64(a.rttMax)/1e6
			point.MinMS, point.P50MS, point.P95MS, point.P99MS, point.MaxMS = &minMS, &p50MS, &p95MS, &p99MS, &maxMS
		}
		if a.timeoutMin > 0 {
			minimum, maximum := float64(a.timeoutMin)/1e6, float64(a.timeoutMax)/1e6
			point.TimeoutMinMS, point.TimeoutMaxMS = &minimum, &maximum
		}
		if a.sent > 0 {
			point.DeadlineMissPct = 100 * float64(a.late+a.unanswered) / float64(a.sent)
			point.NoReplyPct = 100 * float64(a.unanswered) / float64(a.sent)
		}
		points = append(points, point)
	}
	return points
}

type StorageStatus struct {
	Ready               bool              `json:"ready"`
	WriterError         string            `json:"writer_error,omitempty"`
	DatabaseBytes       int64             `json:"database_bytes"`
	WALBytes            int64             `json:"wal_bytes"`
	SHMBytes            int64             `json:"shm_bytes"`
	LiveBytes           int64             `json:"live_bytes"`
	ReusableBytes       int64             `json:"reusable_bytes"`
	MaxBytes            int64             `json:"max_bytes"`
	RawHorizonMS        *int64            `json:"raw_horizon_ms,omitempty"`
	MinuteHorizonMS     *int64            `json:"minute_horizon_ms,omitempty"`
	HourHorizonMS       *int64            `json:"hour_horizon_ms,omitempty"`
	DirtyBuckets        int64             `json:"dirty_buckets"`
	FilesystemBytes     int64             `json:"filesystem_bytes"`
	AvailableBytes      int64             `json:"available_bytes"`
	ReserveBytes        int64             `json:"reserve_bytes"`
	Pressure            string            `json:"pressure"`
	TraceCapabilities   map[string]string `json:"trace_capabilities"`
	InterfaceCapability string            `json:"interface_capability"`
}

func (d *DB) Status(ctx context.Context) (StorageStatus, error) {
	status := StorageStatus{Ready: d.Ready(), MaxBytes: d.config.MaxBytes.Int64(), TraceCapabilities: make(map[string]string)}
	d.statusMu.RLock()
	status.InterfaceCapability = d.interfaceStatus
	for family, value := range d.traceStatus {
		status.TraceCapabilities[family] = value
	}
	d.statusMu.RUnlock()
	if err := d.WriterError(); err != nil {
		status.WriterError = err.Error()
	}
	status.DatabaseBytes = fileSize(d.path)
	status.WALBytes = fileSize(d.path + "-wal")
	status.SHMBytes = fileSize(d.path + "-shm")
	var pageSize, pageCount, freeList int64
	if err := d.readers.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return status, err
	}
	if err := d.readers.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return status, err
	}
	if err := d.readers.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freeList); err != nil {
		return status, err
	}
	status.LiveBytes = (pageCount - freeList) * pageSize
	status.ReusableBytes = freeList * pageSize
	pressure, pressureErr := d.pressure(ctx, d.readers)
	if pressureErr != nil {
		return status, pressureErr
	}
	status.FilesystemBytes, status.AvailableBytes, status.ReserveBytes = pressure.capacity, pressure.available, pressure.reserve
	status.Pressure = "normal"
	if d.pressureHigh(pressure) {
		status.Pressure = "high"
	}
	if d.pressureCritical(pressure) != nil {
		status.Pressure = "critical"
	}
	_ = scanOptionalMillis(d.readers.QueryRowContext(ctx, `SELECT MIN(scheduled_at_us) FROM ping_samples`), &status.RawHorizonMS)
	_ = scanOptionalMillis(d.readers.QueryRowContext(ctx, `SELECT MIN(bucket_start_us) FROM ping_rollups WHERE resolution_s=60`), &status.MinuteHorizonMS)
	_ = scanOptionalMillis(d.readers.QueryRowContext(ctx, `SELECT MIN(bucket_start_us) FROM ping_rollups WHERE resolution_s=3600`), &status.HourHorizonMS)
	if err := d.readers.QueryRowContext(ctx, `SELECT COUNT(*) FROM dirty_rollups`).Scan(&status.DirtyBuckets); err != nil {
		return status, err
	}
	return status, nil
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func scanOptionalMillis(row *sql.Row, destination **int64) error {
	var value sql.NullInt64
	if err := row.Scan(&value); err != nil {
		return err
	}
	if value.Valid {
		ms := value.Int64 / 1000
		*destination = &ms
	}
	return nil
}

type InterfaceSummary struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	IfIndex     int    `json:"ifindex"`
	MAC         string `json:"mac"`
	LastAtMS    int64  `json:"last_at_ms"`
	Present     bool   `json:"present"`
}

type InterfacePoint struct {
	TimeMS    int64   `json:"time_ms"`
	RXMbps    float64 `json:"rx_mbps"`
	TXMbps    float64 `json:"tx_mbps"`
	RXErrors  int64   `json:"rx_errors"`
	TXErrors  int64   `json:"tx_errors"`
	RXDropped int64   `json:"rx_dropped"`
	TXDropped int64   `json:"tx_dropped"`
	RXMissed  int64   `json:"rx_missed"`
	Reset     bool    `json:"reset,omitempty"`
}

type InterfaceSeries struct {
	Name   string           `json:"name"`
	AsOfMS int64            `json:"as_of_ms"`
	Points []InterfacePoint `json:"points"`
}

type interfaceQueryRow struct{ generation, at, rx, tx, rxErr, txErr, rxDrop, txDrop, rxMiss int64 }

func (d *DB) Interfaces(ctx context.Context) ([]InterfaceSummary, error) {
	rows, err := d.readers.QueryContext(ctx, `SELECT ci.name,ci.display_name,COALESCE(g.ifindex,0),COALESCE(g.mac,''),COALESCE(s.sampled_at_us,0),
		CASE WHEN g.id IS NOT NULL AND g.ended_at_us IS NULL AND s.sampled_at_us IS NOT NULL THEN 1 ELSE 0 END
		FROM configured_interfaces ci
		LEFT JOIN interface_generations g ON g.id=(SELECT ig.id FROM interface_generations ig
			JOIN interface_samples latest ON latest.generation_id=ig.id WHERE ig.name=ci.name
			ORDER BY latest.sampled_at_us DESC LIMIT 1)
		LEFT JOIN interface_samples s ON s.generation_id=g.id AND s.sampled_at_us=(
			SELECT MAX(last.sampled_at_us) FROM interface_samples last WHERE last.generation_id=g.id)
		WHERE ci.active=1 ORDER BY ci.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]InterfaceSummary, 0)
	for rows.Next() {
		var item InterfaceSummary
		var at int64
		var present int
		if err := rows.Scan(&item.Name, &item.DisplayName, &item.IfIndex, &item.MAC, &at, &present); err != nil {
			return nil, err
		}
		item.LastAtMS = at / 1000
		item.Present = present != 0
		result = append(result, item)
	}
	return result, rows.Err()
}

func (d *DB) InterfaceSeries(ctx context.Context, name string, from, to time.Time, maxPoints int) (InterfaceSeries, error) {
	if !from.Before(to) || maxPoints < 1 || maxPoints > 20000 {
		return InterfaceSeries{}, errors.New("invalid interface series range")
	}
	tx, err := d.readers.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return InterfaceSeries{}, err
	}
	defer tx.Rollback()
	width := roundWidth((to.Sub(from)+time.Duration(maxPoints)-1)/time.Duration(maxPoints), time.Second)
	byBucket := make(map[int64]*InterfacePoint)
	asOf := time.Now()
	sourceResolution := int64(0)
	var rawStart, minuteStart, hourStart sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(s.sampled_at_us) FROM interface_samples s JOIN interface_generations g ON g.id=s.generation_id WHERE g.name=?`, name).Scan(&rawStart); err != nil {
		return InterfaceSeries{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT MIN(r.bucket_start_us) FROM interface_rollups r JOIN interface_generations g ON g.id=r.generation_id WHERE g.name=? AND r.resolution_s=60`, name).Scan(&minuteStart); err != nil {
		return InterfaceSeries{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT MIN(r.bucket_start_us) FROM interface_rollups r JOIN interface_generations g ON g.id=r.generation_id WHERE g.name=? AND r.resolution_s=3600`, name).Scan(&hourStart); err != nil {
		return InterfaceSeries{}, err
	}
	rawCovers := rawStart.Valid && rawStart.Int64 <= from.UnixMicro()
	minuteCovers := minuteStart.Valid && minuteStart.Int64 <= from.UnixMicro()
	if (width >= time.Hour && hourStart.Valid) || (!rawCovers && !minuteCovers && hourStart.Valid) {
		sourceResolution, width = 3600, roundWidth(width, time.Hour)
	} else if (width >= time.Minute && minuteStart.Valid) || (!rawCovers && minuteStart.Valid) {
		sourceResolution, width = 60, roundWidth(width, time.Minute)
	}
	if sourceResolution == 0 {
		if err := queryInterfaceRaw(ctx, tx, name, from, to, width, byBucket); err != nil {
			return InterfaceSeries{}, err
		}
	} else {
		dirtyMinutes, dirtyHours := make(map[int64]struct{}), make(map[int64]struct{})
		currentMinute := asOf.Truncate(time.Minute).UnixMicro()
		currentHour := asOf.Truncate(time.Hour).UnixMicro()
		if currentMinute < to.UnixMicro() && currentMinute >= from.Add(-width).UnixMicro() {
			dirtyMinutes[currentMinute] = struct{}{}
		}
		if sourceResolution == 3600 && currentHour < to.UnixMicro() && currentHour >= from.Add(-width).UnixMicro() {
			dirtyHours[currentHour] = struct{}{}
		}
		dirtyRows, dirtyErr := tx.QueryContext(ctx, `SELECT d.resolution_s,d.bucket_start_us FROM dirty_rollups d JOIN interface_generations g ON g.id=d.entity_id WHERE d.kind='interface' AND g.name=? AND d.bucket_start_us<?`, name, to.UnixMicro())
		if dirtyErr != nil {
			return InterfaceSeries{}, dirtyErr
		}
		for dirtyRows.Next() {
			var resolution, bucket int64
			if err := dirtyRows.Scan(&resolution, &bucket); err != nil {
				dirtyRows.Close()
				return InterfaceSeries{}, err
			}
			if resolution == 60 {
				dirtyMinutes[bucket] = struct{}{}
				if sourceResolution == 3600 {
					dirtyHours[time.UnixMicro(bucket).Truncate(time.Hour).UnixMicro()] = struct{}{}
				}
			} else if resolution == 3600 && sourceResolution == 3600 {
				dirtyHours[bucket] = struct{}{}
			}
		}
		if err := dirtyRows.Close(); err != nil {
			return InterfaceSeries{}, err
		}
		skip := dirtyMinutes
		if sourceResolution == 3600 {
			skip = dirtyHours
		}
		if err := queryInterfaceRollupPoints(ctx, tx, name, sourceResolution, from, to, width, skip, nil, byBucket); err != nil {
			return InterfaceSeries{}, err
		}
		if sourceResolution == 3600 {
			if err := queryInterfaceRollupPoints(ctx, tx, name, 60, from, to, width, dirtyMinutes, dirtyHours, byBucket); err != nil {
				return InterfaceSeries{}, err
			}
		}
		for minute := range dirtyMinutes {
			start := time.UnixMicro(minute)
			end := start.Add(time.Minute)
			if end.After(from) && start.Before(to) {
				if start.Before(from) {
					start = from
				}
				if end.After(to) {
					end = to
				}
				if err := queryInterfaceRaw(ctx, tx, name, start, end, width, byBucket); err != nil {
					return InterfaceSeries{}, err
				}
			}
		}
	}
	resetRows, err := tx.QueryContext(ctx, `SELECT at_us FROM interface_resets WHERE name=? AND at_us>=? AND at_us<?`, name, from.UnixMicro(), to.UnixMicro())
	if err != nil {
		return InterfaceSeries{}, err
	}
	for resetRows.Next() {
		var at int64
		if err := resetRows.Scan(&at); err != nil {
			resetRows.Close()
			return InterfaceSeries{}, err
		}
		key := at - at%width.Microseconds()
		point := byBucket[key]
		if point == nil {
			point = &InterfacePoint{TimeMS: key / 1000}
			byBucket[key] = point
		}
		point.Reset = true
	}
	if err := resetRows.Close(); err != nil {
		return InterfaceSeries{}, err
	}
	keys := make([]int64, 0, len(byBucket))
	for key := range byBucket {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	points := make([]InterfacePoint, 0, len(keys))
	for _, key := range keys {
		points = append(points, *byBucket[key])
	}
	if err := tx.Commit(); err != nil {
		return InterfaceSeries{}, err
	}
	return InterfaceSeries{Name: name, AsOfMS: asOf.UnixMilli(), Points: points}, nil
}

func queryInterfaceRollupPoints(ctx context.Context, q *sql.Tx, name string, resolution int64, from, to time.Time, width time.Duration, skip, parents map[int64]struct{}, byBucket map[int64]*InterfacePoint) error {
	rows, err := q.QueryContext(ctx, `SELECT r.bucket_start_us,r.elapsed_ns,r.reset_count,r.rx_bytes_delta,r.tx_bytes_delta,r.rx_errors_delta,r.tx_errors_delta,r.rx_dropped_delta,r.tx_dropped_delta,r.rx_missed_delta,r.peak_rx_bytes_per_s,r.peak_tx_bytes_per_s FROM interface_rollups r JOIN interface_generations g ON g.id=r.generation_id WHERE g.name=? AND r.resolution_s=? AND r.bucket_start_us>=? AND r.bucket_start_us<? ORDER BY r.bucket_start_us`, name, resolution, from.Truncate(time.Duration(resolution)*time.Second).UnixMicro(), to.UnixMicro())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bucket, elapsed, resets, rx, tx, rxErr, txErr, rxDrop, txDrop, rxMiss, peakRX, peakTX int64
		if err := rows.Scan(&bucket, &elapsed, &resets, &rx, &tx, &rxErr, &txErr, &rxDrop, &txDrop, &rxMiss, &peakRX, &peakTX); err != nil {
			return err
		}
		if _, excluded := skip[bucket]; excluded {
			continue
		}
		if parents != nil {
			hour := time.UnixMicro(bucket).Truncate(time.Hour).UnixMicro()
			if _, included := parents[hour]; !included {
				continue
			}
		}
		key := bucket - bucket%width.Microseconds()
		point := byBucket[key]
		if point == nil {
			point = &InterfacePoint{TimeMS: key / 1000}
			byBucket[key] = point
		}
		point.RXMbps = max(point.RXMbps, float64(peakRX)*8/1e6)
		point.TXMbps = max(point.TXMbps, float64(peakTX)*8/1e6)
		point.RXErrors += rxErr
		point.TXErrors += txErr
		point.RXDropped += rxDrop
		point.TXDropped += txDrop
		point.RXMissed += rxMiss
		point.Reset = point.Reset || resets > 0
	}
	return rows.Err()
}

func queryInterfaceRaw(ctx context.Context, q *sql.Tx, name string, from, to time.Time, width time.Duration, byBucket map[int64]*InterfacePoint) error {
	rows, err := q.QueryContext(ctx, `SELECT s.generation_id, s.sampled_at_us,
		s.rx_bytes, s.tx_bytes, s.rx_errors, s.tx_errors, s.rx_dropped, s.tx_dropped, s.rx_missed
		FROM interface_samples s JOIN interface_generations g ON g.id=s.generation_id
		WHERE g.name=? AND s.sampled_at_us<? AND (s.sampled_at_us>=? OR s.sampled_at_us=(
			SELECT MAX(p.sampled_at_us) FROM interface_samples p WHERE p.generation_id=s.generation_id AND p.sampled_at_us<?))
		ORDER BY s.generation_id, s.sampled_at_us`, name, to.UnixMicro(), from.UnixMicro(), from.UnixMicro())
	if err != nil {
		return err
	}
	defer rows.Close()
	var previous *interfaceQueryRow
	for rows.Next() {
		var current interfaceQueryRow
		if err := rows.Scan(&current.generation, &current.at, &current.rx, &current.tx, &current.rxErr, &current.txErr, &current.rxDrop, &current.txDrop, &current.rxMiss); err != nil {
			return err
		}
		if previous == nil || previous.generation != current.generation || current.at <= previous.at {
			previous = &current
			continue
		}
		elapsed := float64(current.at-previous.at) / 1e6
		key := current.at - current.at%width.Microseconds()
		point := byBucket[key]
		if point == nil {
			point = &InterfacePoint{TimeMS: key / 1000}
			byBucket[key] = point
		}
		point.RXMbps = max(point.RXMbps, float64(current.rx-previous.rx)*8/elapsed/1e6)
		point.TXMbps = max(point.TXMbps, float64(current.tx-previous.tx)*8/elapsed/1e6)
		point.RXErrors += current.rxErr - previous.rxErr
		point.TXErrors += current.txErr - previous.txErr
		point.RXDropped += current.rxDrop - previous.rxDrop
		point.TXDropped += current.txDrop - previous.txDrop
		point.RXMissed += current.rxMiss - previous.rxMiss
		previous = &current
	}
	return rows.Err()
}

type TraceSummary struct {
	ID          int64  `json:"id"`
	StartedMS   int64  `json:"started_ms"`
	EndedMS     int64  `json:"ended_ms"`
	Method      string `json:"method"`
	Status      string `json:"status"`
	Reached     bool   `json:"reached"`
	ReachedHop  int    `json:"reached_hop"`
	Signature   string `json:"signature"`
	ErrorDetail string `json:"error_detail,omitempty"`
}

type TraceDetail struct {
	TraceSummary
	TargetID string `json:"target_id"`
	Endpoint string `json:"endpoint"`
	Probes   []struct {
		TTL       int      `json:"ttl"`
		Index     int      `json:"index"`
		Responder *string  `json:"responder,omitempty"`
		RTTMS     *float64 `json:"rtt_ms,omitempty"`
		ICMPType  *int     `json:"icmp_type,omitempty"`
		ICMPCode  *int     `json:"icmp_code,omitempty"`
	} `json:"probes"`
}

func (d *DB) Traces(ctx context.Context, stableID string, limit int) ([]TraceSummary, error) {
	if limit < 1 || limit > 500 {
		return nil, errors.New("invalid trace limit")
	}
	rows, err := d.readers.QueryContext(ctx, `SELECT tr.id, tr.started_at_us, tr.ended_at_us, tr.method,
		tr.status, tr.reached, tr.reached_hop, tr.signature, tr.error_detail
        FROM trace_runs tr JOIN targets t ON t.id=tr.target_id
        WHERE t.stable_id=? ORDER BY tr.started_at_us DESC LIMIT ?`, stableID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]TraceSummary, 0)
	for rows.Next() {
		var item TraceSummary
		var started, ended int64
		if err := rows.Scan(&item.ID, &started, &ended, &item.Method, &item.Status, &item.Reached, &item.ReachedHop, &item.Signature, &item.ErrorDetail); err != nil {
			return nil, err
		}
		item.StartedMS, item.EndedMS = started/1000, ended/1000
		result = append(result, item)
	}
	return result, rows.Err()
}

func (d *DB) Trace(ctx context.Context, id int64) (TraceDetail, error) {
	var result TraceDetail
	var started, ended int64
	err := d.readers.QueryRowContext(ctx, `SELECT tr.id, tr.started_at_us, tr.ended_at_us, tr.method,
		tr.status, tr.reached, tr.reached_hop, tr.signature, tr.error_detail, t.stable_id, e.address
        FROM trace_runs tr JOIN targets t ON t.id=tr.target_id JOIN endpoints e ON e.id=tr.endpoint_id
        WHERE tr.id=?`, id).Scan(&result.ID, &started, &ended, &result.Method, &result.Status, &result.Reached,
		&result.ReachedHop, &result.Signature, &result.ErrorDetail, &result.TargetID, &result.Endpoint)
	if err != nil {
		return result, err
	}
	result.StartedMS, result.EndedMS = started/1000, ended/1000
	result.Probes = make([]struct {
		TTL       int      `json:"ttl"`
		Index     int      `json:"index"`
		Responder *string  `json:"responder,omitempty"`
		RTTMS     *float64 `json:"rtt_ms,omitempty"`
		ICMPType  *int     `json:"icmp_type,omitempty"`
		ICMPCode  *int     `json:"icmp_code,omitempty"`
	}, 0)
	rows, err := d.readers.QueryContext(ctx, `SELECT ttl, probe_index, responder_address, rtt_ns, icmp_type, icmp_code
        FROM trace_probes WHERE run_id=? ORDER BY ttl, probe_index`, id)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var ttl, index int
		var responder sql.NullString
		var rtt sql.NullInt64
		var icmpType, icmpCode sql.NullInt64
		if err := rows.Scan(&ttl, &index, &responder, &rtt, &icmpType, &icmpCode); err != nil {
			return result, err
		}
		probe := struct {
			TTL       int      `json:"ttl"`
			Index     int      `json:"index"`
			Responder *string  `json:"responder,omitempty"`
			RTTMS     *float64 `json:"rtt_ms,omitempty"`
			ICMPType  *int     `json:"icmp_type,omitempty"`
			ICMPCode  *int     `json:"icmp_code,omitempty"`
		}{TTL: ttl, Index: index}
		if responder.Valid {
			probe.Responder = &responder.String
		}
		if rtt.Valid {
			value := float64(rtt.Int64) / 1e6
			probe.RTTMS = &value
		}
		if icmpType.Valid {
			value := int(icmpType.Int64)
			probe.ICMPType = &value
		}
		if icmpCode.Valid {
			value := int(icmpCode.Int64)
			probe.ICMPCode = &value
		}
		result.Probes = append(result.Probes, probe)
	}
	return result, rows.Err()
}

type RouteChange struct {
	ID            int64           `json:"id"`
	TargetID      string          `json:"target_id"`
	ConfirmedMS   int64           `json:"confirmed_ms"`
	OldRoute      json.RawMessage `json:"old_route"`
	NewRoute      json.RawMessage `json:"new_route"`
	OldReachedHop int             `json:"old_reached_hop"`
	NewReachedHop int             `json:"new_reached_hop"`
}

func (d *DB) RouteChanges(ctx context.Context, stableID string, limit int) ([]RouteChange, error) {
	rows, err := d.readers.QueryContext(ctx, `SELECT rc.id, t.stable_id, rc.confirmed_at_us, rc.old_signature, rc.new_signature,
		rc.old_reached_hop,rc.new_reached_hop
        FROM route_changes rc JOIN targets t ON t.id=rc.target_id
        WHERE (?='' OR t.stable_id=?) ORDER BY rc.confirmed_at_us DESC LIMIT ?`, stableID, stableID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RouteChange, 0)
	for rows.Next() {
		var item RouteChange
		var at int64
		var oldRoute, newRoute string
		if err := rows.Scan(&item.ID, &item.TargetID, &at, &oldRoute, &newRoute, &item.OldReachedHop, &item.NewReachedHop); err != nil {
			return nil, err
		}
		item.ConfirmedMS = at / 1000
		item.OldRoute, item.NewRoute = json.RawMessage(oldRoute), json.RawMessage(newRoute)
		result = append(result, item)
	}
	return result, rows.Err()
}
