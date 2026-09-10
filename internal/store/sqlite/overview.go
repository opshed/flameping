package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Overview is a recent, exact half-open window of retained observations.
// Windows are limited to an hour, inside the minimum 24-hour raw retention.
// Missing observations do not imply successful probes or complete coverage.
type Overview struct {
	AsOfMS     int64               `json:"as_of_ms"`
	FromMS     int64               `json:"from_ms"`
	ToMS       int64               `json:"to_ms"`
	WindowMS   int64               `json:"window_ms"`
	Targets    []OverviewTarget    `json:"targets"`
	Interfaces []OverviewInterface `json:"interfaces"`
}

type OverviewTarget struct {
	TargetSummary
	Recent OverviewPing    `json:"recent"`
	Trend  []OverviewTrend `json:"trend"`
	Route  OverviewRoute   `json:"route"`
}

// Percentages use sent probes, matching PingSeries. Unresolved probes remain
// pending, not unanswered, and make the result provisional until their deadline.
// RTT quantiles come from merged histograms, never averaged bucket quantiles.
type OverviewPing struct {
	Scheduled       int64    `json:"scheduled"`
	Attempted       int64    `json:"attempted"`
	Sent            int64    `json:"sent"`
	Settled         int64    `json:"settled"`
	Pending         int64    `json:"pending"`
	OnTime          int64    `json:"on_time"`
	Late            int64    `json:"late"`
	Unanswered      int64    `json:"unanswered"`
	SendErrors      int64    `json:"send_errors"`
	SchedulerMissed int64    `json:"scheduler_missed"`
	RTTCount        int64    `json:"rtt_count"`
	P50MS           *float64 `json:"p50_ms,omitempty"`
	P95MS           *float64 `json:"p95_ms,omitempty"`
	DeadlineMissPct *float64 `json:"deadline_miss_pct"`
	NoReplyPct      *float64 `json:"no_reply_pct"`
	Partial         bool     `json:"partial"`
}

type OverviewTrend struct {
	FromMS int64 `json:"from_ms"`
	ToMS   int64 `json:"to_ms"`
	OverviewPing
}

type OverviewRoute struct {
	RouteHistoryCounts
	LastTraceAtMS  *int64 `json:"last_trace_at_ms,omitempty"`
	LastChangeAtMS *int64 `json:"last_change_at_ms,omitempty"`
}

// Interface deltas belong to the pair ending in the window; a pair crossing
// its leading edge makes Partial true. Rates are maxima of observed pair rates.
// Diagnostic counters may overlap headline errors and must not be added to them.
type OverviewInterface struct {
	InterfaceSummary
	PeakRXMbps float64 `json:"peak_rx_mbps"`
	PeakTXMbps float64 `json:"peak_tx_mbps"`
	RXErrors   int64   `json:"rx_errors"`
	TXErrors   int64   `json:"tx_errors"`
	RXDropped  int64   `json:"rx_dropped"`
	TXDropped  int64   `json:"tx_dropped"`
	RXMissed   int64   `json:"rx_missed"`
	RXFIFO     int64   `json:"rx_fifo"`
	TXFIFO     int64   `json:"tx_fifo"`
	RXCRC      int64   `json:"rx_crc"`
	RXFrame    int64   `json:"rx_frame"`
	TXCarrier  int64   `json:"tx_carrier"`
	Collisions int64   `json:"collisions"`
	Resets     int64   `json:"resets"`
	HasDeltas  bool    `json:"has_deltas"`
	HasErrors  bool    `json:"has_errors"`
	HasDrops   bool    `json:"has_drops"`
	Partial    bool    `json:"partial"`
}

func (d *DB) Overview(ctx context.Context, asOf time.Time, window time.Duration) (Overview, error) {
	if window != 5*time.Minute && window != 15*time.Minute && window != time.Hour {
		return Overview{}, errors.New("invalid overview window")
	}
	// Millisecond alignment makes the returned bounds exactly reproducible in
	// detailed chart requests rather than silently dropping fractional samples.
	asOf = time.UnixMilli(asOf.UnixMilli())
	from := asOf.Add(-window)
	result := Overview{AsOfMS: asOf.UnixMilli(), FromMS: from.UnixMilli(), ToMS: asOf.UnixMilli(), WindowMS: window.Milliseconds(), Targets: []OverviewTarget{}, Interfaces: []OverviewInterface{}}
	tx, err := d.readers.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	targets, err := queryTargets(ctx, tx, asOf)
	if err != nil {
		return result, err
	}
	width := max(time.Minute, window/15)
	for _, target := range targets {
		item := OverviewTarget{TargetSummary: target}
		if item.Recent, item.Trend, err = overviewPing(ctx, tx, target.ID, from, asOf, width); err != nil {
			return result, err
		}
		if item.Route, err = overviewRoute(ctx, tx, target.ID, from, asOf); err != nil {
			return result, err
		}
		result.Targets = append(result.Targets, item)
	}
	interfaces, err := queryInterfaces(ctx, tx)
	if err != nil {
		return result, err
	}
	for _, summary := range interfaces {
		item, err := overviewInterface(ctx, tx, summary, from, asOf)
		if err != nil {
			return result, err
		}
		result.Interfaces = append(result.Interfaces, item)
	}
	return result, tx.Commit()
}

// Read only clean, complete minute rollups. Exact raw edge minutes and contiguous
// ranges of dirty or absent minutes fill the remainder. Query cost is independent
// of retained historical depth, and a normal hour needs at most 60 rollup rows
// plus two raw edge minutes per target. No raw/rollup observations are duplicated.
func overviewPing(ctx context.Context, tx *sql.Tx, targetID int64, from, to time.Time, width time.Duration) (OverviewPing, []OverviewTrend, error) {
	minute := time.Minute.Microseconds()
	first := from.Truncate(time.Minute)
	cleanFrom := first
	if cleanFrom.Before(from) {
		cleanFrom = cleanFrom.Add(time.Minute)
	}
	cleanTo := to.Truncate(time.Minute)
	dirty := make(map[int64]struct{})
	rows, err := tx.QueryContext(ctx, `SELECT bucket_start_us FROM dirty_rollups
		WHERE kind='ping' AND entity_id=? AND resolution_s=60 AND bucket_start_us>=? AND bucket_start_us<?`, targetID, first.UnixMicro(), to.UnixMicro())
	if err != nil {
		return OverviewPing{}, nil, err
	}
	for rows.Next() {
		var at int64
		if err := rows.Scan(&at); err != nil {
			rows.Close()
			return OverviewPing{}, nil, err
		}
		dirty[at] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return OverviewPing{}, nil, err
	}
	rows.Close()
	minutes, err := queryRollupAggregates(ctx, tx, targetID, cleanFrom, cleanTo, 60, time.Minute, dirty, nil, 0)
	if err != nil {
		return OverviewPing{}, nil, err
	}
	for at := first.UnixMicro(); at < to.UnixMicro(); {
		if _, clean := minutes[at]; clean {
			at += minute
			continue
		}
		start := at
		for at += minute; at < to.UnixMicro(); at += minute {
			if _, clean := minutes[at]; clean {
				break
			}
		}
		raw, err := queryRawAggregates(ctx, tx, targetID, time.UnixMicro(max(start, from.UnixMicro())), time.UnixMicro(min(at, to.UnixMicro())), to, time.Minute, nil)
		if err != nil {
			return OverviewPing{}, nil, err
		}
		for key, value := range raw {
			minutes[key] = value
		}
	}
	combined := make(map[int64]*queryAggregate)
	total := make(map[int64]*queryAggregate)
	for key, value := range minutes {
		mergeQueryAggregate(combined, key-key%width.Microseconds(), value.pingAggregate, value.partial)
		mergeQueryAggregate(total, 0, value.pingAggregate, value.partial)
	}
	trend := make([]OverviewTrend, 0, 16)
	for at := from.Truncate(width); at.Before(to); at = at.Add(width) {
		trend = append(trend, OverviewTrend{FromMS: max(at.UnixMilli(), from.UnixMilli()), ToMS: min(at.Add(width).UnixMilli(), to.UnixMilli()), OverviewPing: overviewPingValue(combined[at.UnixMicro()])})
	}
	return overviewPingValue(total[0]), trend, nil
}

func overviewPingValue(a *queryAggregate) OverviewPing {
	if a == nil {
		return OverviewPing{}
	}
	settled := a.onTime + a.late + a.unanswered
	result := OverviewPing{Scheduled: a.scheduled, Attempted: a.attempted, Sent: a.sent, Settled: settled, Pending: max(0, a.sent-settled), OnTime: a.onTime, Late: a.late, Unanswered: a.unanswered, SendErrors: a.sendError, SchedulerMissed: a.schedulerMissed, RTTCount: a.rttCount, Partial: a.partial || settled < a.sent}
	if a.sent > 0 {
		deadline, noReply := 100*float64(a.late+a.unanswered)/float64(a.sent), 100*float64(a.unanswered)/float64(a.sent)
		result.DeadlineMissPct, result.NoReplyPct = &deadline, &noReply
	}
	if a.rttCount > 0 {
		q := a.hist.Quantiles([]float64{0.50, 0.95})
		p50, p95 := float64(q[0])/1e6, float64(q[1])/1e6
		result.P50MS, result.P95MS = &p50, &p95
	}
	return result
}

func overviewRoute(ctx context.Context, tx *sql.Tx, targetID int64, from, to time.Time) (OverviewRoute, error) {
	var result OverviewRoute
	var traceAt, changeAt sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(status='completed' AND reached=1),0),
		COALESCE(SUM(status='completed' AND reached=0),0), COALESCE(SUM(status!='completed'),0), MAX(started_at_us)
		FROM trace_runs WHERE target_id=? AND started_at_us>=? AND started_at_us<?`, targetID, from.UnixMicro(), to.UnixMicro()).Scan(&result.Traces, &result.Reached, &result.Unreached, &result.Errors, &traceAt)
	if err != nil {
		return result, err
	}
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*), MAX(confirmed_at_us) FROM route_changes WHERE target_id=? AND confirmed_at_us>=? AND confirmed_at_us<?`, targetID, from.UnixMicro(), to.UnixMicro()).Scan(&result.Changes, &changeAt)
	if traceAt.Valid {
		ms := traceAt.Int64 / 1000
		result.LastTraceAtMS = &ms
	}
	if changeAt.Valid {
		ms := changeAt.Int64 / 1000
		result.LastChangeAtMS = &ms
	}
	return result, err
}

func overviewInterface(ctx context.Context, tx *sql.Tx, summary InterfaceSummary, from, to time.Time) (OverviewInterface, error) {
	result := OverviewInterface{InterfaceSummary: summary}
	points := make(map[int64]*InterfacePoint)
	if err := queryInterfaceRaw(ctx, tx, summary.Name, from, to, to.Sub(from), points); err != nil {
		return result, err
	}
	for _, p := range points {
		result.PeakRXMbps = max(result.PeakRXMbps, p.RXMbps)
		result.PeakTXMbps = max(result.PeakTXMbps, p.TXMbps)
		result.RXErrors += p.RXErrors
		result.TXErrors += p.TXErrors
		result.RXDropped += p.RXDropped
		result.TXDropped += p.TXDropped
		result.RXMissed += p.RXMissed
		result.RXFIFO += p.RXFIFO
		result.TXFIFO += p.TXFIFO
		result.RXCRC += p.RXCRC
		result.RXFrame += p.RXFrame
		result.TXCarrier += p.TXCarrier
		result.Collisions += p.Collisions
		result.HasDeltas = result.HasDeltas || p.HasDeltas
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM interface_resets WHERE name=? AND at_us>=? AND at_us<?`, summary.Name, from.UnixMicro(), to.UnixMicro()).Scan(&result.Resets); err != nil {
		return result, err
	}
	// A retained predecessor permits an honest delta but can cross the requested
	// boundary; this is independent of missing/ended interface presence.
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM interface_generations g WHERE g.name=?
		AND EXISTS(SELECT 1 FROM interface_samples s WHERE s.generation_id=g.id AND s.sampled_at_us<?)
		AND EXISTS(SELECT 1 FROM interface_samples s WHERE s.generation_id=g.id AND s.sampled_at_us>=? AND s.sampled_at_us<?))`, summary.Name, from.UnixMicro(), from.UnixMicro(), to.UnixMicro()).Scan(&result.Partial); err != nil {
		return result, err
	}
	result.HasErrors = result.RXErrors > 0 || result.TXErrors > 0 || result.RXFIFO > 0 || result.TXFIFO > 0 || result.RXCRC > 0 || result.RXFrame > 0 || result.TXCarrier > 0 || result.Collisions > 0
	result.HasDrops = result.RXDropped > 0 || result.TXDropped > 0 || result.RXMissed > 0
	return result, nil
}
