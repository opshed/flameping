package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"
)

// RouteHistoryCounts describe retained observations, not expected trace coverage.
// Completed runs are classified by destination reachability; other statuses,
// including timed_out, are counted as errors. The three categories are disjoint.
type RouteHistoryCounts struct {
	Traces    int64 `json:"traces"`
	Reached   int64 `json:"reached"`
	Unreached int64 `json:"unreached"`
	Errors    int64 `json:"errors"`
	Changes   int64 `json:"changes"`
}

type RouteHistoryBucket struct {
	FromMS int64 `json:"from_ms"`
	ToMS   int64 `json:"to_ms"`
	RouteHistoryCounts
}

type RouteHistory struct {
	FromMS           int64                `json:"from_ms"`
	ToMS             int64                `json:"to_ms"`
	BucketMS         int64                `json:"bucket_ms"`
	Totals           RouteHistoryCounts   `json:"totals"`
	Buckets          []RouteHistoryBucket `json:"buckets"`
	Changes          []RouteChange        `json:"changes"`
	Traces           []TraceSummary       `json:"traces"`
	ChangesTruncated bool                 `json:"changes_truncated"`
	TracesTruncated  bool                 `json:"traces_truncated"`
}

// RouteHistory uses a half-open millisecond range shared with the chart viewport.
// Observation times are trace starts; event times are confirmations. Aggregates
// include every retained row in the range even when the latest detail lists are
// capped. A read transaction keeps those counts and lists on one snapshot.
func (d *DB) RouteHistory(ctx context.Context, stableID string, from, to time.Time, maxPoints, limit int) (RouteHistory, error) {
	result := RouteHistory{FromMS: from.UnixMilli(), ToMS: to.UnixMilli()}
	if maxPoints < 1 || maxPoints > 300 || limit < 1 || limit > 500 || !from.Before(to) ||
		to.Sub(from) > 10*365*24*time.Hour || result.ToMS <= result.FromMS ||
		result.FromMS < math.MinInt64/1000 || result.ToMS > math.MaxInt64/1000 {
		return result, errors.New("invalid route history range or limit")
	}
	span := result.ToMS - result.FromMS
	result.BucketMS = (span + int64(maxPoints) - 1) / int64(maxPoints)
	result.Buckets = make([]RouteHistoryBucket, 0, maxPoints)
	for at := result.FromMS; at < result.ToMS; at += result.BucketMS {
		result.Buckets = append(result.Buckets, RouteHistoryBucket{FromMS: at, ToMS: min(at+result.BucketMS, result.ToMS)})
	}
	fromUS, toUS, bucketUS := result.FromMS*1000, result.ToMS*1000, result.BucketMS*1000
	tx, err := d.readers.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	var targetID int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM targets WHERE stable_id=?`, stableID).Scan(&targetID); err != nil {
		return result, err
	}
	if err := routeHistoryTraceBuckets(ctx, tx, targetID, fromUS, toUS, bucketUS, result.Buckets); err != nil {
		return result, err
	}
	if err := routeHistoryChangeBuckets(ctx, tx, targetID, fromUS, toUS, bucketUS, result.Buckets); err != nil {
		return result, err
	}
	for _, bucket := range result.Buckets {
		result.Totals.Traces += bucket.Traces
		result.Totals.Reached += bucket.Reached
		result.Totals.Unreached += bucket.Unreached
		result.Totals.Errors += bucket.Errors
		result.Totals.Changes += bucket.Changes
	}
	rows, err := tx.QueryContext(ctx, traceSummarySelect+`
		WHERE tr.target_id=? AND tr.started_at_us>=? AND tr.started_at_us<?
		ORDER BY tr.started_at_us DESC,tr.id DESC LIMIT ?`, targetID, fromUS, toUS, limit)
	if err != nil {
		return result, err
	}
	if result.Traces, err = scanTraceSummaries(rows); err != nil {
		return result, err
	}
	rows, err = tx.QueryContext(ctx, routeChangeSelect+`
		WHERE rc.target_id=? AND rc.confirmed_at_us>=? AND rc.confirmed_at_us<?
		ORDER BY rc.confirmed_at_us DESC,rc.id DESC LIMIT ?`, targetID, fromUS, toUS, limit)
	if err != nil {
		return result, err
	}
	if result.Changes, err = scanRouteChanges(rows); err != nil {
		return result, err
	}
	result.TracesTruncated = int64(len(result.Traces)) < result.Totals.Traces
	result.ChangesTruncated = int64(len(result.Changes)) < result.Totals.Changes
	return result, tx.Commit()
}

func routeHistoryTraceBuckets(ctx context.Context, tx *sql.Tx, targetID, fromUS, toUS, bucketUS int64, buckets []RouteHistoryBucket) error {
	rows, err := tx.QueryContext(ctx, `SELECT (started_at_us-?)/? AS bucket,
		COUNT(*), SUM(status='completed' AND reached=1), SUM(status='completed' AND reached=0), SUM(status!='completed')
		FROM trace_runs WHERE target_id=? AND started_at_us>=? AND started_at_us<? GROUP BY bucket`,
		fromUS, bucketUS, targetID, fromUS, toUS)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var index int
		var counts RouteHistoryCounts
		if err := rows.Scan(&index, &counts.Traces, &counts.Reached, &counts.Unreached, &counts.Errors); err != nil {
			return err
		}
		buckets[index].RouteHistoryCounts = counts
	}
	return rows.Err()
}

func routeHistoryChangeBuckets(ctx context.Context, tx *sql.Tx, targetID, fromUS, toUS, bucketUS int64, buckets []RouteHistoryBucket) error {
	rows, err := tx.QueryContext(ctx, `SELECT (confirmed_at_us-?)/? AS bucket, COUNT(*)
		FROM route_changes WHERE target_id=? AND confirmed_at_us>=? AND confirmed_at_us<? GROUP BY bucket`,
		fromUS, bucketUS, targetID, fromUS, toUS)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var index int
		var count int64
		if err := rows.Scan(&index, &count); err != nil {
			return err
		}
		buckets[index].Changes = count
	}
	return rows.Err()
}

const traceSummarySelect = `SELECT tr.id, tr.started_at_us, tr.ended_at_us, tr.method,
	tr.status, tr.reached, tr.reached_hop, tr.signature, tr.error_detail, e.address, tr.flow_id
	FROM trace_runs tr JOIN targets t ON t.id=tr.target_id JOIN endpoints e ON e.id=tr.endpoint_id`

func scanTraceSummaries(rows *sql.Rows) ([]TraceSummary, error) {
	defer rows.Close()
	result := make([]TraceSummary, 0)
	for rows.Next() {
		var item TraceSummary
		var started, ended int64
		if err := rows.Scan(&item.ID, &started, &ended, &item.Method, &item.Status, &item.Reached, &item.ReachedHop,
			&item.Signature, &item.ErrorDetail, &item.Endpoint, &item.FlowID); err != nil {
			return nil, err
		}
		item.StartedMS, item.EndedMS = time.UnixMicro(started).UnixMilli(), time.UnixMicro(ended).UnixMilli()
		result = append(result, item)
	}
	return result, rows.Err()
}

const routeChangeSelect = `SELECT rc.id, t.stable_id, rc.first_seen_us, rc.confirmed_at_us,
	rc.old_signature, rc.new_signature, rc.old_reached_hop, rc.new_reached_hop, e.address,
	COALESCE(confirming.method,candidate.method,old.method,''), COALESCE(confirming.flow_id,candidate.flow_id,old.flow_id,''),
	rc.old_trace_id, rc.candidate_trace_id, rc.confirming_trace_id
	FROM route_changes rc JOIN targets t ON t.id=rc.target_id JOIN endpoints e ON e.id=rc.endpoint_id
	LEFT JOIN trace_runs confirming ON confirming.id=rc.confirming_trace_id
	LEFT JOIN trace_runs candidate ON candidate.id=rc.candidate_trace_id
	LEFT JOIN trace_runs old ON old.id=rc.old_trace_id`

func scanRouteChanges(rows *sql.Rows) ([]RouteChange, error) {
	defer rows.Close()
	result := make([]RouteChange, 0)
	for rows.Next() {
		var item RouteChange
		var firstSeen, confirmed int64
		var oldRoute, newRoute string
		if err := rows.Scan(&item.ID, &item.TargetID, &firstSeen, &confirmed, &oldRoute, &newRoute,
			&item.OldReachedHop, &item.NewReachedHop, &item.Endpoint, &item.Method, &item.FlowID,
			&item.OldTraceID, &item.CandidateTraceID, &item.ConfirmingTraceID); err != nil {
			return nil, err
		}
		item.FirstSeenMS, item.ConfirmedMS = time.UnixMicro(firstSeen).UnixMilli(), time.UnixMicro(confirmed).UnixMilli()
		item.OldRoute, item.NewRoute = json.RawMessage(oldRoute), json.RawMessage(newRoute)
		result = append(result, item)
	}
	return result, rows.Err()
}
