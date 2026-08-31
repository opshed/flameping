package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"flameping/internal/model"
)

func insertEndpointChange(ctx context.Context, tx *sql.Tx, change model.EndpointChanged) error {
	if !change.NewAddress.IsValid() || change.TargetID == 0 {
		return fmt.Errorf("invalid endpoint change")
	}
	at := change.ChangedAt
	if at.IsZero() {
		at = time.Now()
	}
	newFamily := 6
	if change.NewAddress.Is4() {
		newFamily = 4
	}
	newID, err := ensureEndpoint(ctx, tx, change.TargetID, addrText(change.NewAddress), newFamily, change.NewAddress.Zone(), at.UnixMicro())
	if err != nil {
		return err
	}
	var oldID any
	if change.OldAddress.IsValid() {
		oldFamily := 6
		if change.OldAddress.Is4() {
			oldFamily = 4
		}
		id, ensureErr := ensureEndpoint(ctx, tx, change.TargetID, addrText(change.OldAddress), oldFamily, change.OldAddress.Zone(), at.UnixMicro())
		if ensureErr != nil {
			return ensureErr
		}
		oldID = id
	}
	detail, _ := json.Marshal(map[string]string{"source": change.ConfiguredBy})
	_, err = tx.ExecContext(ctx, `INSERT INTO target_events(target_id, at_us, kind, old_endpoint_id, new_endpoint_id, detail_json)
		VALUES(?, ?, 'endpoint_changed', ?, ?, ?)`, change.TargetID, at.UnixMicro(), oldID, newID, string(detail))
	return err
}

func insertInterfaceEvent(ctx context.Context, tx *sql.Tx, event model.Event) error {
	item := event.Interface
	if event.Kind == model.EventInterfaceReset {
		var generationID sql.NullInt64
		_ = tx.QueryRowContext(ctx, `SELECT id FROM interface_generations WHERE name=? AND ended_at_us IS NULL ORDER BY started_at_us DESC LIMIT 1`, item.Name).Scan(&generationID)
		if _, err := tx.ExecContext(ctx, `UPDATE interface_generations SET ended_at_us=? WHERE name=? AND ended_at_us IS NULL`, item.SampledAt.UnixMicro(), item.Name); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO interface_resets(generation_id, name, at_us, reason) VALUES(?, ?, ?, ?)`, nullableInt(generationID), item.Name, item.SampledAt.UnixMicro(), item.ResetReason)
		if err != nil {
			return err
		}
		if generationID.Valid {
			return markDirty(ctx, tx, "interface", generationID.Int64, 60, minuteBucket(item.SampledAt))
		}
		return nil
	}
	generationID := item.GenerationID
	if generationID == 0 {
		var err error
		generationID, err = ensureInterfaceGeneration(ctx, tx, item)
		if err != nil {
			return err
		}
	}
	c := item.Counters
	values := []uint64{c.RXBytes, c.TXBytes, c.RXPackets, c.TXPackets, c.RXErrors, c.TXErrors, c.RXDropped, c.TXDropped, c.RXMissed, c.RXFIFO, c.TXFIFO, c.RXCRC, c.RXFrame, c.TXCarrier, c.Collisions}
	for _, value := range values {
		if value > math.MaxInt64 {
			return fmt.Errorf("interface counter exceeds SQLite signed range")
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO interface_samples(
        generation_id, sampled_at_us, rx_bytes, tx_bytes, rx_packets, tx_packets,
        rx_errors, tx_errors, rx_dropped, tx_dropped, rx_missed, rx_fifo, tx_fifo,
        rx_crc, rx_frame, tx_carrier, collisions
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, generationID, item.SampledAt.UnixMicro(),
		c.RXBytes, c.TXBytes, c.RXPackets, c.TXPackets, c.RXErrors, c.TXErrors, c.RXDropped, c.TXDropped,
		c.RXMissed, c.RXFIFO, c.TXFIFO, c.RXCRC, c.RXFrame, c.TXCarrier, c.Collisions)
	if err != nil {
		return err
	}
	return markDirty(ctx, tx, "interface", generationID, 60, minuteBucket(item.SampledAt))
}

func ensureInterfaceGeneration(ctx context.Context, tx *sql.Tx, item model.InterfaceEvent) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM interface_generations
        WHERE boot_id=? AND ifindex=? AND name=? AND mac=? AND ended_at_us IS NULL
        ORDER BY started_at_us DESC LIMIT 1`, item.BootID, item.IfIndex, item.Name, item.MAC).Scan(&id)
	if err == nil {
		var previous [15]int64
		var lastAt int64
		args := make([]any, 1, len(previous)+1)
		args[0] = &lastAt
		for index := range previous {
			args = append(args, &previous[index])
		}
		lastErr := tx.QueryRowContext(ctx, `SELECT sampled_at_us,rx_bytes,tx_bytes,rx_packets,tx_packets,rx_errors,tx_errors,
			rx_dropped,tx_dropped,rx_missed,rx_fifo,tx_fifo,rx_crc,rx_frame,tx_carrier,collisions
			FROM interface_samples WHERE generation_id=? ORDER BY sampled_at_us DESC LIMIT 1`, id).Scan(args...)
		if lastErr == sql.ErrNoRows {
			return id, nil
		}
		if lastErr != nil {
			return 0, lastErr
		}
		current := interfaceCounterValues(item.Counters)
		resetReason := ""
		for index := range previous {
			if current[index] < uint64(previous[index]) {
				resetReason = "counter_decreased_while_stopped"
				break
			}
		}
		if item.SampledAt.UnixMicro() <= lastAt {
			resetReason = "non_monotonic_sample_time"
		}
		if resetReason == "" {
			return id, nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE interface_generations SET ended_at_us=? WHERE id=?`, item.SampledAt.UnixMicro(), id); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO interface_resets(generation_id,name,at_us,reason) VALUES(?,?,?,?)`, id, item.Name, item.SampledAt.UnixMicro(), resetReason); err != nil {
			return 0, err
		}
		if err := markDirty(ctx, tx, "interface", id, 60, minuteBucket(item.SampledAt)); err != nil {
			return 0, err
		}
		err = sql.ErrNoRows
	}
	if err != sql.ErrNoRows {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM interface_generations WHERE name=? AND ended_at_us IS NULL`, item.Name)
	if err != nil {
		return 0, err
	}
	var stale []int64
	for rows.Next() {
		var oldID int64
		if err := rows.Scan(&oldID); err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, oldID)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE interface_generations SET ended_at_us=? WHERE name=? AND ended_at_us IS NULL`, item.SampledAt.UnixMicro(), item.Name); err != nil {
		return 0, err
	}
	for _, oldID := range stale {
		if _, err := tx.ExecContext(ctx, `INSERT INTO interface_resets(generation_id,name,at_us,reason) VALUES(?,?,?,'collector_restart_or_identity_change')`, oldID, item.Name, item.SampledAt.UnixMicro()); err != nil {
			return 0, err
		}
		if err := markDirty(ctx, tx, "interface", oldID, 60, minuteBucket(item.SampledAt)); err != nil {
			return 0, err
		}
	}
	display := item.Name
	result, err := tx.ExecContext(ctx, `INSERT INTO interface_generations(
        boot_id, ifindex, name, display_name, mac, started_at_us
    ) VALUES(?, ?, ?, ?, ?, ?)`, item.BootID, item.IfIndex, item.Name, display, item.MAC, item.SampledAt.UnixMicro())
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func interfaceCounterValues(c model.InterfaceCounters) [15]uint64 {
	return [15]uint64{c.RXBytes, c.TXBytes, c.RXPackets, c.TXPackets, c.RXErrors, c.TXErrors,
		c.RXDropped, c.TXDropped, c.RXMissed, c.RXFIFO, c.TXFIFO, c.RXCRC, c.RXFrame, c.TXCarrier, c.Collisions}
}

func insertTrace(ctx context.Context, tx *sql.Tx, trace model.TraceResult) error {
	endpointID := trace.EndpointID
	if endpointID == 0 {
		if !trace.Endpoint.IsValid() {
			return fmt.Errorf("trace endpoint is invalid")
		}
		family := 6
		if trace.Endpoint.Is4() {
			family = 4
		}
		var err error
		endpointID, err = ensureEndpoint(ctx, tx, trace.TargetID, addrText(trace.Endpoint), family, trace.Endpoint.Zone(), trace.StartedAt.UnixMicro())
		if err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO trace_runs(
        target_id, endpoint_id, method, flow_id, started_at_us, ended_at_us,
        status, reached, reached_hop, signature, error_detail
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, trace.TargetID, endpointID, trace.Method, trace.FlowID,
		trace.StartedAt.UnixMicro(), trace.EndedAt.UnixMicro(), trace.Status, trace.Reached, trace.ReachedHop, trace.Signature, trace.ErrorDetail)
	if err != nil {
		return err
	}
	runID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO trace_probes(
        run_id, ttl, probe_index, token, sent_at_us, reply_at_us,
        responder_address, rtt_ns, icmp_type, icmp_code
    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, probe := range trace.Probes {
		var replyAt, responder, rtt, icmpType, icmpCode any
		if !probe.ReplyAt.IsZero() {
			replyAt, responder, rtt, icmpType, icmpCode = probe.ReplyAt.UnixMicro(), probe.Responder.String(), probe.RTT.Nanoseconds(), probe.ICMPType, probe.ICMPCode
		}
		if _, err := stmt.ExecContext(ctx, runID, probe.TTL, probe.Index, probe.Token, probe.SentAt.UnixMicro(), replyAt, responder, rtt, icmpType, icmpCode); err != nil {
			return err
		}
	}
	trace.EndpointID = endpointID
	return updateRouteCandidate(ctx, tx, runID, trace)
}

func updateRouteCandidate(ctx context.Context, tx *sql.Tx, runID int64, trace model.TraceResult) error {
	if trace.Status != "completed" || trace.Signature == "" {
		return nil
	}
	var currentID, candidateID sql.NullInt64
	var candidate sql.NullString
	var candidateReached sql.NullInt64
	var currentSignature, currentMethod, currentFlow sql.NullString
	var currentReached sql.NullInt64
	var currentEndpoint int64
	var consecutive int
	err := tx.QueryRowContext(ctx, `SELECT current_trace_id, candidate_trace_id, candidate_signature, candidate_reached_hop, consecutive_count,
		endpoint_id, current_signature, current_reached_hop, current_method, current_flow_id
		FROM route_candidates WHERE target_id=?`, trace.TargetID).Scan(&currentID, &candidateID, &candidate, &candidateReached, &consecutive,
		&currentEndpoint, &currentSignature, &currentReached, &currentMethod, &currentFlow)
	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, `INSERT INTO route_candidates(target_id, endpoint_id, current_trace_id, consecutive_count,
			current_signature, current_reached_hop, current_method, current_flow_id) VALUES(?, ?, ?, 0, ?, ?, ?, ?)`,
			trace.TargetID, trace.EndpointID, runID, trace.Signature, trace.ReachedHop, trace.Method, trace.FlowID)
		return err
	}
	if err != nil {
		return err
	}
	if !currentSignature.Valid || currentEndpoint != trace.EndpointID || currentMethod.String != trace.Method || currentFlow.String != trace.FlowID {
		_, err = tx.ExecContext(ctx, `UPDATE route_candidates SET endpoint_id=?, current_trace_id=?, current_signature=?,
			current_reached_hop=?, current_method=?, current_flow_id=?, candidate_trace_id=NULL,
			candidate_signature=NULL, candidate_reached_hop=NULL, consecutive_count=0 WHERE target_id=?`, trace.EndpointID, runID, trace.Signature,
			trace.ReachedHop, trace.Method, trace.FlowID, trace.TargetID)
		return err
	}
	if trace.Signature == currentSignature.String && trace.ReachedHop == int(currentReached.Int64) {
		_, err = tx.ExecContext(ctx, `UPDATE route_candidates SET candidate_trace_id=NULL, candidate_signature=NULL, candidate_reached_hop=NULL, consecutive_count=0, endpoint_id=? WHERE target_id=?`, trace.EndpointID, trace.TargetID)
		return err
	}
	if trace.ReachedHop == int(currentReached.Int64) && !routeMeaningfullyDifferent(currentSignature.String, trace.Signature) {
		_, err = tx.ExecContext(ctx, `UPDATE route_candidates SET candidate_trace_id=NULL, candidate_signature=NULL, candidate_reached_hop=NULL, consecutive_count=0, endpoint_id=? WHERE target_id=?`, trace.EndpointID, trace.TargetID)
		return err
	}
	if !candidate.Valid || candidate.String != trace.Signature || !candidateReached.Valid || int(candidateReached.Int64) != trace.ReachedHop {
		_, err = tx.ExecContext(ctx, `UPDATE route_candidates SET candidate_trace_id=?, candidate_signature=?, candidate_reached_hop=?, consecutive_count=1, endpoint_id=? WHERE target_id=?`, runID, trace.Signature, trace.ReachedHop, trace.EndpointID, trace.TargetID)
		return err
	}
	if consecutive+1 < 2 {
		_, err = tx.ExecContext(ctx, `UPDATE route_candidates SET consecutive_count=consecutive_count+1 WHERE target_id=?`, trace.TargetID)
		return err
	}
	var firstSeen int64
	if candidateID.Valid {
		_ = tx.QueryRowContext(ctx, `SELECT started_at_us FROM trace_runs WHERE id=?`, candidateID.Int64).Scan(&firstSeen)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO route_changes(
        target_id, endpoint_id, old_trace_id, candidate_trace_id, confirming_trace_id,
        old_signature, new_signature, first_seen_us, confirmed_at_us, old_reached_hop, new_reached_hop
    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, trace.TargetID, trace.EndpointID, nullableInt(currentID), nullableInt(candidateID), runID,
		currentSignature.String, trace.Signature, firstSeen, trace.EndedAt.UnixMicro(), currentReached.Int64, trace.ReachedHop)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE route_candidates SET current_trace_id=?, current_signature=?, current_reached_hop=?,
		current_method=?, current_flow_id=?, candidate_trace_id=NULL, candidate_signature=NULL, candidate_reached_hop=NULL, consecutive_count=0 WHERE target_id=?`,
		runID, trace.Signature, trace.ReachedHop, trace.Method, trace.FlowID, trace.TargetID)
	return err
}

func routeMeaningfullyDifferent(oldSignature, newSignature string) bool {
	if oldSignature == "" || newSignature == "" || oldSignature == newSignature {
		return false
	}
	var oldHops, newHops []struct {
		TTL        int      `json:"ttl"`
		Responders []string `json:"responders"`
	}
	if json.Unmarshal([]byte(oldSignature), &oldHops) != nil || json.Unmarshal([]byte(newSignature), &newHops) != nil {
		return false
	}
	oldByTTL := make(map[int]map[string]struct{}, len(oldHops))
	for _, hop := range oldHops {
		set := make(map[string]struct{}, len(hop.Responders))
		for _, responder := range hop.Responders {
			set[responder] = struct{}{}
		}
		oldByTTL[hop.TTL] = set
	}
	for _, hop := range newHops {
		oldSet := oldByTTL[hop.TTL]
		if len(oldSet) == 0 || len(hop.Responders) == 0 {
			continue
		}
		overlap := false
		for _, responder := range hop.Responders {
			if _, ok := oldSet[responder]; ok {
				overlap = true
				break
			}
		}
		if !overlap {
			return true
		}
	}
	return false
}

func nullableInt(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}
