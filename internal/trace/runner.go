package trace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
)

type Target struct {
	ID       int64
	StableID string
	Endpoint netip.Addr
}

type Runner struct {
	v4       *rawReceiver
	v6       *rawReceiver
	v4Error  string
	v6Error  string
	cfg      config.TracerouteConfig
	rateMu   sync.Mutex
	nextSend time.Time
}

func OpenRunner(cfg config.TracerouteConfig) (*Runner, []error) {
	runner := &Runner{cfg: cfg}
	var errs []error
	var err error
	runner.v4, err = openRawReceiver(4)
	if err != nil {
		runner.v4Error = err.Error()
		errs = append(errs, fmt.Errorf("IPv4 traceroute: %w", err))
	}
	runner.v6, err = openRawReceiver(6)
	if err != nil {
		runner.v6Error = err.Error()
		errs = append(errs, fmt.Errorf("IPv6 traceroute: %w", err))
	}
	return runner, errs
}

// Capabilities reports why a family is unavailable as well as successful
// availability. Callers expose this through status instead of making a
// missing trace target look like an empty history.
func (r *Runner) Capabilities() map[string]string {
	result := map[string]string{"ipv4": "available", "ipv6": "available"}
	if r.v4 == nil {
		result["ipv4"] = "unavailable: " + r.v4Error
	}
	if r.v6 == nil {
		result["ipv6"] = "unavailable: " + r.v6Error
	}
	return result
}

func (r *Runner) Available(endpoint netip.Addr) bool {
	if endpoint.Is4() {
		return r.v4 != nil
	}
	return r.v6 != nil
}

func (r *Runner) Trace(ctx context.Context, target Target) (model.TraceResult, error) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.OverallTimeout.Value())
	defer cancel()
	if r.cfg.Method != "auto" {
		return r.traceOnce(ctx, target, r.cfg.Method)
	}
	parisBudget := r.cfg.OverallTimeout.Value() / 2
	if parisBudget < r.cfg.HopTimeout.Value() {
		parisBudget = r.cfg.HopTimeout.Value()
	}
	parisCtx, cancelParis := context.WithTimeout(ctx, parisBudget)
	parisResult, parisErr := r.traceOnce(parisCtx, target, "paris-udp")
	cancelParis()
	if parisErr == nil && traceHasReply(parisResult) {
		return parisResult, nil
	}
	classicResult, classicErr := r.traceOnce(ctx, target, "classic-udp")
	if classicErr != nil && parisErr != nil {
		return classicResult, fmt.Errorf("paris-udp failed: %v; classic-udp failed: %w", parisErr, classicErr)
	}
	return classicResult, classicErr
}

func traceHasReply(result model.TraceResult) bool {
	for _, probe := range result.Probes {
		if probe.Responder.IsValid() {
			return true
		}
	}
	return false
}

func (r *Runner) traceOnce(ctx context.Context, target Target, method string) (model.TraceResult, error) {
	started := time.Now()
	result := model.TraceResult{TargetID: target.ID, Endpoint: target.Endpoint, Method: method, StartedAt: started, Status: "error"}
	receiver := r.v6
	network := "udp6"
	if target.Endpoint.Is4() {
		receiver, network = r.v4, "udp4"
	}
	if receiver == nil {
		return result, fmt.Errorf("raw ICMP receiver is unavailable for %s", target.Endpoint)
	}
	basePort, sourcePort := stableFlowPorts(target.StableID, r.cfg.MaxHops*r.cfg.ProbesPerHop+1)
	source, err := routedSource(network, target.Endpoint, basePort)
	if err != nil {
		return result, err
	}
	local := &net.UDPAddr{IP: net.IP(source.AsSlice()), Zone: source.Zone(), Port: sourcePort}
	udp, err := net.ListenUDP(network, local)
	if err != nil {
		return result, err
	}
	defer udp.Close()
	boundSourcePort := uint16(udp.LocalAddr().(*net.UDPAddr).Port)
	result.FlowID = source.String() + ":" + strconv.Itoa(int(boundSourcePort)) + "->" + target.Endpoint.String() + ":" + strconv.Itoa(basePort)

	runCtx, cancel := context.WithTimeout(ctx, r.cfg.OverallTimeout.Value())
	defer cancel()
	probes := make([]model.TraceProbe, 0, r.cfg.MaxHops*r.cfg.ProbesPerHop)
	reachedHop := 0
	sequence := 0
	for firstTTL := 1; firstTTL <= r.cfg.MaxHops && reachedHop == 0; firstTTL += r.cfg.PipelineHops {
		outstanding := make(map[uint16]int)
		lastTTL := min(firstTTL+r.cfg.PipelineHops-1, r.cfg.MaxHops)
		for ttl := firstTTL; ttl <= lastTTL; ttl++ {
			for index := 0; index < r.cfg.ProbesPerHop; index++ {
				select {
				case <-runCtx.Done():
					result.Status = "timed_out"
					result.Probes = probes
					result.EndedAt = time.Now()
					result.Signature = routeSignature(probes)
					return result, nil
				default:
				}
				if err := r.waitRate(runCtx); err != nil {
					result.Status = "timed_out"
					result.Probes = probes
					result.EndedAt = time.Now()
					result.Signature = routeSignature(probes)
					return result, nil
				}
				sequence++
				token := uint16(sequence)
				destinationPort := uint16(basePort)
				payload := []byte{'F', 'L', 'M', 'P', byte(ttl), byte(index)}
				if method == "classic-udp" {
					destinationPort += token
				} else {
					payload, err = parisPayload(source, target.Endpoint, boundSourcePort, destinationPort, token)
					if err != nil {
						return result, err
					}
				}
				if err := setTTL(udp, target.Endpoint, ttl); err != nil {
					return result, err
				}
				sentAt := time.Now()
				_, err := udp.WriteToUDP(payload, &net.UDPAddr{IP: net.IP(target.Endpoint.AsSlice()), Zone: target.Endpoint.Zone(), Port: int(destinationPort)})
				if err != nil {
					return result, err
				}
				probe := model.TraceProbe{TTL: ttl, Index: index, Token: token, SentAt: sentAt}
				probes = append(probes, probe)
				match := destinationPort
				if method != "classic-udp" {
					match = token
				}
				outstanding[match] = len(probes) - 1
			}
		}
		deadline := time.Now().Add(r.cfg.HopTimeout.Value())
		if overall, ok := runCtx.Deadline(); ok && overall.Before(deadline) {
			deadline = overall
		}
		for len(outstanding) > 0 && time.Now().Before(deadline) {
			response, err := receiver.read(deadline)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					break
				}
				if runCtx.Err() != nil {
					break
				}
				return result, err
			}
			if response.sourcePort != boundSourcePort {
				continue
			}
			match := response.destinationPort
			if method != "classic-udp" {
				match = response.checksum
			}
			probeIndex, ok := outstanding[match]
			if !ok {
				continue
			}
			probes[probeIndex].ReplyAt = time.Now()
			probes[probeIndex].Responder = response.source
			probes[probeIndex].RTT = time.Since(probes[probeIndex].SentAt)
			probes[probeIndex].ICMPType = response.icmpType
			probes[probeIndex].ICMPCode = response.icmpCode
			delete(outstanding, match)
			if response.reached && response.source.Unmap() == target.Endpoint.Unmap() {
				if reachedHop == 0 || probes[probeIndex].TTL < reachedHop {
					reachedHop = probes[probeIndex].TTL
				}
			}
		}
	}
	result.Probes = probes
	result.Reached = reachedHop > 0
	result.ReachedHop = reachedHop
	result.Status = "completed"
	result.EndedAt = time.Now()
	result.Signature = routeSignature(probes)
	return result, nil
}

func (r *Runner) waitRate(ctx context.Context) error {
	interval := time.Second / time.Duration(r.cfg.MaxPacketsPerSecond)
	r.rateMu.Lock()
	sendAt := time.Now()
	if r.nextSend.After(sendAt) {
		sendAt = r.nextSend
	}
	r.nextSend = sendAt.Add(interval)
	r.rateMu.Unlock()
	wait := time.Until(sendAt)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func stableFlowPorts(id string, span int) (int, int) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	value := h.Sum64()
	maxBase := 65535 - span
	base := 33434
	if maxBase > base {
		base += int(value % uint64(maxBase-base))
	}
	source := 40000 + int((value>>32)%20000)
	return base, source
}

func setTTL(conn *net.UDPConn, target netip.Addr, ttl int) error {
	if target.Is4() {
		return ipv4.NewPacketConn(conn).SetTTL(ttl)
	}
	return ipv6.NewPacketConn(conn).SetHopLimit(ttl)
}

func routedSource(network string, target netip.Addr, port int) (netip.Addr, error) {
	conn, err := net.DialUDP(network, nil, &net.UDPAddr{IP: net.IP(target.AsSlice()), Zone: target.Zone(), Port: port})
	if err != nil {
		return netip.Addr{}, err
	}
	defer conn.Close()
	local := conn.LocalAddr().(*net.UDPAddr)
	addr, ok := netip.AddrFromSlice(local.IP)
	if !ok {
		return netip.Addr{}, errors.New("could not determine routed source address")
	}
	addr = addr.Unmap()
	if local.Zone != "" {
		addr = addr.WithZone(local.Zone)
	}
	return addr, nil
}

type signatureHop struct {
	TTL        int      `json:"ttl"`
	Responders []string `json:"responders"`
}

func routeSignature(probes []model.TraceProbe) string {
	sets := make(map[int]map[string]struct{})
	for _, probe := range probes {
		if !probe.Responder.IsValid() {
			continue
		}
		if sets[probe.TTL] == nil {
			sets[probe.TTL] = make(map[string]struct{})
		}
		sets[probe.TTL][probe.Responder.String()] = struct{}{}
	}
	hops := make([]signatureHop, 0, len(sets))
	for ttl, set := range sets {
		responders := make([]string, 0, len(set))
		for responder := range set {
			responders = append(responders, responder)
		}
		sort.Strings(responders)
		hops = append(hops, signatureHop{TTL: ttl, Responders: responders})
	}
	sort.Slice(hops, func(i, j int) bool { return hops[i].TTL < hops[j].TTL })
	data, _ := json.Marshal(hops)
	return string(data)
}

func (r *Runner) Close() error {
	var errs []error
	if r.v4 != nil {
		errs = append(errs, r.v4.Close())
	}
	if r.v6 != nil {
		errs = append(errs, r.v6.Close())
	}
	return errors.Join(errs...)
}
