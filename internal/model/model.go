package model

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"time"
)

type RunID [8]byte

func (r RunID) String() string { return hex.EncodeToString(r[:]) }
func (r RunID) Bytes() []byte {
	b := make([]byte, len(r))
	copy(b, r[:])
	return b
}

func RunIDFromBytes(b []byte) (RunID, error) {
	var id RunID
	if len(b) != len(id) {
		return id, fmt.Errorf("run id length %d; want %d", len(b), len(id))
	}
	copy(id[:], b)
	return id, nil
}

type ProbeKey struct {
	RunID    RunID
	Sequence uint64
}

func (k ProbeKey) String() string { return fmt.Sprintf("%s/%d", k.RunID, k.Sequence) }

type ReplyClass string

const (
	ReplyOnTime ReplyClass = "on_time"
	ReplyLate   ReplyClass = "late"
)

type EventKind uint8

const (
	EventProbeSent EventKind = iota + 1
	EventProbeSendError
	EventProbeReply
	EventSchedulerGap
	EventEndpointChanged
	EventInterfaceSnapshot
	EventInterfaceReset
	EventTraceCompleted
)

type Event struct {
	Kind      EventKind
	Probe     ProbeEvent
	Gap       SchedulerGap
	Endpoint  EndpointChanged
	Interface InterfaceEvent
	Trace     TraceResult
}

type ProbeEvent struct {
	Key              ProbeKey
	TargetID         int64
	EndpointID       int64
	Endpoint         netip.Addr
	ScheduledAt      time.Time
	SentAt           time.Time
	Timeout          time.Duration
	ReplyAt          time.Time
	RTT              time.Duration
	ReplyClass       ReplyClass
	Responder        netip.Addr
	ICMPType         int
	ICMPCode         int
	SendErrorCode    string
	SendErrorMessage string
}

type SchedulerGap struct {
	TargetID       int64
	FirstScheduled time.Time
	Interval       time.Duration
	MissedCount    uint64
}

type EndpointChanged struct {
	TargetID     int64
	OldEndpoint  int64
	NewEndpoint  int64
	OldAddress   netip.Addr
	NewAddress   netip.Addr
	ChangedAt    time.Time
	ConfiguredBy string
}

type InterfaceCounters struct {
	RXBytes    uint64
	TXBytes    uint64
	RXPackets  uint64
	TXPackets  uint64
	RXErrors   uint64
	TXErrors   uint64
	RXDropped  uint64
	TXDropped  uint64
	RXMissed   uint64
	RXFIFO     uint64
	TXFIFO     uint64
	RXCRC      uint64
	RXFrame    uint64
	TXCarrier  uint64
	Collisions uint64
}

type InterfaceEvent struct {
	GenerationID int64
	Name         string
	BootID       string
	IfIndex      int
	MAC          string
	SampledAt    time.Time
	Counters     InterfaceCounters
	ResetReason  string
}

type TraceProbe struct {
	TTL       int
	Index     int
	Token     uint16
	SentAt    time.Time
	ReplyAt   time.Time
	Responder netip.Addr
	RTT       time.Duration
	ICMPType  int
	ICMPCode  int
}

type TraceResult struct {
	TargetID    int64
	EndpointID  int64
	Endpoint    netip.Addr
	Method      string
	FlowID      string
	StartedAt   time.Time
	EndedAt     time.Time
	Status      string
	ErrorDetail string
	Reached     bool
	ReachedHop  int
	Signature   string
	Probes      []TraceProbe
}
