package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flameping/internal/config"
	"flameping/internal/eventbus"
	"flameping/internal/histogram"
	"flameping/internal/model"
)

func openTestDB(t *testing.T) (*DB, config.Config, Target, model.RunID) {
	t.Helper()
	cfg := config.Defaults()
	cfg.Storage.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.Storage.MaxBytes = config.Bytes(128 << 20)
	cfg.Storage.FreeSpaceReserve = 0
	db, err := Open(context.Background(), cfg.Storage)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cfg.Targets = []config.TargetConfig{{ID: "loopback", Name: "Loopback", Address: "127.0.0.1"}}
	targets, err := db.SyncTargets(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var runID model.RunID
	if _, err := rand.Read(runID[:]); err != nil {
		t.Fatal(err)
	}
	if err := db.StartRun(context.Background(), runID, "test-boot", "test"); err != nil {
		t.Fatal(err)
	}
	return db, cfg, targets[0], runID
}

func TestWriterBuffersReplyBeforeSent(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	bus := eventbus.New(16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- db.RunWriter(ctx, bus) }()

	key := model.ProbeKey{RunID: runID, Sequence: 1}
	sent := time.Now().Add(-2 * time.Second)
	reply := model.Event{Kind: model.EventProbeReply, Probe: model.ProbeEvent{
		Key: key, ReplyAt: sent.Add(1500 * time.Millisecond), RTT: 1500 * time.Millisecond,
		ReplyClass: model.ReplyLate, Responder: netip.MustParseAddr("127.0.0.1"),
	}}
	if err := bus.Publish(ctx, reply); err != nil {
		t.Fatal(err)
	}
	probe := model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{
		Key: key, TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"),
		ScheduledAt: sent, SentAt: sent, Timeout: time.Second,
	}}
	if err := bus.Publish(ctx, probe); err != nil {
		t.Fatal(err)
	}
	bus.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	var class string
	var rtt int64
	if err := db.readers.QueryRow(`SELECT reply_class, rtt_ns FROM ping_samples WHERE run_id=? AND sequence=1`, runID.Bytes()).Scan(&class, &rtt); err != nil {
		t.Fatal(err)
	}
	if class != "late" || rtt != (1500*time.Millisecond).Nanoseconds() {
		t.Fatalf("reply = %s, %d", class, rtt)
	}
}

func TestRollupIncludesUnansweredAndSchedulerGap(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	bucket := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	tx, err := db.writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	orphans := make(map[model.ProbeKey]orphanReply)
	probe := model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{
		Key: model.ProbeKey{RunID: runID, Sequence: 2}, TargetID: target.ID,
		Endpoint: netip.MustParseAddr("127.0.0.1"), ScheduledAt: bucket.Add(time.Second),
		SentAt: bucket.Add(time.Second), Timeout: time.Second,
	}}
	if err := insertProbe(ctx, tx, probe, orphans); err != nil {
		t.Fatal(err)
	}
	if err := insertGap(ctx, tx, model.SchedulerGap{TargetID: target.ID, FirstScheduled: bucket.Add(5 * time.Second), Interval: 5 * time.Second, MissedCount: 2}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.recomputeDirty(ctx, 32, time.Now()); err != nil {
		t.Fatal(err)
	}
	var scheduled, unanswered, missed int
	if err := db.readers.QueryRow(`SELECT scheduled_count, unanswered_count, scheduler_missed_count
        FROM ping_rollups WHERE target_id=? AND resolution_s=60 AND bucket_start_us=?`, target.ID, bucket.UnixMicro()).Scan(&scheduled, &unanswered, &missed); err != nil {
		t.Fatal(err)
	}
	if scheduled != 3 || unanswered != 1 || missed != 2 {
		t.Fatalf("rollup scheduled=%d unanswered=%d missed=%d", scheduled, unanswered, missed)
	}
}

func TestMigrationChecksumAndSQLiteVersion(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	if !versionAtLeast("3.53.3", 3, 51, 3) || versionAtLeast("3.51.2", 3, 51, 3) {
		t.Fatal("version comparison is incorrect")
	}
	if err := migrate(context.Background(), db.writer); err != nil {
		t.Fatalf("idempotent migrate: %v", err)
	}
}

func TestPruneStatementsOnEmptyDatabase(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	if err := db.prune(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.maintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupCheckpointsCrashLeftWALBeforeRejectingBudget(t *testing.T) {
	const helperPath = "FLAMEPING_TEST_CRASH_WAL_PATH"
	if path := os.Getenv(helperPath); path != "" {
		cfg := config.Defaults().Storage
		cfg.Path = path
		cfg.MaxBytes = config.Bytes(128 << 20)
		cfg.FreeSpaceReserve = 0
		db, err := OpenMaintenance(context.Background(), cfg)
		if err == nil {
			_, err = db.writer.Exec(`CREATE TABLE wal_fixture(id INTEGER PRIMARY KEY, value BLOB NOT NULL)`)
		}
		if err == nil {
			_, err = db.writer.Exec(`INSERT INTO wal_fixture(id,value) VALUES(1,randomblob(262144))`)
		}
		if err == nil {
			err = db.checkpoint(context.Background(), "TRUNCATE")
		}
		for i := 0; err == nil && i < 48; i++ {
			_, err = db.writer.Exec(`UPDATE wal_fixture SET value=randomblob(262144) WHERE id=1`)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		// Deliberately bypass Close: this is the crash condition under test.
		os.Exit(0)
	}

	path := filepath.Join(t.TempDir(), "crash.db")
	command := exec.Command(os.Args[0], "-test.run=^TestStartupCheckpointsCrashLeftWALBeforeRejectingBudget$")
	command.Env = append(os.Environ(), helperPath+"="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create crash-left WAL: %v: %s", err, output)
	}
	walBefore, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	const budget = int64(4 << 20)
	if walBefore.Size() < budget {
		t.Fatalf("test WAL is only %d bytes; want at least %d", walBefore.Size(), budget)
	}

	cfg := config.Defaults().Storage
	cfg.Path = path
	cfg.MaxBytes = config.Bytes(budget)
	cfg.FreeSpaceReserve = 0
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open should recover the oversized crash-left WAL: %v", err)
	}
	defer db.Close()
	walAfter, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if walAfter.Size() >= walBefore.Size() {
		t.Fatalf("startup checkpoint did not shrink WAL: before=%d after=%d", walBefore.Size(), walAfter.Size())
	}
}

func TestCapacityFullRetainsBatchAndRecoversBySafePruning(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	tx, err := db.writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	endpointID, err := ensureEndpoint(ctx, tx, target.ID, "127.0.0.1", 4, "", time.Now().UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	oldDay := time.Now().Add(-48 * time.Hour).Truncate(24 * time.Hour)
	for i := 0; i < 32; i++ {
		started := oldDay.Add(time.Duration(i) * time.Second)
		if _, err := db.writer.ExecContext(ctx, `INSERT INTO trace_runs(target_id,endpoint_id,method,flow_id,started_at_us,ended_at_us,status,reached,reached_hop,signature,error_detail)
			VALUES(?,?,'udp','fixture',?,?,'error',0,0,'[]',zeroblob(262144))`, target.ID, endpointID, started.UnixMicro(), started.Add(time.Second).UnixMicro()); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.checkpoint(ctx, "TRUNCATE"); err != nil {
		t.Fatal(err)
	}
	var pages, freePages int64
	if err := db.writer.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := db.writer.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
		t.Fatal(err)
	}
	if freePages != 0 {
		t.Fatalf("fixture unexpectedly has %d reusable pages", freePages)
	}
	var applied int64
	if err := db.writer.QueryRowContext(ctx, fmt.Sprintf(`PRAGMA max_page_count=%d`, pages)).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != pages {
		t.Fatalf("page limit=%d, want %d", applied, pages)
	}

	traceAt := time.Now()
	events := []model.Event{{Kind: model.EventTraceCompleted, Trace: model.TraceResult{
		TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"), Method: "udp", FlowID: "retained",
		StartedAt: traceAt, EndedAt: traceAt.Add(time.Second), Status: "error", Signature: "[]", ErrorDetail: strings.Repeat("x", 512<<10),
	}}}
	orphans := make(map[model.ProbeKey]orphanReply)
	if err := db.applyRetainedBatch(ctx, events, &orphans); !isSQLiteFull(err) {
		t.Fatalf("fixture did not reach SQLITE_FULL: %v", err)
	}
	if err := db.applyBatchRecovering(ctx, events, &orphans); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := db.readers.QueryRowContext(ctx, `SELECT COUNT(*) FROM trace_runs WHERE flow_id='retained'`).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("retained batch rows=%d, want 1", retained)
	}
}

func TestReplyForEvictedCommittedProbeIsDiagnostic(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	key := model.ProbeKey{RunID: runID, Sequence: 44}
	sentAt := time.Now().Add(-48 * time.Hour)
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventProbeSent, Probe: model.ProbeEvent{Key: key, TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"), ScheduledAt: sentAt, SentAt: sentAt, Timeout: time.Second}}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.ExecContext(ctx, `DELETE FROM ping_samples WHERE run_id=? AND sequence=?`, runID.Bytes(), 44); err != nil {
		t.Fatal(err)
	}
	db.prunedThrough.Store(time.Now().Add(-24 * time.Hour).UnixMicro())
	orphans := make(map[model.ProbeKey]orphanReply)
	reply := model.Event{Kind: model.EventProbeReply, Probe: model.ProbeEvent{Key: key, SentAt: sentAt, Responder: netip.MustParseAddr("127.0.0.1"), ReplyAt: time.Now(), RTT: 48 * time.Hour, ReplyClass: model.ReplyLate}}
	if err := db.applyBatch(ctx, []model.Event{reply}, orphans); err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatal("evicted reply was incorrectly buffered")
	}
	var value int
	if err := db.readers.QueryRow(`SELECT value FROM diagnostics WHERE name='late_reply_evicted'`).Scan(&value); err != nil || value != 1 {
		t.Fatalf("diagnostic=%d err=%v", value, err)
	}
}

func TestDirtyOverlayPreservesCleanSiblingRollup(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	start := time.Now().Add(-10 * 24 * time.Hour).Truncate(2 * time.Minute)
	h := histogram.New()
	h.Observe(uint64((10 * time.Millisecond).Nanoseconds()))
	blob, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for i, count := range []int64{5, 9} {
		bucket := start.Add(time.Duration(i) * time.Minute)
		_, err = db.writer.ExecContext(ctx, `INSERT INTO ping_rollups(target_id,resolution_s,bucket_start_us,scheduled_count,attempted_count,sent_count,on_time_count,late_count,unanswered_count,send_error_count,scheduler_missed_count,rtt_count,rtt_sum_ns,rtt_min_ns,rtt_max_ns,histogram,updated_at_us,timeout_min_ns,timeout_max_ns) VALUES(?,60,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, target.ID, bucket.UnixMicro(), count, count, count, count, 0, 0, 0, 0, count, count*int64(10*time.Millisecond), int64(10*time.Millisecond), int64(10*time.Millisecond), blob, time.Now().UnixMicro(), int64(time.Second), int64(time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
	probeAt := start.Add(time.Minute + 5*time.Second)
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventProbeSent, Probe: model.ProbeEvent{Key: model.ProbeKey{RunID: runID, Sequence: 88}, TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"), ScheduledAt: probeAt, SentAt: probeAt, Timeout: time.Second}}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	series, err := db.PingSeries(ctx, target.StableID, start, start.Add(4*time.Minute), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Points) == 0 || series.Points[0].Scheduled != 6 {
		t.Fatalf("points=%+v, want first scheduled=6", series.Points)
	}
}

func TestSeriesResolutionDistinguishesNewEntitiesFromPrunedHistory(t *testing.T) {
	minute := int64(time.Minute / time.Microsecond)
	hour := int64(time.Hour / time.Microsecond)
	valid := func(value int64) sql.NullInt64 { return sql.NullInt64{Int64: value, Valid: true} }
	tests := []struct {
		name          string
		desired       time.Duration
		coverageStart int64
		rawStart      sql.NullInt64
		minuteStart   sql.NullInt64
		hourStart     sql.NullInt64
		wantSource    int64
		wantWidth     time.Duration
	}{
		{
			name:          "window predates newly monitored entity",
			desired:       5 * time.Minute,
			coverageStart: 6 * hour,
			rawStart:      valid(6*hour + 5_000_000),
			minuteStart:   valid(6 * hour),
			hourStart:     valid(6 * hour),
			wantSource:    60,
			wantWidth:     5 * time.Minute,
		},
		{
			name:          "hour tier covers genuinely pruned history",
			desired:       5 * time.Minute,
			coverageStart: 6 * hour,
			rawStart:      valid(8 * hour),
			minuteStart:   valid(7 * hour),
			hourStart:     valid(6 * hour),
			wantSource:    3600,
			wantWidth:     time.Hour,
		},
		{
			name:          "minute tier covers pruned raw history",
			desired:       5 * time.Minute,
			coverageStart: 6 * hour,
			rawStart:      valid(7 * hour),
			minuteStart:   valid(6*hour - minute),
			wantSource:    60,
			wantWidth:     5 * time.Minute,
		},
		{
			name:          "later hour tier does not hide covering minutes",
			desired:       time.Hour,
			coverageStart: 6 * hour,
			rawStart:      valid(7 * hour),
			minuteStart:   valid(6 * hour),
			hourStart:     valid(7 * hour),
			wantSource:    60,
			wantWidth:     time.Hour,
		},
		{
			name:          "later minute tier does not hide covering raw data",
			desired:       5 * time.Minute,
			coverageStart: 6 * hour,
			rawStart:      valid(6 * hour),
			minuteStart:   valid(7 * hour),
			hourStart:     valid(7 * hour),
			wantSource:    0,
			wantWidth:     5 * time.Minute,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, width := planSeriesResolution(test.desired, test.coverageStart, test.rawStart, test.minuteStart, test.hourStart)
			if source != test.wantSource || width != test.wantWidth {
				t.Fatalf("source=%d width=%s, want source=%d width=%s", source, width, test.wantSource, test.wantWidth)
			}
		})
	}
}

func TestSeriesCoverageStartAllowsStartupButNotRetentionGaps(t *testing.T) {
	hour := int64(time.Hour / time.Microsecond)
	valid := func(value int64) sql.NullInt64 { return sql.NullInt64{Int64: value, Valid: true} }
	if got := seriesCoverageStart(0, 6*hour, valid(6*hour+5_000_000), 5*time.Second); got != 6*hour+5_000_000 {
		t.Fatalf("new target coverage start=%d", got)
	}
	if got := seriesCoverageStart(0, 2*hour, valid(6*hour), 5*time.Second); got != 2*hour {
		t.Fatalf("retained target coverage start=%d", got)
	}
}

func TestPresetSeriesResolutions(t *testing.T) {
	start := sql.NullInt64{Int64: 0, Valid: true}
	tests := []struct {
		name       string
		desired    time.Duration
		wantSource int64
	}{
		{name: "one hour preset", desired: time.Minute, wantSource: 60},
		{name: "six hour preset", desired: 5 * time.Minute, wantSource: 60},
		{name: "day preset", desired: 15 * time.Minute, wantSource: 60},
		{name: "week preset", desired: time.Hour, wantSource: 3600},
		{name: "month preset", desired: 4 * time.Hour, wantSource: 3600},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, width := planSeriesResolution(test.desired, 0, start, start, start)
			if source != test.wantSource || width != test.desired {
				t.Fatalf("source=%d width=%s, want source=%d width=%s", source, width, test.wantSource, test.desired)
			}
		})
	}
}

func TestSeriesPointWidthReservesAlignedBoundaryPoint(t *testing.T) {
	tests := []struct {
		span      time.Duration
		maxPoints int
		want      time.Duration
	}{
		{span: time.Hour, maxPoints: 61, want: time.Minute},
		{span: 6 * time.Hour, maxPoints: 73, want: 5 * time.Minute},
		{span: 24 * time.Hour, maxPoints: 97, want: 15 * time.Minute},
		{span: 7 * 24 * time.Hour, maxPoints: 169, want: time.Hour},
		{span: 30 * 24 * time.Hour, maxPoints: 181, want: 4 * time.Hour},
		{span: time.Hour, maxPoints: 1, want: time.Hour},
	}
	for _, test := range tests {
		if got := seriesPointWidth(test.span, test.maxPoints); got != test.want {
			t.Errorf("span=%s max_points=%d width=%s, want %s", test.span, test.maxPoints, got, test.want)
		}
	}
}

func TestUnalignedSeriesRespectPointCap(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	start := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	address := netip.MustParseAddr("127.0.0.1")
	events := make([]model.Event, 0, 124)
	for index := 0; index < 62; index++ {
		at := start.Add(time.Duration(index)*time.Minute + 10*time.Second)
		events = append(events,
			model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{Key: model.ProbeKey{RunID: runID, Sequence: uint64(index + 1)}, TargetID: target.ID, Endpoint: address, ScheduledAt: at, SentAt: at, Timeout: time.Second}},
			model.Event{Kind: model.EventInterfaceSnapshot, Interface: model.InterfaceEvent{Name: "eth0", BootID: "boot", IfIndex: 2, MAC: "00:11:22:33:44:55", SampledAt: at, Counters: model.InterfaceCounters{RXBytes: uint64(index * 1000), TXBytes: uint64(index * 500)}}},
		)
	}
	if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	from := start.Add(30 * time.Second)
	to := from.Add(time.Hour)
	ping, err := db.PingSeries(ctx, target.StableID, from, to, 61)
	if err != nil {
		t.Fatal(err)
	}
	if ping.ResolutionS != 60 || len(ping.Points) > 61 {
		t.Fatalf("ping resolution=%d points=%d", ping.ResolutionS, len(ping.Points))
	}
	interfaces, err := db.InterfaceSeries(ctx, "eth0", from, to, 61)
	if err != nil {
		t.Fatal(err)
	}
	if len(interfaces.Points) > 61 {
		t.Fatalf("interface points=%d", len(interfaces.Points))
	}
	ping, err = db.PingSeries(ctx, target.StableID, from, to, 1)
	if err != nil || len(ping.Points) != 1 {
		t.Fatalf("single-point ping series points=%d err=%v", len(ping.Points), err)
	}
	interfaces, err = db.InterfaceSeries(ctx, "eth0", from, to, 1)
	if err != nil || len(interfaces.Points) != 1 {
		t.Fatalf("single-point interface series points=%d err=%v", len(interfaces.Points), err)
	}
}

func TestPingSeriesUsesCreationBoundaryWithoutHidingRetainedHistory(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	from := time.Now().Add(-7 * time.Hour).Truncate(time.Minute)
	to := from.Add(6 * time.Hour)
	entityStart := to.Add(-30 * time.Minute)
	h := histogram.New()
	h.Observe(uint64(time.Millisecond))
	blob, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	insertRollup := func(resolution int, bucket time.Time, count int64) {
		t.Helper()
		_, err := db.writer.ExecContext(ctx, `INSERT INTO ping_rollups(target_id,resolution_s,bucket_start_us,scheduled_count,attempted_count,sent_count,on_time_count,late_count,unanswered_count,send_error_count,scheduler_missed_count,rtt_count,rtt_sum_ns,rtt_min_ns,rtt_max_ns,histogram,updated_at_us,timeout_min_ns,timeout_max_ns)
			VALUES(?,?,?,?,?,?,?,0,0,0,0,?,?,?,?,?,?,?,?)`, target.ID, resolution, bucket.UnixMicro(), count, count, count, count, count, count*int64(time.Millisecond), int64(time.Millisecond), int64(time.Millisecond), blob, time.Now().UnixMicro(), int64(time.Second), int64(time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
	insertRollup(60, entityStart.Truncate(time.Minute), 7)
	insertRollup(3600, entityStart.Truncate(time.Hour), 90)
	if _, err := db.writer.ExecContext(ctx, `UPDATE targets SET first_seen_us=? WHERE id=?`, entityStart.UnixMicro(), target.ID); err != nil {
		t.Fatal(err)
	}
	series, err := db.PingSeries(ctx, target.StableID, from, to, 73)
	if err != nil {
		t.Fatal(err)
	}
	if series.ResolutionS != 300 || len(series.Points) != 1 || series.Points[0].Scheduled != 7 {
		t.Fatalf("new target series=%+v", series)
	}

	insertRollup(3600, from.Truncate(time.Hour), 99)
	if _, err := db.writer.ExecContext(ctx, `UPDATE targets SET first_seen_us=? WHERE id=?`, from.Add(-24*time.Hour).UnixMicro(), target.ID); err != nil {
		t.Fatal(err)
	}
	series, err = db.PingSeries(ctx, target.StableID, from, to, 73)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled int64
	for _, point := range series.Points {
		scheduled += point.Scheduled
	}
	if series.ResolutionS != 3600 || scheduled != 189 {
		t.Fatalf("retained target resolution=%d scheduled=%d points=%+v", series.ResolutionS, scheduled, series.Points)
	}
}

func TestRejectsNewerDatabaseSchema(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	if _, err := db.writer.Exec(`INSERT INTO schema_migrations(version,checksum,applied_at_us) VALUES(999,'future',?)`, time.Now().UnixMicro()); err != nil {
		t.Fatal(err)
	}
	if err := migrate(context.Background(), db.writer); err == nil {
		t.Fatal("newer schema was accepted")
	}
}

func TestLossRatesExcludeLocalAndSchedulerFailures(t *testing.T) {
	points := aggregatesToPoints(map[int64]*queryAggregate{0: {pingAggregate: pingAggregate{scheduled: 100, attempted: 2, sent: 1, unanswered: 1, sendError: 1, schedulerMissed: 98, hist: histogram.New()}}})
	if len(points) != 1 || points[0].NoReplyPct != 100 || points[0].DeadlineMissPct != 100 {
		t.Fatalf("rates=%+v", points)
	}
}

func TestInterfaceSeriesUsesHistoricalRollupAndReset(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	at := time.Now().Add(-10 * 24 * time.Hour).Truncate(time.Minute)
	result, err := db.writer.ExecContext(ctx, `INSERT INTO interface_generations(boot_id,ifindex,name,display_name,mac,started_at_us) VALUES('boot',2,'eth0','Ethernet','00:11:22:33:44:55',?)`, at.Add(-time.Hour).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	generation, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.writer.ExecContext(ctx, `INSERT INTO interface_rollups(generation_id,resolution_s,bucket_start_us,elapsed_ns,sample_count,reset_count,rx_bytes_delta,tx_bytes_delta,rx_packets_delta,tx_packets_delta,rx_errors_delta,tx_errors_delta,rx_dropped_delta,tx_dropped_delta,rx_missed_delta,rx_fifo_delta,tx_fifo_delta,rx_crc_delta,rx_frame_delta,tx_carrier_delta,collisions_delta,peak_rx_bytes_per_s,peak_tx_bytes_per_s,updated_at_us) VALUES(?,60,?,60000000000,12,0,60000000,30000000,0,0,2,3,4,5,6,0,0,0,0,0,0,1000000,500000,?)`, generation, at.UnixMicro(), time.Now().UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.writer.ExecContext(ctx, `INSERT INTO interface_resets(generation_id,name,at_us,reason) VALUES(?,'eth0',?,'counter_decreased')`, generation, at.Add(10*time.Second).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	series, err := db.InterfaceSeries(ctx, "eth0", at, at.Add(4*time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Points) != 1 || series.Points[0].RXMbps != 8 || !series.Points[0].Reset || series.Points[0].RXMissed != 6 {
		t.Fatalf("series=%+v", series)
	}
}

func TestInterfacesExposePersistentMissingState(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	if _, err := db.writer.ExecContext(ctx, `INSERT INTO configured_interfaces(name,display_name,active,last_seen_us) VALUES('eth0','Ethernet 0',1,?)`, time.Now().UnixMicro()); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	snapshot := model.Event{Kind: model.EventInterfaceSnapshot, Interface: model.InterfaceEvent{Name: "eth0", BootID: "boot", IfIndex: 2, MAC: "00:11:22:33:44:55", SampledAt: at}}
	if err := db.applyBatch(ctx, []model.Event{snapshot}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	interfaces, err := db.Interfaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(interfaces) != 1 || !interfaces[0].Present {
		t.Fatalf("present interface summary=%+v", interfaces)
	}
	missing := model.Event{Kind: model.EventInterfaceReset, Interface: model.InterfaceEvent{Name: "eth0", BootID: "boot", IfIndex: 2, MAC: "00:11:22:33:44:55", SampledAt: at.Add(time.Second), ResetReason: "interface_missing"}}
	if err := db.applyBatch(ctx, []model.Event{missing}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	interfaces, err = db.Interfaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(interfaces) != 1 || interfaces[0].Present || interfaces[0].LastAtMS != at.UnixMilli() {
		t.Fatalf("missing interface summary=%+v", interfaces)
	}
}

func TestWriterDoesNotTreatSequenceAsContiguousWatermark(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	address := netip.MustParseAddr("127.0.0.1")
	now := time.Now()
	orphans := make(map[model.ProbeKey]orphanReply)
	sent := func(sequence uint64) model.Event {
		return model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{
			Key: model.ProbeKey{RunID: runID, Sequence: sequence}, TargetID: target.ID, Endpoint: address,
			ScheduledAt: now, SentAt: now, Timeout: time.Second,
		}}
	}
	if err := db.applyBatch(ctx, []model.Event{sent(2)}, orphans); err != nil {
		t.Fatal(err)
	}
	reply := model.Event{Kind: model.EventProbeReply, Probe: model.ProbeEvent{
		Key: model.ProbeKey{RunID: runID, Sequence: 1}, SentAt: now, ReplyAt: now.Add(time.Millisecond),
		RTT: time.Millisecond, ReplyClass: model.ReplyOnTime, Responder: address,
	}}
	if err := db.applyBatch(ctx, []model.Event{reply}, orphans); err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 {
		t.Fatalf("reply for sequence hole was not buffered: %d", len(orphans))
	}
	if err := db.applyBatch(ctx, []model.Event{sent(1)}, orphans); err != nil {
		t.Fatal(err)
	}
	var replyAt int64
	if err := db.readers.QueryRow(`SELECT reply_at_us FROM ping_samples WHERE run_id=? AND sequence=1`, runID.Bytes()).Scan(&replyAt); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyCollectionsEncodeAsArrays(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	values := []any{}
	interfaces, err := db.Interfaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	traces, err := db.Traces(ctx, target.StableID, 10)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := db.RouteChanges(ctx, target.StableID, 10)
	if err != nil {
		t.Fatal(err)
	}
	values = append(values, interfaces, traces, changes)
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "[]" {
			t.Fatalf("empty collection encoded as %s", data)
		}
	}
}

func TestPingSeriesUsesExistingRollupWhenRawTierIsAbsent(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	start := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	h := histogram.New()
	h.Observe(uint64(time.Millisecond))
	blob, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.writer.Exec(`INSERT INTO ping_rollups(target_id,resolution_s,bucket_start_us,scheduled_count,attempted_count,sent_count,on_time_count,late_count,unanswered_count,send_error_count,scheduler_missed_count,rtt_count,rtt_sum_ns,rtt_min_ns,rtt_max_ns,histogram,updated_at_us,timeout_min_ns,timeout_max_ns)
		VALUES(?,60,?,7,7,7,7,0,0,0,0,7,7000000,1000000,1000000,?,?,1000000000,1000000000)`, target.ID, start.UnixMicro(), blob, time.Now().UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	// A coarser dirty key must not cause a minute source bucket to be removed.
	if _, err := db.writer.Exec(`INSERT INTO dirty_rollups(kind,entity_id,resolution_s,bucket_start_us) VALUES('ping',?,3600,?)`, target.ID, start.Truncate(time.Hour).UnixMicro()); err != nil {
		t.Fatal(err)
	}
	series, err := db.PingSeries(context.Background(), target.StableID, start, start.Add(2*time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Points) != 1 || series.Points[0].Scheduled != 7 {
		t.Fatalf("series=%+v", series)
	}
}

func TestFirstInterfaceSnapshotAfterRestartStartsGenerationOnDecrease(t *testing.T) {
	db, cfg, _, _ := openTestDB(t)
	ctx := context.Background()
	cfg.Interfaces = []config.InterfaceConfig{{Name: "eth0", DisplayName: "Ethernet"}}
	if _, err := db.SyncTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Minute)
	base := model.InterfaceEvent{Name: "eth0", BootID: "boot", IfIndex: 2, MAC: "00:11:22:33:44:55", SampledAt: at,
		Counters: model.InterfaceCounters{RXBytes: 100, TXBytes: 200}}
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventInterfaceSnapshot, Interface: base}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	base.SampledAt = at.Add(time.Second)
	base.Counters.RXBytes = 10
	base.Counters.TXBytes = 20
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventInterfaceSnapshot, Interface: base}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	var generations, resets int
	if err := db.readers.QueryRow(`SELECT COUNT(*) FROM interface_generations WHERE name='eth0'`).Scan(&generations); err != nil {
		t.Fatal(err)
	}
	if err := db.readers.QueryRow(`SELECT COUNT(*) FROM interface_resets WHERE name='eth0' AND reason='counter_decreased_while_stopped'`).Scan(&resets); err != nil {
		t.Fatal(err)
	}
	if generations != 2 || resets != 1 {
		t.Fatalf("generations=%d resets=%d", generations, resets)
	}
	interfaces, err := db.Interfaces(ctx)
	if err != nil || len(interfaces) != 1 || interfaces[0].DisplayName != "Ethernet" {
		t.Fatalf("interfaces=%+v err=%v", interfaces, err)
	}
}

func TestConfiguredInterfaceIsVisibleBeforeFirstObservation(t *testing.T) {
	db, cfg, _, _ := openTestDB(t)
	cfg.Interfaces = []config.InterfaceConfig{{Name: "eth9", DisplayName: "Uplink"}}
	if _, err := db.SyncTargets(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	interfaces, err := db.Interfaces(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(interfaces) != 1 || interfaces[0].Name != "eth9" || interfaces[0].DisplayName != "Uplink" || interfaces[0].LastAtMS != 0 {
		t.Fatalf("interfaces=%+v", interfaces)
	}
}

func TestRouteCandidateIncludesReachedHop(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	address := netip.MustParseAddr("127.0.0.1")
	now := time.Now()
	insert := func(hop int) {
		now = now.Add(time.Second)
		trace := model.TraceResult{TargetID: target.ID, Endpoint: address, Method: "paris-udp", FlowID: "stable",
			StartedAt: now, EndedAt: now.Add(time.Millisecond), Status: "completed", Reached: true, ReachedHop: hop, Signature: `[{"ttl":1,"responders":["192.0.2.1"]}]`}
		if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventTraceCompleted, Trace: trace}}, make(map[model.ProbeKey]orphanReply)); err != nil {
			t.Fatal(err)
		}
	}
	insert(3)
	insert(4)
	insert(5)
	var changes int
	if err := db.readers.QueryRow(`SELECT COUNT(*) FROM route_changes`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes != 0 {
		t.Fatal("different reached hops incorrectly confirmed one candidate")
	}
	insert(4)
	insert(4)
	var oldHop, newHop int
	if err := db.readers.QueryRow(`SELECT old_reached_hop,new_reached_hop FROM route_changes`).Scan(&oldHop, &newHop); err != nil {
		t.Fatal(err)
	}
	if oldHop != 3 || newHop != 4 {
		t.Fatalf("route reached transition=%d -> %d", oldHop, newHop)
	}
}

func TestPressureUsesPhysicalFilesAndSeparatesReserve(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	db.config.MaxBytes = config.Bytes(1000)
	snapshot := pressureSnapshot{main: 999, wal: 1, live: 1, available: 1 << 30, reserve: 0}
	if db.pressureCritical(snapshot) == nil {
		t.Fatal("hard budget did not count physical database and WAL")
	}
	snapshot = pressureSnapshot{main: 900, live: 100, reusable: 800, available: 1 << 30}
	if db.pressureCritical(snapshot) != nil || db.budgetHigh(snapshot) {
		t.Fatal("reusable high-water database was treated as unrecoverably full")
	}
	snapshot = pressureSnapshot{main: 1, available: 10, reserve: 10}
	if db.budgetHigh(snapshot) || !db.reserveLow(snapshot) {
		t.Fatal("budget and filesystem reserve causes were conflated")
	}
}

func TestAgedReplyBeforeRowIsNotMistakenForPrunedData(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	address := netip.MustParseAddr("127.0.0.1")
	sentAt := time.Now().Add(-48 * time.Hour)
	key := model.ProbeKey{RunID: runID, Sequence: 991}
	orphans := make(map[model.ProbeKey]orphanReply)
	reply := model.Event{Kind: model.EventProbeReply, Probe: model.ProbeEvent{Key: key, SentAt: sentAt,
		ReplyAt: sentAt.Add(time.Second), RTT: time.Second, ReplyClass: model.ReplyOnTime, Responder: address}}
	if err := db.applyBatch(ctx, []model.Event{reply}, orphans); err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 {
		t.Fatal("aged but not explicitly pruned reply was discarded")
	}
	sent := model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{Key: key, TargetID: target.ID,
		Endpoint: address, ScheduledAt: sentAt, SentAt: sentAt, Timeout: 2 * time.Second}}
	if err := db.applyBatch(ctx, []model.Event{sent}, orphans); err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatal("aged reordered reply did not join its row")
	}
}

func TestClosedGenerationSealsEarlierDirtyMinute(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	start := time.Now().Add(-3 * time.Minute).Truncate(time.Minute)
	result, err := db.writer.ExecContext(ctx, `INSERT INTO interface_generations(boot_id,ifindex,name,display_name,mac,started_at_us,ended_at_us) VALUES('boot',2,'eth0','eth0','mac',?,?)`, start.Add(-time.Minute).UnixMicro(), start.Add(time.Minute+5*time.Second).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := result.LastInsertId()
	for index, at := range []time.Time{start.Add(-5 * time.Second), start.Add(50 * time.Second)} {
		value := index * 100
		if _, err := db.writer.ExecContext(ctx, `INSERT INTO interface_samples(generation_id,sampled_at_us,rx_bytes,tx_bytes,rx_packets,tx_packets,rx_errors,tx_errors,rx_dropped,tx_dropped,rx_missed,rx_fifo,tx_fifo,rx_crc,rx_frame,tx_carrier,collisions) VALUES(?,?,?,?,0,0,0,0,0,0,0,0,0,0,0,0,0)`, generation, at.UnixMicro(), value, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.writer.ExecContext(ctx, `INSERT INTO dirty_rollups(kind,entity_id,resolution_s,bucket_start_us) VALUES('interface',?,60,?)`, generation, start.UnixMicro()); err != nil {
		t.Fatal(err)
	}
	if err := db.recomputeDirty(ctx, 16, time.Now()); err != nil {
		t.Fatal(err)
	}
	var rollups int
	if err := db.readers.QueryRow(`SELECT COUNT(*) FROM interface_rollups WHERE generation_id=? AND resolution_s=60 AND bucket_start_us=?`, generation, start.UnixMicro()).Scan(&rollups); err != nil {
		t.Fatal(err)
	}
	if rollups != 1 {
		t.Fatal("closed generation left an earlier minute dirty")
	}
	if got := interfaceRate(math.MaxInt64, 1); got != math.MaxInt64 {
		t.Fatalf("overflow-safe rate=%d", got)
	}
}

func TestRetentionQueriesHaveGlobalTimeIndexes(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	queries := map[string]struct {
		sql  string
		args []any
	}{
		"ping_rollups_retention":      {`EXPLAIN QUERY PLAN SELECT target_id FROM ping_rollups WHERE resolution_s=3600 AND bucket_start_us<?`, []any{time.Now().UnixMicro()}},
		"interface_rollups_retention": {`EXPLAIN QUERY PLAN SELECT generation_id FROM interface_rollups WHERE resolution_s=3600 AND bucket_start_us<?`, []any{time.Now().UnixMicro()}},
		"trace_runs_retention":        {`EXPLAIN QUERY PLAN SELECT id FROM trace_runs WHERE ended_at_us<?`, []any{time.Now().UnixMicro()}},
		"route_changes_retention":     {`EXPLAIN QUERY PLAN SELECT id FROM route_changes WHERE confirmed_at_us<?`, []any{time.Now().UnixMicro()}},
		"dirty_rollups_work":          {`EXPLAIN QUERY PLAN SELECT entity_id FROM dirty_rollups WHERE kind='ping' AND resolution_s=60 AND bucket_start_us<? ORDER BY bucket_start_us`, []any{time.Now().UnixMicro()}},
		"ping_samples_sent_retention": {`EXPLAIN QUERY PLAN SELECT 1 FROM ping_samples WHERE sent_at_us<? LIMIT 1`, []any{time.Now().UnixMicro()}},
		"scheduler_gaps_retention":    {`EXPLAIN QUERY PLAN SELECT id FROM scheduler_gaps WHERE first_scheduled_at_us+((missed_count-1)*(interval_ns/1000))<? ORDER BY first_scheduled_at_us+((missed_count-1)*(interval_ns/1000))`, []any{time.Now().UnixMicro()}},
		"scheduler_gaps_target_end":   {`EXPLAIN QUERY PLAN SELECT id FROM scheduler_gaps WHERE target_id=? ORDER BY first_scheduled_at_us+((missed_count-1)*(interval_ns/1000)) DESC LIMIT 1`, []any{1}},
	}
	for index, query := range queries {
		rows, err := db.readers.Query(query.sql, query.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan.WriteString(detail)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan.String(), index) {
			t.Fatalf("query did not use %s: %s", index, plan.String())
		}
	}
}

func TestSQLitePageLimitFailsLoudly(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	if _, err := db.writer.ExecContext(ctx, `CREATE TABLE page_limit_fixture(value BLOB)`); err != nil {
		t.Fatal(err)
	}
	var pages int64
	if err := db.writer.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	var applied int64
	if err := db.writer.QueryRowContext(ctx, fmt.Sprintf(`PRAGMA max_page_count=%d`, pages+2)).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.ExecContext(ctx, `INSERT INTO page_limit_fixture(value) VALUES(randomblob(1048576))`); err == nil {
		t.Fatal("SQLite page-limit exhaustion was silently accepted")
	}
}

func TestInterfaceRollupOwnsRightEdgeOnceAndCountsReset(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	start := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	result, err := db.writer.ExecContext(ctx, `INSERT INTO interface_generations(boot_id,ifindex,name,display_name,mac,started_at_us) VALUES('boot',2,'eth0','eth0','mac',?)`, start.Add(-time.Minute).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := result.LastInsertId()
	for index, at := range []time.Time{start.Add(-10 * time.Second), start, start.Add(10 * time.Second), start.Add(time.Minute)} {
		value := index * 10
		_, err := db.writer.ExecContext(ctx, `INSERT INTO interface_samples(generation_id,sampled_at_us,rx_bytes,tx_bytes,rx_packets,tx_packets,rx_errors,tx_errors,rx_dropped,tx_dropped,rx_missed,rx_fifo,tx_fifo,rx_crc,rx_frame,tx_carrier,collisions) VALUES(?,?,?,?,0,0,0,0,0,0,0,0,0,0,0,0,0)`, generation, at.UnixMicro(), value, value)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.writer.ExecContext(ctx, `INSERT INTO interface_resets(generation_id,name,at_us,reason) VALUES(?,'eth0',?,'test')`, generation, start.Add(5*time.Second).UnixMicro()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.ExecContext(ctx, `INSERT INTO dirty_rollups(kind,entity_id,resolution_s,bucket_start_us) VALUES('interface',?,60,?)`, generation, start.UnixMicro()); err != nil {
		t.Fatal(err)
	}
	if err := db.recomputeInterfaceMinute(ctx, dirtyKey{kind: "interface", entityID: generation, resolution: 60, bucketUS: start.UnixMicro()}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var delta, resets int
	if err := db.readers.QueryRow(`SELECT rx_bytes_delta,reset_count FROM interface_rollups WHERE generation_id=? AND resolution_s=60 AND bucket_start_us=?`, generation, start.UnixMicro()).Scan(&delta, &resets); err != nil {
		t.Fatal(err)
	}
	if delta != 20 || resets != 1 {
		t.Fatalf("delta=%d resets=%d", delta, resets)
	}
}

func TestTraceErrorAndZeroProbesArePersisted(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	at := time.Now()
	trace := model.TraceResult{TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"), Method: "classic-udp",
		StartedAt: at, EndedAt: at.Add(time.Millisecond), Status: "error", ErrorDetail: "permission denied"}
	if err := db.applyBatch(context.Background(), []model.Event{{Kind: model.EventTraceCompleted, Trace: trace}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := db.readers.QueryRow(`SELECT id FROM trace_runs`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	detail, err := db.Trace(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(detail.Probes)
	if err != nil || string(data) != "[]" || detail.ErrorDetail != "permission denied" {
		t.Fatalf("detail=%+v probes=%s err=%v", detail, data, err)
	}
}
