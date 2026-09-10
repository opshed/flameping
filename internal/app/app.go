package app

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	clockpkg "github.com/benbjohnson/clock"

	"github.com/opshed/flameping/internal/buildinfo"
	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/echo"
	"github.com/opshed/flameping/internal/eventbus"
	"github.com/opshed/flameping/internal/httpapi"
	"github.com/opshed/flameping/internal/ifstats"
	"github.com/opshed/flameping/internal/model"
	"github.com/opshed/flameping/internal/obsess"
	"github.com/opshed/flameping/internal/resolver"
	"github.com/opshed/flameping/internal/scheduler"
	"github.com/opshed/flameping/internal/store/sqlite"
	"github.com/opshed/flameping/internal/trace"
)

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) (runErr error) {
	db, err := sqlite.Open(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, db.Close()) }()
	targets, err := db.SyncTargets(ctx, cfg)
	if err != nil {
		return err
	}
	resolved := resolveTargets(ctx, db, targets, logger)

	var runID model.RunID
	if _, err := rand.Read(runID[:]); err != nil {
		return err
	}
	bootID := readBootID()
	if err := db.StartRun(ctx, runID, bootID, buildinfo.Current().Version); err != nil {
		return err
	}
	bus := eventbus.New(cfg.Ping.EventQueue)

	need4, need6 := families(resolved)
	var v4, v6 echo.PacketIO
	if need4 {
		v4, err = openEcho(4)
		if err != nil {
			return fmt.Errorf("open IPv4 echo socket (check net.ipv4.ping_group_range): %w", err)
		}
	}
	if need6 {
		v6, err = openEcho(6)
		if err != nil {
			if v4 != nil {
				_ = v4.Close()
			}
			return fmt.Errorf("open IPv6 echo socket: %w", err)
		}
	}
	for _, item := range resolved {
		if _, parseErr := netip.ParseAddr(item.target.ConfiguredAddress); parseErr == nil {
			continue
		}
		if v4 == nil && (item.target.Family == "auto" || item.target.Family == "4") {
			if candidate, openErr := openEcho(4); openErr == nil {
				v4 = candidate
			} else {
				logger.Warn("IPv4 echo unavailable for a future DNS change", "error", openErr)
			}
		}
		if v6 == nil && (item.target.Family == "auto" || item.target.Family == "6") {
			if candidate, openErr := openEcho(6); openErr == nil {
				v6 = candidate
			} else {
				logger.Warn("IPv6 echo unavailable for a future DNS change", "error", openErr)
			}
		}
	}
	engine, err := echo.NewEngine(bus, runID, v4, v6, cfg.Ping.SendQueue)
	if err != nil {
		return err
	}

	schedulerTargets := make([]scheduler.Target, 0, len(resolved))
	for _, item := range resolved {
		value := echo.Target{ID: item.target.ID, StableID: item.target.StableID, Endpoint: item.endpoint, Interval: item.target.Interval, Timeout: item.target.Timeout}
		schedulerTargets = append(schedulerTargets, scheduler.Target{ID: item.target.ID, StableID: item.target.StableID, Interval: item.target.Interval, Value: value})
	}
	sched := scheduler.New(clockpkg.New(), schedulerTargets, engine)
	configured := make(map[string]config.TargetConfig, len(cfg.Targets))
	for _, target := range cfg.Targets {
		configured[target.ID] = target
	}
	var obsessTargets []obsess.Target
	obsessLogTargets := make(map[int64]sqlite.Target)
	for _, item := range resolved {
		if options := configured[item.target.StableID].EffectiveObsess(cfg); options != nil {
			obsessLogTargets[item.target.ID] = item.target
			obsessTargets = append(obsessTargets, obsess.Target{
				ID: item.target.ID, Endpoint: item.endpoint, Interval: item.target.Interval, Config: options,
			})
		}
	}
	var controller *obsess.Controller
	if len(obsessTargets) > 0 {
		controller, err = obsess.New(clockpkg.New(), obsessTargets,
			func(id int64, interval time.Duration) { sched.UpdateInterval(id, interval) },
			func(change obsess.Transition) {
				target := obsessLogTargets[change.TargetID]
				logObsessTransition(logger, target, configured[target.StableID].EffectiveObsess(cfg), change)
			})
		if err != nil {
			return fmt.Errorf("configure obsess: %w", err)
		}
		engine.SetObserver(controller)
	}

	var collector *ifstats.Collector
	if len(cfg.Interfaces) > 0 {
		source, sourceErr := ifstats.OpenSource()
		if sourceErr != nil {
			db.SetInterfaceCapability("unavailable: " + sourceErr.Error())
			logger.Warn("interface statistics unavailable", "error", sourceErr)
		} else {
			db.SetInterfaceCapability("available")
			collector = ifstats.New(source, bus, cfg.Interfaces)
		}
	} else {
		db.SetInterfaceCapability("disabled")
	}
	var traceEngine *trace.Engine
	var traceRunner *trace.Runner
	if cfg.Traceroute.Enabled {
		traceRunner, warnings := trace.OpenRunner(cfg.Traceroute)
		db.SetTraceCapabilities(traceRunner.Capabilities())
		for _, warning := range warnings {
			logger.Warn("traceroute capability unavailable", "error", warning)
		}
		traceTargets := make([]trace.Target, 0)
		for _, item := range resolved {
			if item.target.Trace {
				traceTargets = append(traceTargets, trace.Target{ID: item.target.ID, StableID: item.target.StableID, Endpoint: item.endpoint})
			}
		}
		traceEngine = trace.NewEngine(traceRunner, bus, traceTargets, cfg.Traceroute.Interval.Value())
	} else {
		db.SetTraceCapabilities(map[string]string{"ipv4": "disabled", "ipv6": "disabled"})
	}

	api, err := httpapi.New(cfg.Server.Listen, db, logger)
	if err != nil {
		return err
	}
	if controller != nil {
		api.SetObsessProvider(controller)
	}
	listener, err := net.Listen("tcp", cfg.Server.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Server.Listen, err)
	}
	logger.Info("flameping ready", "listen", cfg.Server.Listen, "targets", len(resolved), "database", cfg.Storage.Path)

	producerCtx, cancelProducers := context.WithCancel(context.Background())
	schedulerCtx, cancelScheduler := context.WithCancel(context.Background())
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	defer cancelProducers()
	defer cancelScheduler()
	defer cancelWriter()
	errCh := make(chan error, 8)
	var producers sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		producers.Add(1)
		go func() {
			defer producers.Done()
			if runErr := fn(producerCtx); runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, net.ErrClosed) {
				select {
				case errCh <- fmt.Errorf("%s: %w", name, runErr):
				default:
				}
			}
		}()
	}
	writerDone := make(chan error, 1)
	go func() { writerDone <- db.RunWriter(writerCtx, bus) }()
	for _, item := range resolved {
		if item.endpoint.IsValid() && item.previous.IsValid() && item.endpoint != item.previous {
			if err := bus.Publish(ctx, model.Event{Kind: model.EventEndpointChanged, Endpoint: model.EndpointChanged{
				TargetID: item.target.ID, OldAddress: item.previous, NewAddress: item.endpoint,
				ChangedAt: time.Now(), ConfiguredBy: "startup_dns_refresh",
			}}); err != nil {
				return err
			}
		}
	}
	start("echo", engine.Run)
	if controller != nil {
		start("obsess", controller.Run)
	}
	schedulerDone := make(chan struct{})
	producers.Add(1)
	go func() {
		defer producers.Done()
		defer close(schedulerDone)
		if schedulerErr := sched.Run(schedulerCtx); schedulerErr != nil && !errors.Is(schedulerErr, context.Canceled) {
			select {
			case errCh <- fmt.Errorf("scheduler: %w", schedulerErr):
			default:
			}
		}
	}()
	start("dns refresh", func(runCtx context.Context) error {
		return refreshDNS(runCtx, cfg, resolved, engine, traceEngine, bus, logger)
	})
	if collector != nil {
		start("interface collector", func(runCtx context.Context) error {
			collectorErr := collector.Run(runCtx)
			if collectorErr != nil && !errors.Is(collectorErr, context.Canceled) {
				db.SetInterfaceCapability("unavailable: " + collectorErr.Error())
				logger.Error("interface collector stopped; other measurements continue", "error", collectorErr)
				return nil
			}
			return collectorErr
		})
	}
	if traceEngine != nil {
		start("traceroute", traceEngine.Run)
	}
	httpDone := make(chan error, 1)
	go func() {
		err := api.HTTP.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpDone <- err
	}()

	writerStopped := false
	pressurePaused := false
	httpStopped := false
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
	case engineErr := <-engine.Failures():
		runErr = fmt.Errorf("echo: %w", engineErr)
	case runErr = <-httpDone:
		httpStopped = true
		if runErr == nil {
			runErr = errors.New("HTTP server stopped unexpectedly")
		}
	case pressureErr := <-db.Pressure():
		pressurePaused = true
		runErr = pressureErr
		logger.Error("measurement admission paused; HTTP diagnostics remain available", "error", pressureErr)
	case runErr = <-writerDone:
		writerStopped = true
		if runErr == nil {
			runErr = errors.New("storage writer stopped unexpectedly")
		}
	}
	stopHTTP := func() {
		if httpStopped {
			return
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr := api.Shutdown(shutdownCtx)
		shutdownCancel()
		runErr = errors.Join(runErr, shutdownErr)
		if shutdownErr != nil {
			runErr = errors.Join(runErr, api.HTTP.Close())
		}
		httpStopped = true
	}
	if !pressurePaused {
		stopHTTP()
	}
	// Stop admission first. The echo engine remains alive while the scheduler
	// flushes its coalesced gaps to the lossless event stream.
	cancelScheduler()
	select {
	case <-schedulerDone:
	case <-time.After(5 * time.Second):
		if runErr == nil {
			runErr = errors.New("scheduler did not stop within 5s")
		}
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	drainErr := engine.DrainInputs(drainCtx)
	drainCancel()
	if drainErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("drain accepted echo work: %w", drainErr))
	}
	runErr = errors.Join(runErr, engine.Close())
	cancelProducers()
	if collector != nil {
		runErr = errors.Join(runErr, collector.Close())
	}
	if traceRunner != nil {
		runErr = errors.Join(runErr, traceRunner.Close())
	}
	producersDone := make(chan struct{})
	go func() { producers.Wait(); close(producersDone) }()
	select {
	case <-producersDone:
	case <-time.After(10 * time.Second):
		cancelWriter()
		if runErr == nil {
			runErr = errors.New("collectors did not stop within 10s")
		}
	}
	bus.Close()
	if pressurePaused {
		select {
		case <-ctx.Done():
		case httpErr := <-httpDone:
			httpStopped = true
			if httpErr != nil {
				runErr = errors.Join(runErr, httpErr)
			}
		}
		cancelWriter()
	}
	if !writerStopped {
		select {
		case writerErr := <-writerDone:
			writerStopped = true
			if writerErr != nil && !errors.Is(writerErr, context.Canceled) && runErr == nil {
				runErr = writerErr
			}
		case <-time.After(10 * time.Second):
			cancelWriter()
			if runErr == nil {
				runErr = errors.New("storage writer did not drain within 10s")
			}
		}
	}
	stopHTTP()
	return runErr
}

type resolvedTarget struct {
	target   sqlite.Target
	endpoint netip.Addr
	previous netip.Addr
}

func resolveTargets(ctx context.Context, db *sqlite.DB, targets []sqlite.Target, logger *slog.Logger) []resolvedTarget {
	r := resolver.Resolver{}
	result := make([]resolvedTarget, 0, len(targets))
	for _, target := range targets {
		previous, previousErr := db.LastEndpoint(ctx, target.ID)
		if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
			logger.Warn("load previous endpoint failed", "target", target.StableID, "error", previousErr)
			previous = netip.Addr{}
		}
		endpoint, err := r.Resolve(ctx, target.ConfiguredAddress, target.Family, previous)
		if err != nil {
			previousMatches := previous.IsValid() && (target.Family == "auto" || (target.Family == "4" && previous.Is4()) || (target.Family == "6" && previous.Is6()))
			if previousMatches {
				endpoint = previous
				logger.Warn("DNS lookup failed; using last persisted endpoint", "target", target.StableID, "endpoint", previous, "error", err)
			} else {
				logger.Warn("target starts unresolved; only this target is paused", "target", target.StableID, "error", err)
			}
		}
		result = append(result, resolvedTarget{target: target, endpoint: endpoint, previous: previous})
	}
	return result
}
func families(targets []resolvedTarget) (bool, bool) {
	var v4, v6 bool
	for _, t := range targets {
		if !t.endpoint.IsValid() {
			continue
		}
		if t.endpoint.Is4() {
			v4 = true
		} else {
			v6 = true
		}
	}
	return v4, v6
}

func openEcho(family int) (echo.PacketIO, error) {
	socket, err := echo.OpenUnprivileged(family)
	if err == nil {
		return socket, nil
	}
	raw, rawErr := echo.OpenRaw(family)
	if rawErr == nil {
		return raw, nil
	}
	return nil, fmt.Errorf("ping socket: %v; raw fallback: %w", err, rawErr)
}

func refreshDNS(ctx context.Context, cfg config.Config, targets []resolvedTarget, engine *echo.Engine, traceEngine *trace.Engine, bus *eventbus.Bus, logger *slog.Logger) error {
	ticker := time.NewTicker(cfg.DNS.RefreshInterval.Value())
	defer ticker.Stop()
	current := make(map[int64]netip.Addr, len(targets))
	byID := make(map[int64]sqlite.Target, len(targets))
	for _, item := range targets {
		current[item.target.ID] = item.endpoint
		byID[item.target.ID] = item.target
	}
	r := resolver.Resolver{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for id, target := range byID {
				if _, parseErr := netip.ParseAddr(target.ConfiguredAddress); parseErr == nil {
					continue
				}
				next, resolveErr := r.Resolve(ctx, target.ConfiguredAddress, target.Family, current[id])
				if resolveErr != nil {
					logger.Warn("DNS refresh failed", "target", target.StableID, "error", resolveErr)
					continue
				}
				if next == current[id] {
					continue
				}
				old := current[id]
				if publishErr := bus.Publish(ctx, model.Event{Kind: model.EventEndpointChanged, Endpoint: model.EndpointChanged{TargetID: id, OldAddress: old, NewAddress: next, ChangedAt: time.Now(), ConfiguredBy: "dns_refresh"}}); publishErr != nil {
					return publishErr
				}
				current[id] = next
				engine.UpdateEndpoint(id, next)
				if traceEngine != nil {
					traceEngine.UpdateEndpoint(id, next)
				}
				logger.Info("target endpoint changed", "target", target.StableID, "old", old, "new", next)
			}
		}
	}
}
func readBootID() string {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(data))
}
