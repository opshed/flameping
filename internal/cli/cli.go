package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/opshed/flameping/internal/app"
	"github.com/opshed/flameping/internal/buildinfo"
	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/echo"
	"github.com/opshed/flameping/internal/ifstats"
	"github.com/opshed/flameping/internal/store/sqlite"
	"github.com/opshed/flameping/internal/trace"
)

func Execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "run":
		err = run(ctx, args[1:], stderr)
	case "check-config":
		err = checkConfig(args[1:], stdout)
	case "doctor":
		err = doctor(ctx, args[1:], stdout)
	case "version":
		info := buildinfo.Current()
		_, err = fmt.Fprintf(stdout, "flameping %s (%s, %s)\n", info.Version, info.Commit, info.Date)
	case "db":
		err = database(ctx, args[1:], stdout)
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		usage(stderr)
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "flameping:", err)
		return 1
	}
	return 0
}

func configFlag(name string, args []string, stderr io.Writer) (config.Config, error) {
	set := flag.NewFlagSet(name, flag.ContinueOnError)
	set.SetOutput(stderr)
	path := set.String("config", "/etc/flameping/config.yaml", "configuration file")
	if err := set.Parse(args); err != nil {
		return config.Config{}, err
	}
	if set.NArg() != 0 {
		return config.Config{}, errors.New("unexpected positional arguments")
	}
	return config.Load(*path)
}
func run(parent context.Context, args []string, stderr io.Writer) error {
	cfg, err := configFlag("run", args, stderr)
	if err != nil {
		return err
	}
	logger := newLogger(cfg, stderr)
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return app.Run(ctx, cfg, logger)
}
func checkConfig(args []string, stdout io.Writer) error {
	cfg, err := configFlag("check-config", args, io.Discard)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "configuration valid: %d targets, %d interfaces\n", len(cfg.Targets), len(cfg.Interfaces))
	return err
}

func doctor(ctx context.Context, args []string, stdout io.Writer) error {
	cfg, err := configFlag("doctor", args, io.Discard)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(stdout, "configuration: ok")
	db, err := sqlite.Open(ctx, cfg.Storage)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	status, statusErr := db.Status(ctx)
	_ = db.Close()
	if statusErr != nil {
		return statusErr
	}
	_, _ = fmt.Fprintf(stdout, "storage: ok (%d live bytes, %d byte budget)\n", status.LiveBytes, status.MaxBytes)
	for _, family := range []int{4, 6} {
		socket, socketErr := echo.OpenUnprivileged(family)
		if socketErr != nil {
			raw, rawErr := echo.OpenRaw(family)
			if rawErr != nil {
				_, _ = fmt.Fprintf(stdout, "icmp%d echo: unavailable (ping socket: %v; raw: %v)\n", family, socketErr, rawErr)
			} else {
				_, _ = fmt.Fprintf(stdout, "icmp%d echo: raw fallback ok\n", family)
				_ = raw.Close()
			}
		} else {
			_, _ = fmt.Fprintf(stdout, "icmp%d echo: ok\n", family)
			_ = socket.Close()
		}
	}
	if runtime.GOOS == "linux" {
		source, sourceErr := ifstats.OpenSource()
		if sourceErr != nil {
			_, _ = fmt.Fprintf(stdout, "rtnetlink: unavailable: %v\n", sourceErr)
		} else {
			links, listErr := source.List()
			_ = source.Close()
			if listErr != nil {
				_, _ = fmt.Fprintf(stdout, "rtnetlink: unavailable: %v\n", listErr)
			} else {
				_, _ = fmt.Fprintf(stdout, "rtnetlink: ok (%d links)\n", len(links))
			}
		}
	}
	if cfg.Traceroute.Enabled {
		runner, warnings := trace.OpenRunner(cfg.Traceroute)
		for _, warning := range warnings {
			_, _ = fmt.Fprintln(stdout, "raw ICMP:", warning)
		}
		if len(warnings) == 0 {
			_, _ = fmt.Fprintln(stdout, "raw ICMP: ok")
		}
		_ = runner.Close()
	}
	if data, readErr := os.ReadFile("/proc/sys/net/ipv4/ping_group_range"); readErr == nil {
		_, _ = fmt.Fprintf(stdout, "ping_group_range: %s", data)
	}
	return nil
}

func database(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("db requires backup or compact")
	}
	command := args[0]
	set := flag.NewFlagSet("db "+command, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	path := set.String("config", "/etc/flameping/config.yaml", "configuration file")
	output := set.String("output", "", "backup output")
	if err := set.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	open := sqlite.Open
	if command == "compact" {
		open = sqlite.OpenMaintenance
	}
	db, err := open(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer db.Close()
	switch command {
	case "backup":
		if *output == "" {
			return errors.New("db backup requires --output")
		}
		if err := db.Backup(ctx, *output); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, "backup created:", *output)
		return err
	case "compact":
		if err := db.Compact(ctx); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, "database compacted")
		return err
	default:
		return fmt.Errorf("unknown db command %q", command)
	}
}

func newLogger(cfg config.Config, w io.Writer) *slog.Logger {
	level := slog.LevelInfo
	_ = level.UnmarshalText([]byte(cfg.Logging.Level))
	options := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.Logging.Format == "text" {
		handler = slog.NewTextHandler(w, options)
	} else {
		handler = slog.NewJSONHandler(w, options)
	}
	return slog.New(handler)
}
func usage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "usage: flameping <run|check-config|doctor|version|db backup|db compact> [options]")
}
