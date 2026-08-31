package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"flameping/internal/config"

	_ "modernc.org/sqlite"
)

type DB struct {
	writer          *sql.DB
	readers         *sql.DB
	path            string
	config          config.StorageConfig
	ready           atomic.Bool
	writerFailed    atomic.Pointer[writerFailure]
	prunedThrough   atomic.Int64
	pressureCh      chan error
	statusMu        sync.RWMutex
	traceStatus     map[string]string
	interfaceStatus string
}

func (d *DB) SetInterfaceCapability(value string) {
	d.statusMu.Lock()
	d.interfaceStatus = value
	d.statusMu.Unlock()
}

func (d *DB) SetTraceCapabilities(value map[string]string) {
	d.statusMu.Lock()
	d.traceStatus = make(map[string]string, len(value))
	for family, status := range value {
		d.traceStatus[family] = status
	}
	d.statusMu.Unlock()
}

type writerFailure struct{ err error }

func Open(ctx context.Context, cfg config.StorageConfig) (*DB, error) {
	return openDB(ctx, cfg, true)
}

// OpenMaintenance permits an offline compact to open a database whose
// existing high-water mark exceeds its newly configured runtime budget.
func OpenMaintenance(ctx context.Context, cfg config.StorageConfig) (*DB, error) {
	return openDB(ctx, cfg, false)
}

func openDB(ctx context.Context, cfg config.StorageConfig, enforceRuntimeBudget bool) (*DB, error) {
	path, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	if err := ensureLocalFilesystem(filepath.Dir(path)); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("create database file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o640); err != nil {
		return nil, fmt.Errorf("set database permissions: %w", err)
	}
	dsn := fileDSN(path, false)
	writer, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	db := &DB{writer: writer, path: path, config: cfg, pressureCh: make(chan error, 1)}
	closeOnError := func(err error) (*DB, error) {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("open database: %w", err))
	}
	if err := configureWriter(ctx, writer, cfg); err != nil {
		return closeOnError(err)
	}
	if enforceRuntimeBudget {
		if err := enforceBudget(ctx, writer, cfg); err != nil {
			return closeOnError(err)
		}
	}
	if err := migrate(ctx, writer); err != nil {
		return closeOnError(err)
	}
	var prunedThrough int64
	if err := writer.QueryRowContext(ctx, `SELECT value_us FROM retention_state WHERE name='ping_pruned_through'`).Scan(&prunedThrough); err != nil {
		return closeOnError(fmt.Errorf("load retention watermark: %w", err))
	}
	db.prunedThrough.Store(prunedThrough)
	if enforceRuntimeBudget {
		if snapshot, err := db.pressure(ctx, writer); err != nil {
			return closeOnError(fmt.Errorf("inspect filesystem capacity: %w", err))
		} else if pressureErr := db.pressureCritical(snapshot); pressureErr != nil {
			checkpointCtx, checkpointCancel := context.WithTimeout(ctx, 5*time.Second)
			checkpointErr := db.checkpointWithRetry(checkpointCtx, "TRUNCATE", 5*time.Second)
			checkpointCancel()
			if checkpointErr != nil {
				return closeOnError(fmt.Errorf("recover over-budget startup WAL: %w", checkpointErr))
			}
			snapshot, err = db.pressure(ctx, writer)
			if err != nil {
				return closeOnError(fmt.Errorf("remeasure filesystem capacity: %w", err))
			}
			if pressureErr = db.pressureCritical(snapshot); pressureErr != nil {
				return closeOnError(pressureErr)
			}
		}
	}
	if err := os.Chmod(path, 0o640); err != nil {
		return closeOnError(fmt.Errorf("set database permissions after migration: %w", err))
	}

	readers, err := sql.Open("sqlite", fileDSN(path, true))
	if err != nil {
		return closeOnError(err)
	}
	readers.SetMaxOpenConns(4)
	readers.SetMaxIdleConns(4)
	if err := readers.PingContext(ctx); err != nil {
		_ = readers.Close()
		return closeOnError(fmt.Errorf("open read pool: %w", err))
	}
	db.readers = readers
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		if _, statErr := os.Stat(sidecar); statErr == nil {
			if err := os.Chmod(sidecar, 0o640); err != nil {
				_ = readers.Close()
				return closeOnError(fmt.Errorf("set SQLite sidecar permissions: %w", err))
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			_ = readers.Close()
			return closeOnError(fmt.Errorf("inspect SQLite sidecar: %w", statErr))
		}
	}
	db.ready.Store(true)
	return db, nil
}

func fileDSN(path string, readOnly bool) string {
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	if readOnly {
		q.Set("mode", "ro")
		q.Add("_pragma", "query_only(1)")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func configureWriter(ctx context.Context, db *sql.DB, cfg config.StorageConfig) error {
	var version string
	if err := db.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&version); err != nil {
		return err
	}
	if !versionAtLeast(version, 3, 51, 3) {
		return fmt.Errorf("SQLite %s is unsafe for WAL; require 3.51.3 or newer", version)
	}
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	if strings.ToLower(mode) != "wal" {
		return fmt.Errorf("database refused WAL mode: %s", mode)
	}
	synchronous := "NORMAL"
	if cfg.Durability == "full" {
		synchronous = "FULL"
	}
	for _, statement := range []string{
		`PRAGMA synchronous=` + synchronous,
		`PRAGMA wal_autocheckpoint=0`,
		`PRAGMA foreign_keys=ON`,
		`PRAGMA busy_timeout=5000`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure SQLite (%s): %w", statement, err)
		}
	}
	return nil
}

func enforceBudget(ctx context.Context, db *sql.DB, cfg config.StorageConfig) error {
	var pageSize, pageCount int64
	if err := db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return err
	}
	mainAllowance := cfg.MaxBytes.Int64() * 9 / 10
	highWater := pageSize * pageCount
	if highWater > mainAllowance {
		return fmt.Errorf("existing database high-water size %d exceeds main-file allowance %d; increase storage.max_bytes or run db compact offline", highWater, mainAllowance)
	}
	maxPages := mainAllowance / pageSize
	if maxPages < pageCount {
		return errors.New("configured database page limit cannot contain the existing file")
	}
	var applied int64
	if err := db.QueryRowContext(ctx, `PRAGMA max_page_count=`+strconv.FormatInt(maxPages, 10)).Scan(&applied); err != nil {
		return fmt.Errorf("set max_page_count: %w", err)
	}
	if applied < maxPages {
		return fmt.Errorf("SQLite applied max_page_count %d below requested %d", applied, maxPages)
	}
	return nil
}

func versionAtLeast(version string, major, minor, patch int) bool {
	parts := strings.SplitN(version, ".", 4)
	if len(parts) < 3 {
		return false
	}
	values := make([]int, 3)
	for i := range values {
		values[i], _ = strconv.Atoi(parts[i])
	}
	if values[0] != major {
		return values[0] > major
	}
	if values[1] != minor {
		return values[1] > minor
	}
	return values[2] >= patch
}

func (d *DB) Close() error {
	d.ready.Store(false)
	var errs []error
	if d.readers != nil {
		errs = append(errs, d.readers.Close())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if d.writer != nil {
		_, checkpointErr := d.writer.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
		errs = append(errs, checkpointErr, d.writer.Close())
	}
	return errors.Join(errs...)
}

func (d *DB) Ready() bool { return d.ready.Load() }

func (d *DB) WriterError() error {
	v := d.writerFailed.Load()
	if v == nil {
		return nil
	}
	return v.err
}

func (d *DB) Pressure() <-chan error { return d.pressureCh }

func (d *DB) pauseForPressure(err error) {
	d.writerFailed.Store(&writerFailure{err: err})
	d.ready.Store(false)
	select {
	case d.pressureCh <- err:
	default:
	}
}
