package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type pressureSnapshot struct {
	capacity, available, reserve   int64
	main, live, reusable, wal, shm int64
}

func (d *DB) pressure(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (pressureSnapshot, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(d.path), &stat); err != nil {
		return pressureSnapshot{}, err
	}
	s := pressureSnapshot{capacity: int64(stat.Blocks) * int64(stat.Bsize), available: int64(stat.Bavail) * int64(stat.Bsize), main: fileSize(d.path), wal: fileSize(d.path + "-wal"), shm: fileSize(d.path + "-shm")}
	percent := s.capacity * int64(d.config.FreeSpaceReservePercent) / 100
	s.reserve = max(d.config.FreeSpaceReserve.Int64(), percent)
	var pageSize, pageCount, freePages int64
	if err := queryer.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return s, err
	}
	if err := queryer.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return s, err
	}
	if err := queryer.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
		return s, err
	}
	s.live = (pageCount - freePages) * pageSize
	s.reusable = freePages * pageSize
	return s, nil
}

func (s pressureSnapshot) used() int64 { return s.main + s.wal + s.shm }
func (d *DB) budgetHigh(s pressureSnapshot) bool {
	return s.live+s.wal+s.shm >= d.config.MaxBytes.Int64()*8/10
}
func (d *DB) reserveLow(s pressureSnapshot) bool { return s.available <= s.reserve+(64<<20) }
func (d *DB) pressureHigh(s pressureSnapshot) bool {
	return d.budgetHigh(s) || d.reserveLow(s)
}
func (d *DB) pressureCritical(s pressureSnapshot) error {
	if s.used() >= d.config.MaxBytes.Int64() {
		return fmt.Errorf("storage budget critical: database, WAL, and SHM use %d of %d bytes", s.used(), d.config.MaxBytes.Int64())
	}
	if s.available <= s.reserve+(32<<20) {
		return fmt.Errorf("filesystem reserve reached: %d bytes available, %d reserved", s.available, s.reserve)
	}
	return nil
}
