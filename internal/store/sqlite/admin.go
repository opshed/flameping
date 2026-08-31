package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (d *DB) Backup(ctx context.Context, output string) error {
	absolute, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	if _, err := os.Stat(absolute); err == nil {
		return errors.New("backup output already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o750); err != nil {
		return err
	}
	if _, err := d.writer.ExecContext(ctx, `VACUUM INTO ?`, absolute); err != nil {
		return fmt.Errorf("create backup: %w", err)
	}
	return os.Chmod(absolute, 0o640)
}

func (d *DB) Compact(ctx context.Context) error {
	if d.readers != nil {
		if err := d.readers.Close(); err != nil {
			return err
		}
		d.readers = nil
	}
	if _, err := d.writer.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return err
	}
	_, err := d.writer.ExecContext(ctx, `VACUUM`)
	return err
}
