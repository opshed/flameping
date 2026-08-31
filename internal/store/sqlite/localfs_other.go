//go:build !linux

package sqlite

func ensureLocalFilesystem(string) error { return nil }
