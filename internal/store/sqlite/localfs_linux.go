//go:build linux

package sqlite

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func ensureLocalFilesystem(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return err
	}
	network := map[int64]string{
		0x6969: "NFS", 0x517b: "SMB", 0xff534d42: "CIFS", 0x73757245: "Coda",
		0x564c: "NCP", 0x5346414f: "AFS", 0x00c36400: "Ceph", 0x01021997: "9P",
	}
	if name, ok := network[int64(stat.Type)]; ok {
		return fmt.Errorf("SQLite WAL requires a local filesystem; %s detected at %s", name, path)
	}
	return nil
}
