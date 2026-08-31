//go:build !linux

package ifstats

import "fmt"

func OpenSource() (Source, error) {
	return nil, fmt.Errorf("interface statistics are unsupported on this operating system")
}
