package histogram

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	formatVersion = byte(1)
	gamma         = 1.02
	maxPairs      = 1 << 20
)

var logGamma = math.Log(gamma)

type Histogram struct {
	buckets map[uint32]uint64
	count   uint64
}

func New() *Histogram { return &Histogram{buckets: make(map[uint32]uint64)} }

func (h *Histogram) Observe(value uint64) {
	if value == 0 {
		value = 1
	}
	idx := bucket(value)
	h.buckets[idx]++
	h.count++
}

func (h *Histogram) Count() uint64 { return h.count }

func (h *Histogram) Merge(other *Histogram) {
	if h.buckets == nil {
		h.buckets = make(map[uint32]uint64)
	}
	for idx, count := range other.buckets {
		h.buckets[idx] += count
	}
	h.count += other.count
}

func (h *Histogram) Quantile(q float64) uint64 {
	if h.count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	} else if q > 1 {
		q = 1
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	if rank < 1 {
		rank = 1
	}
	indices := h.indices()
	var seen uint64
	for _, idx := range indices {
		seen += h.buckets[idx]
		if seen >= rank {
			value := math.Exp((float64(idx) + 0.5) * logGamma)
			if value >= float64(math.MaxUint64) {
				return math.MaxUint64
			}
			return uint64(math.Round(value))
		}
	}
	return 0
}

func (h *Histogram) MarshalBinary() ([]byte, error) {
	indices := h.indices()
	buf := make([]byte, 1, 1+len(indices)*4)
	buf[0] = formatVersion
	buf = binary.AppendUvarint(buf, uint64(len(indices)))
	var previous uint32
	for i, idx := range indices {
		delta := idx
		if i > 0 {
			delta = idx - previous
		}
		buf = binary.AppendUvarint(buf, uint64(delta))
		buf = binary.AppendUvarint(buf, h.buckets[idx])
		previous = idx
	}
	return buf, nil
}

func UnmarshalBinary(data []byte) (*Histogram, error) {
	if len(data) == 0 || data[0] != formatVersion {
		return nil, errors.New("unsupported histogram version")
	}
	data = data[1:]
	pairs, n := binary.Uvarint(data)
	if n <= 0 || pairs > maxPairs {
		return nil, errors.New("invalid histogram pair count")
	}
	data = data[n:]
	h := New()
	var previous uint64
	for i := uint64(0); i < pairs; i++ {
		delta, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, errors.New("invalid histogram bucket")
		}
		data = data[n:]
		count, n := binary.Uvarint(data)
		if n <= 0 || count == 0 {
			return nil, errors.New("invalid histogram count")
		}
		data = data[n:]
		idx := delta
		if i > 0 {
			if delta == 0 {
				return nil, errors.New("histogram buckets are not strictly increasing")
			}
			idx = previous + delta
		}
		if idx > math.MaxUint32 {
			return nil, errors.New("histogram bucket overflows")
		}
		h.buckets[uint32(idx)] = count
		h.count += count
		previous = idx
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("histogram has %d trailing bytes", len(data))
	}
	return h, nil
}

func bucket(value uint64) uint32 {
	idx := math.Floor(math.Log(float64(value)) / logGamma)
	if idx <= 0 {
		return 0
	}
	if idx >= math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(idx)
}

func (h *Histogram) indices() []uint32 {
	indices := make([]uint32, 0, len(h.buckets))
	for idx := range h.buckets {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	return indices
}
