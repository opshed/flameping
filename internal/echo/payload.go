package echo

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/opshed/flameping/internal/model"
)

const (
	PayloadSize = 56
	tagOffset   = 40
)

var payloadMagic = [4]byte{'F', 'L', 'M', 'P'}

type Identity struct {
	RunID      model.RunID
	Sequence   uint64
	SendOffset time.Duration
	Timeout    time.Duration
}

func EncodePayload(id Identity, secret []byte) ([]byte, error) {
	if len(secret) < 16 {
		return nil, errors.New("probe secret must be at least 16 bytes")
	}
	buf := make([]byte, PayloadSize)
	copy(buf[:4], payloadMagic[:])
	buf[4] = 1
	buf[5] = 0
	binary.BigEndian.PutUint16(buf[6:8], PayloadSize)
	copy(buf[8:16], id.RunID[:])
	binary.BigEndian.PutUint64(buf[16:24], id.Sequence)
	binary.BigEndian.PutUint64(buf[24:32], uint64(id.SendOffset))
	binary.BigEndian.PutUint64(buf[32:40], uint64(id.Timeout))
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(buf[:tagOffset])
	copy(buf[tagOffset:], mac.Sum(nil)[:16])
	return buf, nil
}

func DecodePayload(buf, secret []byte) (Identity, error) {
	var id Identity
	if len(buf) != PayloadSize || string(buf[:4]) != string(payloadMagic[:]) || buf[4] != 1 || int(binary.BigEndian.Uint16(buf[6:8])) != PayloadSize {
		return id, errors.New("invalid probe payload header")
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(buf[:tagOffset])
	if !hmac.Equal(buf[tagOffset:], mac.Sum(nil)[:16]) {
		return id, errors.New("invalid probe payload authentication")
	}
	copy(id.RunID[:], buf[8:16])
	id.Sequence = binary.BigEndian.Uint64(buf[16:24])
	id.SendOffset = time.Duration(binary.BigEndian.Uint64(buf[24:32]))
	id.Timeout = time.Duration(binary.BigEndian.Uint64(buf[32:40]))
	if id.Timeout <= 0 || id.SendOffset < 0 {
		return Identity{}, errors.New("invalid probe payload timing")
	}
	return id, nil
}
