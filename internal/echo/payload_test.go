package echo

import (
	"testing"
	"time"

	"flameping/internal/model"
)

func TestPayloadRoundTripAndAuthentication(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	id := Identity{RunID: model.RunID{1, 2, 3}, Sequence: 1<<32 + 7, SendOffset: 3 * time.Second, Timeout: 750 * time.Millisecond}
	buf, err := EncodePayload(id, secret)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePayload(buf, secret)
	if err != nil || got != id {
		t.Fatalf("Decode = %#v, %v", got, err)
	}
	buf[20] ^= 1
	if _, err := DecodePayload(buf, secret); err == nil {
		t.Fatal("accepted modified payload")
	}
}

func FuzzDecodePayload(f *testing.F) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	valid, _ := EncodePayload(Identity{RunID: model.RunID{1}, Sequence: 7, SendOffset: time.Second, Timeout: time.Second}, secret)
	f.Add(valid)
	f.Add([]byte("short"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodePayload(data, secret)
		if err == nil {
			encoded, encodeErr := EncodePayload(decoded, secret)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if len(encoded) != PayloadSize {
				t.Fatalf("encoded size %d", len(encoded))
			}
		}
	})
}
