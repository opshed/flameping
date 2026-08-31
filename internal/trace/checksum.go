package trace

import (
	"encoding/binary"
	"fmt"
	"net/netip"
)

func parisPayload(source, destination netip.Addr, sourcePort, destinationPort, desiredChecksum uint16) ([]byte, error) {
	if desiredChecksum == 0 {
		return nil, fmt.Errorf("Paris UDP checksum token cannot be zero")
	}
	payload := []byte{'F', 'L', 'M', 'P', 0, 0, 0, 0}
	// The final word compensates the one's-complement sum so the kernel emits
	// desiredChecksum. UDP length is 8-byte header plus payload.
	sum, err := udpPseudoSum(source, destination, sourcePort, destinationPort, payload)
	if err != nil {
		return nil, err
	}
	targetSum := ^desiredChecksum
	comp := onesSubtract(targetSum, fold(sum))
	binary.BigEndian.PutUint16(payload[6:8], comp)
	if got, err := udpChecksum(source, destination, sourcePort, destinationPort, payload); err != nil || got != desiredChecksum {
		return nil, fmt.Errorf("checksum compensation produced %#x, want %#x: %w", got, desiredChecksum, err)
	}
	return payload, nil
}

func udpChecksum(source, destination netip.Addr, sourcePort, destinationPort uint16, payload []byte) (uint16, error) {
	sum, err := udpPseudoSum(source, destination, sourcePort, destinationPort, payload)
	if err != nil {
		return 0, err
	}
	result := ^fold(sum)
	if result == 0 {
		result = 0xffff
	}
	return result, nil
}

func udpPseudoSum(source, destination netip.Addr, sourcePort, destinationPort uint16, payload []byte) (uint32, error) {
	if source.Is4() != destination.Is4() {
		return 0, fmt.Errorf("source and destination families differ")
	}
	var bytes []byte
	if source.Is4() {
		src, dst := source.As4(), destination.As4()
		bytes = append(bytes, src[:]...)
		bytes = append(bytes, dst[:]...)
		bytes = append(bytes, 0, 17)
		bytes = binary.BigEndian.AppendUint16(bytes, uint16(8+len(payload)))
	} else {
		src, dst := source.As16(), destination.As16()
		bytes = append(bytes, src[:]...)
		bytes = append(bytes, dst[:]...)
		bytes = binary.BigEndian.AppendUint32(bytes, uint32(8+len(payload)))
		bytes = append(bytes, 0, 0, 0, 17)
	}
	bytes = binary.BigEndian.AppendUint16(bytes, sourcePort)
	bytes = binary.BigEndian.AppendUint16(bytes, destinationPort)
	bytes = binary.BigEndian.AppendUint16(bytes, uint16(8+len(payload)))
	bytes = append(bytes, 0, 0)
	bytes = append(bytes, payload...)
	if len(bytes)%2 != 0 {
		bytes = append(bytes, 0)
	}
	var sum uint32
	for i := 0; i < len(bytes); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(bytes[i : i+2]))
	}
	return sum, nil
}

func fold(sum uint32) uint16 {
	for sum>>16 != 0 {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return uint16(sum)
}

func onesSubtract(a, b uint16) uint16 {
	// a - b in one's-complement arithmetic.
	result := uint32(a) + uint32(^b)
	return fold(result)
}
