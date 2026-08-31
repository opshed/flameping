//go:build linux

package ifstats

import (
	"errors"

	"github.com/jsimonetti/rtnetlink/v2"

	"flameping/internal/model"
)

type linuxSource struct{ conn *rtnetlink.Conn }

func OpenSource() (Source, error) {
	conn, err := rtnetlink.Dial(nil)
	if err != nil {
		return nil, err
	}
	return &linuxSource{conn: conn}, nil
}

func (s *linuxSource) List() ([]Link, error) {
	messages, err := s.conn.Link.List()
	if err != nil {
		return nil, err
	}
	links := make([]Link, 0, len(messages))
	for _, message := range messages {
		if message.Attributes == nil || message.Attributes.Stats64 == nil {
			continue
		}
		a := message.Attributes
		stats := a.Stats64
		links = append(links, Link{Name: a.Name, IfIndex: int(message.Index), MAC: a.Address.String(), Counters: model.InterfaceCounters{
			RXBytes: stats.RXBytes, TXBytes: stats.TXBytes, RXPackets: stats.RXPackets, TXPackets: stats.TXPackets,
			RXErrors: stats.RXErrors, TXErrors: stats.TXErrors, RXDropped: stats.RXDropped, TXDropped: stats.TXDropped,
			RXMissed: stats.RXMissedErrors, RXFIFO: stats.RXFIFOErrors, TXFIFO: stats.TXFIFOErrors,
			RXCRC: stats.RXCRCErrors, RXFrame: stats.RXFrameErrors, TXCarrier: stats.TXCarrierErrors, Collisions: stats.Collisions,
		}})
	}
	return links, nil
}

func (s *linuxSource) Close() error {
	if s.conn == nil {
		return errors.New("rtnetlink source is not open")
	}
	return s.conn.Close()
}
