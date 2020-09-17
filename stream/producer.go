// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"encoding/binary"

	"github.com/ava-labs/avalanchego/ipcs/socket"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

// producer reads from the socket and writes to the event stream
type Producer struct {
	chainID     string
	eventType   EventType
	sock        *socket.Client
	binFilterFn binFilterFn
	writeBuffer *writeBuffer
}

// NewProducer creates a producer using the given config
func NewProducer(conf cfg.Config, networkID uint32, _ string, chainID string, eventType EventType, log *logging.Log) (*Producer, error) {
	var err error
	err = initializeProducerTasker(conf, log)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		chainID:     chainID,
		eventType:   eventType,
		binFilterFn: newBinFilterFn(conf.Filter.Min, conf.Filter.Max),
		writeBuffer: newWriteBuffer(conf.Brokers, GetTopicName(networkID, chainID, eventType)),
	}

	p.sock, err = socket.Dial(getSocketName(conf.Producer.IPCRoot, networkID, chainID, eventType))
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewConsensusProducerProcessor creates a producer for consensus events
func NewConsensusProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string, log *logging.Log) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID, EventTypeConsensus, log)
}

// NewDecisionsProducerProcessor creates a producer for decision events
func NewDecisionsProducerProcessor(conf cfg.Config, networkID uint32, chainVM string, chainID string, log *logging.Log) (Processor, error) {
	return NewProducer(conf, networkID, chainVM, chainID, EventTypeDecisions, log)
}

// Close shuts down the producer
func (p *Producer) Close() error {
	return p.writeBuffer.close()
}

// ProcessNextMessage takes in a Message from the IPC socket and writes it to
// Kafka
func (p *Producer) ProcessNextMessage(_ context.Context, log logging.Logger) error {
	rawMsg, err := p.sock.Recv()
	if err != nil {
		log.Error("sock.Recv: %s", err.Error())
		return err
	}

	if p.binFilterFn(rawMsg) {
		return nil
	}

	if _, err = p.writeBuffer.Write(rawMsg); err != nil {
		log.Error("writeBuffer.Write: %s", err.Error())
		return err
	}
	return nil
}

func (p *Producer) Write(msg []byte) (int, error) {
	return p.writeBuffer.Write(msg)
}

type binFilterFn func([]byte) bool

// newBinFilterFn returns a binFilterFn with the given range
func newBinFilterFn(min uint32, max uint32) binFilterFn {
	return func(input []byte) bool {
		value := binary.LittleEndian.Uint32(input[:4])
		return !(value < min || value > max)
	}
}
