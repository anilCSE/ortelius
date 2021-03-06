// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/ava-labs/avalanchego/ids"
)

var (
	ErrUnknownVM = errors.New("unknown VM")

	ErrInvalidTopicName    = errors.New("invalid topic name")
	ErrWrongTopicEventType = errors.New("wrong topic event type")
	ErrWrongTopicNetworkID = errors.New("wrong topic networkID")
)

var (
	EventTypeConsensus EventType = "consensus"
	EventTypeDecisions EventType = "decisions"
)

type EventType string

// Message is a message on the event stream
type Message struct {
	id        string
	chainID   string
	body      []byte
	timestamp int64
}

func (m *Message) ID() string       { return m.id }
func (m *Message) ChainID() string  { return m.chainID }
func (m *Message) Body() []byte     { return m.body }
func (m *Message) Timestamp() int64 { return m.timestamp }

func getSocketName(root string, networkID uint32, chainID string, eventType EventType) string {
	return path.Join(root, fmt.Sprintf("%d-%s-%s", networkID, chainID, eventType))
}

func GetTopicName(networkID uint32, chainID string, eventType EventType) string {
	return fmt.Sprintf("%d-%s-%s", networkID, chainID, eventType)
}

func parseTopicNameToChainID(topic string, networkID uint32, eventType EventType) (ids.ID, error) {
	parts := strings.Split(topic, "-")
	if len(parts) != 3 {
		return ids.Empty, ErrInvalidTopicName
	}

	if parts[0] != strconv.Itoa(int(networkID)) {
		return ids.Empty, ErrWrongTopicNetworkID
	}

	if parts[2] != string(eventType) {
		return ids.Empty, ErrWrongTopicEventType
	}

	return ids.FromString(parts[1])
}
