package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	invalidTopicNameError     = errors.New(fmt.Sprintf("invalid topic name. It must be up to  %d bytes lengh", TopicNameBytesLength))
	invalidMsgKeyError        = errors.New(fmt.Sprintf("invalid msg key. It must be up to  %d bytes lengh", MsgKeyBytesLength))
	invalidPubSubPayloadError = errors.New("invalid payload. Payload for MsgTypePublish and MsgTypeConsume must be PubSubPayload")
)

// Protocol constants
const (
	MsgTypeGoodbye byte = 0x01

	MsgTypePublish      byte = 0x02
	MsgTypeConsume      byte = 0x03
	MsgTypeConsumeEmpty byte = 0x04

	MsgBytesLength          uint8 = 4
	MsgTypeBytesLength      uint8 = 1
	MsgTimestampBytesLength uint8 = 8

	TopicNameBytesLength uint8 = 1
	MsgKeyBytesLength    uint8 = 1
)

// ProtocolMessage structure for binary protocol
// [4 bytes: message length][1 byte: message type][8 bytes: timestamp][N bytes: payload]
type ProtocolMessage struct {
	Length    uint32
	Type      byte
	Timestamp int64
	Payload   any
}

func NewProtocolMessage(msgType byte, payload any) *ProtocolMessage {
	return &ProtocolMessage{
		Length:    uint32(MsgBytesLength + MsgTypeBytesLength + MsgTimestampBytesLength),
		Type:      msgType,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}
}

func ParseToProtocolMessage(ioReader io.Reader) (*ProtocolMessage, error) {
	var length uint32
	if err := binary.Read(ioReader, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	var msgType byte
	if err := binary.Read(ioReader, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}

	var timestamp int64
	if err := binary.Read(ioReader, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}

	payloadLength := length - uint32(MsgTypeBytesLength+MsgTimestampBytesLength)
	msg := &ProtocolMessage{
		Length:    length,
		Type:      msgType,
		Timestamp: timestamp,
		Payload:   nil,
	}

	var err error
	switch msgType {
	case MsgTypeConsumeEmpty:
		msg.Payload = make([]byte, 0)
	case MsgTypePublish, MsgTypeConsume:
		msg.Payload, err = parsePubSubPayload(ioReader, payloadLength)
	default:
		msg.Payload = make([]byte, payloadLength)
		err = binary.Read(ioReader, binary.BigEndian, msg.Payload)
	}
	return msg, err
}

func WriteProtocolMessage(ioWriter io.Writer, msg *ProtocolMessage) error {

	var length = uint32(MsgTypeBytesLength + MsgTimestampBytesLength)

	switch msg.Type {
	case MsgTypeConsumeEmpty:
		length += 0
	case MsgTypePublish, MsgTypeConsume:
		payload, ok := msg.Payload.(*PubSubPayload)
		if !ok {
			return invalidPubSubPayloadError
		}
		length += payload.Length()
	default:
		length += uint32(len(msg.Payload.([]byte)))
	}

	if err := binary.Write(ioWriter, binary.BigEndian, length); err != nil {
		return err
	}

	if err := binary.Write(ioWriter, binary.BigEndian, msg.Type); err != nil {
		return err
	}

	if err := binary.Write(ioWriter, binary.BigEndian, msg.Timestamp); err != nil {
		return err
	}

	switch msg.Type {
	case MsgTypeConsumeEmpty:
		return nil
	case MsgTypePublish, MsgTypeConsume:
		return writePubSubPayload(ioWriter, msg.Payload.(*PubSubPayload))
	default:
		return binary.Write(ioWriter, binary.BigEndian, msg.Payload.([]byte))
	}
}

// PubSubPayload structure for binary protocol
// [1 byte: topic-name-length][N bytes: topic name][1 byte: msg key length][N: bytes msg key][N bytes: data]
type PubSubPayload struct {
	TopicName []byte
	MsgKey    []byte
	Data      []byte
}

func (m *PubSubPayload) Length() uint32 {
	return uint32(len(m.TopicName) + len(m.MsgKey) + len(m.Data))
}

func NewPubSubPayload(topicName string, msgKey string, data []byte) *PubSubPayload {
	return &PubSubPayload{
		TopicName: []byte(topicName),
		MsgKey:    []byte(msgKey),
		Data:      data,
	}
}

func writePubSubPayload(ioWriter io.Writer, payload *PubSubPayload) error {
	// 255 is the max value for a byte
	if len(payload.TopicName) > 255 {
		return invalidTopicNameError
	}
	if len(payload.MsgKey) > 255 {
		return invalidMsgKeyError
	}

	topicLength := uint8(len(payload.TopicName))
	if err := binary.Write(ioWriter, binary.BigEndian, &topicLength); err != nil {
		return err
	}

	if err := binary.Write(ioWriter, binary.BigEndian, payload.TopicName); err != nil {
		return err
	}

	msgKeyLength := uint8(len(payload.MsgKey))
	if err := binary.Write(ioWriter, binary.BigEndian, &msgKeyLength); err != nil {
		return err
	}

	if err := binary.Write(ioWriter, binary.BigEndian, payload.MsgKey); err != nil {
		return err
	}

	return binary.Write(ioWriter, binary.BigEndian, payload.Data)
}

func parsePubSubPayload(ioReader io.Reader, payloadLength uint32) (*PubSubPayload, error) {
	var topicNameLength uint8
	if err := binary.Read(ioReader, binary.BigEndian, &topicNameLength); err != nil {
		return nil, err
	}

	data := &PubSubPayload{
		TopicName: make([]byte, topicNameLength),
	}
	if err := binary.Read(ioReader, binary.BigEndian, data.TopicName); err != nil {
		return nil, err
	}

	var msgKeyLength uint8
	if err := binary.Read(ioReader, binary.BigEndian, &msgKeyLength); err != nil {
		return nil, err
	}

	data.MsgKey = make([]byte, msgKeyLength)
	if err := binary.Read(ioReader, binary.BigEndian, data.MsgKey); err != nil {
		return nil, err
	}

	data.Data = make([]byte, payloadLength-uint32(topicNameLength+msgKeyLength))
	if err := binary.Read(ioReader, binary.BigEndian, data.Data); err != nil {
		return nil, err
	}
	return data, nil
}
