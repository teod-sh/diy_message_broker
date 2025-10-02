package src

import (
	"context"
	"sync"

	"github.com/teod-sh/diy_message_broker/protocol"
)

type runningPublisher struct {
	inMessagesChannel <-chan *Message
	outErrorsChannel  chan<- error
}

type runningConsumer struct {
	outMessagesChannel chan<- *Message
	outErrorsChannel   chan<- error
}

type MessageBrokerManager struct {
	Topics    map[string]*Topic
	Consumers map[string]*Consumer

	runningPublishers map[string]*runningPublisher
	runningConsumers  map[string]*runningConsumer

	topicsLock    sync.Mutex
	consumersLock sync.Mutex
}

func NewTopicManager() *MessageBrokerManager {
	return &MessageBrokerManager{
		Topics:    make(map[string]*Topic),
		Consumers: make(map[string]*Consumer),
	}
}

func (m *MessageBrokerManager) GetTopicByName(name string) *Topic {
	m.topicsLock.Lock()
	defer m.topicsLock.Unlock()
	if topic, ok := m.Topics[name]; ok {
		return topic
	}

	m.Topics[name] = m.createNewTopic(name)
	return m.Topics[name]
}

func (m *MessageBrokerManager) createNewTopic(name string) *Topic {
	storage := NewTopicStorage()
	topic := NewTopicService(name, storage)
	return topic
}

func (m *MessageBrokerManager) GetConsumerByTopicName(name string) *Consumer {
	m.consumersLock.Lock()
	defer m.consumersLock.Unlock()
	if consumer, ok := m.Consumers[name]; ok {
		return consumer
	}

	m.Consumers[name] = m.createNewConsumer(name)
	return m.Consumers[name]
}

func (m *MessageBrokerManager) createNewConsumer(topicName string) *Consumer {
	topic := m.GetTopicByName(topicName)
	return NewConsumer(topic.GetStorageReference())
}

func (m *MessageBrokerManager) ConsumeFrom(ctx context.Context, payload *protocol.PubSubPayload) (*protocol.PubSubPayload, error) {
	topicName := string(payload.TopicName)
	consumer := m.GetConsumerByTopicName(topicName)
	msg, err := consumer.ConsumeOne(ctx)
	if err != nil {
		return nil, err
	}
	if msg != nil {
		protocolPayload := protocol.NewPubSubPayload(topicName, msg.Key, msg.Value)
		return protocolPayload, nil
	}

	return nil, nil
}

func (m *MessageBrokerManager) PublishTo(ctx context.Context, payload *protocol.PubSubPayload) error {
	topicName := string(payload.TopicName)
	msg := NewMessageFromRawBytes(payload.MsgKey, payload.Data)
	topic := m.GetTopicByName(topicName)
	return topic.PublishMessage(ctx, msg)
}
