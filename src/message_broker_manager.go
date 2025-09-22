package src

import "sync"

type MessageBrokerManager struct {
	Topics    map[string]*Topic
	Consumers map[string]*Consumer

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
