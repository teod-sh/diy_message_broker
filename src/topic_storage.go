package src

import (
	"context"
	"sync"
)

type Message struct {
	Key   string
	Value []byte
}

type TopicStorage struct {
	storage       []*Message
	lastReadIndex int
	lock          sync.Mutex
}

func NewTopicStorage() *TopicStorage {
	return &TopicStorage{
		storage:       make([]*Message, 0),
		lastReadIndex: -1,
	}
}

func (m *TopicStorage) Put(ctx context.Context, msg *Message) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.storage = append(m.storage, msg)

	return nil
}

func (m *TopicStorage) GetNextMessage(ctx context.Context) (*Message, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isOutOfBounds(m.lastReadIndex + 1) {
		return nil, NO_MORE_MESSAGES
	}
	target := m.getNextIndex()

	return m.storage[target], nil
}

func (m *TopicStorage) getNextIndex() int {
	m.lastReadIndex += 1
	return m.lastReadIndex
}

func (m *TopicStorage) isOutOfBounds(index int) bool {
	return index < 0 || index >= len(m.storage)
}

func (m *TopicStorage) IsEmpty() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.storage) == 0
}
