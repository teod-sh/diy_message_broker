package src

import (
	"context"
	"log"
	"time"
)

type Topic struct {
	Name    string
	storage *TopicStorage
}

func NewTopicService(name string, topicStorage *TopicStorage) *Topic {
	if topicStorage == nil {
		panic("Topic storage is nil")
	}

	if name == "" {
		panic("Topic name is empty")
	}

	return &Topic{
		Name:    name,
		storage: topicStorage,
	}
}

func (m *Topic) WatchForMessages(ctx context.Context, inMessagesChannel <-chan *Message, outErrorsChannel chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-inMessagesChannel:
			err := m.storage.Put(ctx, msg)
			if err != nil {
				outErrorsChannel <- err
				continue
			}

			log.Println("Message published")

		default:
			log.Println("waiting...no messages to process")
			time.Sleep(1 * time.Second)
		}
	}
}

func (m *Topic) PublishMessage(ctx context.Context, msg *Message) error {
	return m.storage.Put(ctx, msg)
}

func (m *Topic) GetStorageReference() *TopicStorage {
	return m.storage
}
