package src

import (
	"context"
	"errors"
	"log"
	"time"
)

type Consumer struct {
	targetStorage *TopicStorage
}

func NewConsumer(targetStorage *TopicStorage) *Consumer {
	return &Consumer{
		targetStorage: targetStorage,
	}
}

func (m *Consumer) Consume(ctx context.Context, outMessagesChannel chan<- *Message, outErrorsChannel chan<- error) {
	for {
		select {

		case <-ctx.Done():
			return
		default:
			if m.targetStorage.IsEmpty() {
				log.Println("No messages to consume")
				time.Sleep(1 * time.Second)
				continue
			}

			message, err := m.targetStorage.GetNextMessage(ctx)

			if err != nil {
				if errors.Is(err, NO_MORE_MESSAGES) {
					time.Sleep(1 * time.Second)
					log.Println("No more messages to consume...")
					continue
				}
				outErrorsChannel <- err
				continue
			}
			log.Println("sending message...")
			outMessagesChannel <- message
		}
	}
}

func (m *Consumer) ConsumeOne(ctx context.Context) (*Message, error) {
	if m.targetStorage.IsEmpty() {
		return nil, nil
	}

	message, err := m.targetStorage.GetNextMessage(ctx)
	if err != nil && errors.Is(err, NO_MORE_MESSAGES) {
		log.Println("No more messages to consume...")
		return nil, nil
	}
	return message, err
}
