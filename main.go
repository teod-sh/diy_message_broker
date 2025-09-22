package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/teod-sh/diy_message_broker/src"
)

var messageBroker *src.MessageBrokerManager

func main() {
	port := "8080"
	if envPort := os.Getenv("SERVER_PORT"); envPort != "" {
		port = envPort
	}

	messageBroker = src.NewTopicManager()

	http.HandleFunc("/ws/topic", handleTopicConnection)
	http.HandleFunc("/ws/consumer", handleConsumerConnection)

	log.Printf("WebSocket server starting on :%s\n", port)
	log.Printf("Producer endpoint: ws://localhost:%s/ws/topic?topic=<topic_name>\n", port)
	log.Printf("Consumer endpoint: ws://localhost:%s/ws/consumer?topic=<topic_name>\n", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}

// handleTopicConnection manages connections from clients that want to publish messages to a topic
func handleTopicConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		log.Printf("Failed to accept WebSocket connection: %v", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "Connection closed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		log.Printf("No topic specified")
		conn.Write(ctx, websocket.MessageText, []byte("ERROR: No topic specified"))
		return
	}

	topic := messageBroker.GetTopicByName(topicName)
	inMessagesCh := make(chan *src.Message)
	outErrorsCh := make(chan error)

	go topic.WatchForMessages(ctx, inMessagesCh, outErrorsCh)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-outErrorsCh:
				log.Printf("TOPIC: %s, Error Occurred: %v", topicName, err)
			}
		}
	}()

	log.Printf("Producer connected to topic '%s' from %s\n", topicName, r.RemoteAddr)
	err = conn.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf("Connected to topic: %s", topicName)))
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}

	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			log.Printf("Failed to read message: %v", err)
			break
		}

		payload := strings.Split(string(data), "|")
		if len(payload) < 2 {
			log.Printf("Invalid message format: %s", string(data))
			conn.Write(ctx, websocket.MessageText, []byte("ERROR: Invalid message format. Use CMD|DATA"))
			continue
		}

		cmd := payload[0]
		msgData := payload[1]
		log.Printf("Received command: %s, data: %s on topic: %s\n", cmd, msgData, topicName)

		if cmd != "PUT" {
			conn.Write(ctx, websocket.MessageText, []byte("ERROR: Unknown command. Supported commands: PUT"))
			continue
		}

		data_ := strings.Split(msgData, ":")
		if len(data_) < 2 {
			conn.Write(ctx, websocket.MessageText, []byte("ERROR: Invalid message data. Use KEY:VALUE"))
			continue
		}

		msg := &src.Message{
			Key:   data_[0],
			Value: []byte(data_[1]),
		}

		inMessagesCh <- msg

		response := []byte("OK")
		err = conn.Write(ctx, msgType, response)
		if err != nil {
			log.Printf("Failed to send response: %v", err)
			break
		}
		time.Sleep(1 * time.Second)
	}
	log.Printf("Producer disconnected from topic '%s' (%s)\n", topicName, r.RemoteAddr)
}

// handleConsumerConnection manages connections from clients that want to consume messages from a topic
func handleConsumerConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		log.Printf("Failed to accept WebSocket connection: %v", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "Connection closed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		log.Printf("No topic specified")
		conn.Write(ctx, websocket.MessageText, []byte("ERROR: No topic specified"))
		return
	}

	consumer := messageBroker.GetConsumerByTopicName(topicName)
	outMessagesCh := make(chan *src.Message, 10)
	outErrorsCh := make(chan error)
	go consumer.Consume(ctx, outMessagesCh, outErrorsCh)

	log.Printf("Consumer connected to topic '%s' from %s\n", topicName, r.RemoteAddr)
	err = conn.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf("Connected as consumer to topic: %s", topicName)))
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-outErrorsCh:
			log.Printf("CONSUMER: %s, Error Occurred: %v", topicName, err)
		case msg := <-outMessagesCh:
			err = conn.Write(ctx, websocket.MessageText, msg.Value)
			if err != nil {
				log.Printf("Failed to send message to consumer: %v", err)
				return
			}
		default:
			log.Println("no msgs")
			time.Sleep(1 * time.Second)
		}
	}
}
