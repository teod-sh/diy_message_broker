package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const (
	ModeProducer = "producer"
	ModeConsumer = "consumer"
)

func main() {
	serverAddr := "localhost:8080"
	if envAddr := os.Getenv("SERVER_ADDR"); envAddr != "" {
		serverAddr = envAddr
	}

	topic := "default"
	if envTopic := os.Getenv("TOPIC"); envTopic != "" {
		topic = envTopic
	}

	mode := ModeConsumer
	if envMode := os.Getenv("MODE"); envMode != "" {
		mode = strings.ToLower(envMode)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		log.Println("\nReceived interrupt signal. Shutting down...")
		cancel()
		os.Exit(0)
	}()

	switch mode {
	case ModeProducer:
		runProducer(ctx, serverAddr, topic)
	case ModeConsumer:
		runConsumer(ctx, serverAddr, topic)
	default:
		log.Fatalf("Unknown mode: %s. Use 'producer' or 'consumer'", mode)
	}
}

func runProducer(ctx context.Context, serverAddr, topic string) {
	url := fmt.Sprintf("ws://%s/ws/topic?topic=%s", serverAddr, topic)
	log.Printf("Connecting to %s as producer\n", url)

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		log.Fatalf("Failed to connect to WebSocket server: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "Client disconnecting")

	_, data, err := conn.Read(ctx)
	if err != nil {
		log.Fatalf("Failed to read welcome message: %v", err)
	}
	log.Printf("Server says: %s\n", string(data))

	log.Println("Type messages to publish (format: KEY:VALUE) or 'exit' to quit")

	counter := 0
	for {
		counter++
		msg := fmt.Sprintf("PUT|msg%d:Automatic message %d on topic %s", counter, counter, topic)

		err := conn.Write(ctx, websocket.MessageText, []byte(msg))
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			return
		}

		_, respData, err := conn.Read(ctx)
		if err != nil {
			log.Printf("Failed to read response: %v", err)
			return
		}
		log.Printf("Published message %d, server response: %s\n", counter, string(respData))
		if counter == 10 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	log.Println("Producer finished")
}

func runConsumer(ctx context.Context, serverAddr, topic string) {
	url := fmt.Sprintf("ws://%s/ws/consumer?topic=%s", serverAddr, topic)
	log.Printf("Connecting to %s as consumer\n", url)

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		log.Fatalf("Failed to connect to WebSocket server: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "Client disconnecting")

	_, data, err := conn.Read(ctx)
	if err != nil {
		log.Fatalf("Failed to read welcome message: %v", err)
	}
	log.Printf("Server says: %s\n", string(data))

	log.Println("Waiting for messages on topic:", topic)

	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_, msgData, err := conn.Read(ctx)
				if err != nil {
					if strings.Contains(err.Error(), "status = StatusNormalClosure") {
						log.Println("Connection closed normally by server")
					} else {
						log.Printf("Error reading message: %v\n", err)
					}
					return
				}
				log.Printf("Received message: %s\n", string(msgData))
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// Keep the main goroutine alive
	<-ctx.Done()
}
