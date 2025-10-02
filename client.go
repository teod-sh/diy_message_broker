package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/teod-sh/diy_message_broker/protocol"
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

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatal("Failed to connect to server:", err)
	}
	defer conn.Close()

	mode := ModeConsumer
	if envMode := os.Getenv("MODE"); envMode != "" {
		mode = strings.ToLower(envMode)
	}

	topic := "default"
	if envTopic := os.Getenv("TOPIC"); envTopic != "" {
		topic = envTopic
	}

	log.Printf("Connected to server at %s\n", serverAddr)
	log.Printf("Starting in %s mode for topic %s \n", mode, topic)

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
		runProducer(conn, ctx, topic)
	case ModeConsumer:
		runConsumer(conn, ctx, topic)
	default:
		log.Fatalf("Unknown mode: %s. Use 'producer' or 'consumer'", mode)
	}

	log.Println("Disconnected from server")
}

func runProducer(conn net.Conn, ctx context.Context, topic string) {
	msgs := []string{
		"Hello, world!",
		"This is a test message",
		"This is another test message",
		"Goodbye!",
	}
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return
		default:
			err := protocol.WriteProtocolMessage(
				conn,
				protocol.NewProtocolMessage(protocol.MsgTypePublish, protocol.NewPubSubPayload(topic, "", []byte(msg))),
			)

			if err != nil {
				log.Printf("Failed to send message: %v\n", err)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func askForMsg(conn net.Conn, topic string) error {
	if err := protocol.WriteProtocolMessage(conn, protocol.NewProtocolMessage(protocol.MsgTypeConsume, protocol.NewPubSubPayload(topic, "", nil))); err != nil {
		return err
	}
	log.Println("asked for msg")
	return nil
}

func readMsg(ctx context.Context, conn net.Conn) *protocol.ProtocolMessage {
	var msg *protocol.ProtocolMessage
	var errReadingMessage error
	log.Println("trying to read msg")
	for {

		select {
		case <-ctx.Done():
			return nil
		default:
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

			msg, errReadingMessage = protocol.ParseToProtocolMessage(conn)
			if errReadingMessage != nil {
				if netErr, ok := errReadingMessage.(net.Error); ok && netErr.Timeout() {
					continue
				}

				if errReadingMessage == io.EOF {
					log.Printf("\nServer closed connection\n")
				} else {
					log.Printf("Error receiving message from server: %v\n", errReadingMessage)
				}

				return nil
			}
			break
		}
		if msg != nil {
			break
		}
	}

	conn.SetReadDeadline(time.Time{})
	log.Printf("Received one msg")
	return msg
}

func runConsumer(conn net.Conn, ctx context.Context, topic string) {
	log.Printf("Subscribed to topic %s \n", topic)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := askForMsg(conn, topic); err != nil {
				log.Printf("Failed to ask for message: %v\n", err)
				return
			}
			time.Sleep(100 * time.Millisecond)

			msg := readMsg(ctx, conn)
			log.Printf("Received message from server: Type=0x%02x, Payload=%+v\n", msg.Type, msg.Payload)
			switch msg.Type {

			case protocol.MsgTypeConsumeEmpty:
				log.Println("No messages to consume")
				time.Sleep(200 * time.Millisecond)

			case protocol.MsgTypeConsume:
				log.Println("Received message")
				log.Printf("Message Payload: %+v\n", msg.Payload)

			default:
				log.Printf("\n[UNKNOWN MESSAGE] Type: 0x%02x\n> ", msg.Type)
			}
		}
	}
}
