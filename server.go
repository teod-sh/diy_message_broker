package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/teod-sh/diy_message_broker/protocol"
	"github.com/teod-sh/diy_message_broker/src"
)

var messageBroker *src.MessageBrokerManager

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("Failed to start server:", err)
	}
	defer listener.Close()

	fmt.Println("Server listening on :8080")
	fmt.Println("Using binary protocol format:")
	fmt.Println("  [4 bytes: length][1 byte: type][8 bytes: timestamp][N bytes: payload]")
	fmt.Println("  [1 byte: topic-name-length][N bytes: topic name][1 byte: msg key length][N: bytes msg key][N bytes: data]")

	messageBroker = src.NewTopicManager()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}

		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("Client %s connected\n", clientAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		_msg, err := protocol.ParseToProtocolMessage(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client %s disconnected\n", clientAddr)
			} else {
				log.Printf("Error receiving message from %s: %v\n", clientAddr, err)
			}
			break
		}
		log.Printf("Received from %s: Type=0x%02x\n", clientAddr, _msg.Type)

		switch _msg.Type {

		case protocol.MsgTypePublish:
			if err := messageBroker.PublishTo(ctx, _msg.Payload.(*protocol.PubSubPayload)); err != nil {
				log.Printf("Failed to publish message to topic: %v\n", err)
				continue
			}
			log.Printf("Published message to topic %s\n", _msg.Payload.(*protocol.PubSubPayload).TopicName)

		case protocol.MsgTypeConsume:
			msg, err := messageBroker.ConsumeFrom(ctx, _msg.Payload.(*protocol.PubSubPayload))
			if err != nil {
				log.Printf("Failed to consume message from topic: %v\n", err)
				continue
			}

			if msg == nil {
				log.Println("sending empty msg")
				toClient := protocol.NewProtocolMessage(protocol.MsgTypeConsumeEmpty, nil)
				if err := protocol.WriteProtocolMessage(conn, toClient); err != nil {
					log.Printf("Failed to send message to %s: %v\n", clientAddr, err)
				}
				continue
			}

			toClient := protocol.NewProtocolMessage(protocol.MsgTypeConsume, msg)
			if err := protocol.WriteProtocolMessage(conn, toClient); err != nil {
				log.Printf("Failed to send message to %s: %v\n", clientAddr, err)
				continue
			}
			log.Println("msg sent to client")

		case protocol.MsgTypeGoodbye:
			if err := protocol.WriteProtocolMessage(conn, protocol.NewProtocolMessage(protocol.MsgTypeGoodbye, []byte("Goodbye from server"))); err != nil {
				log.Printf("Failed to send goodbye message to %s: %v\n", clientAddr, err)
			}
			return
		}
	}

	log.Printf("Client %s connection closed\n", clientAddr)
}
