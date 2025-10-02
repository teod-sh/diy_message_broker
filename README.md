# how to run?

```bash
git clone https://github.com/teod-sh/diy_message_broker && cd diy_message_broker
go mod tidy
go run server.go #terminal1
MODE=producer TOPIC=mytopic go run client.go #terminal2
MODE=consumer TOPIC=mytopic go run client.go #terminal3
```