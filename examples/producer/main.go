// Example RabbitMQ producer: connect, publish once, close.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	librmq "github.com/andywang2011/go-rabbitmq"
)

func main() {
	p, err := librmq.NewProducer(context.Background(), librmq.Config{
		Host: "127.0.0.1", Port: 5672,
		User: "guest", Password: "guest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Publish(ctx, "", "demo-queue", []byte("hello"), librmq.WithJSON()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("published")
}
