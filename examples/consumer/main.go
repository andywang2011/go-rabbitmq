// Example RabbitMQ consumer: ensure queue, concurrent consume, graceful shutdown.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	librmq "github.com/andywang2011/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	cfg := librmq.Config{
		Host: "127.0.0.1", Port: 5672,
		User: "guest", Password: "guest",
		PrefetchCount: 10,
	}

	consumer, err := librmq.NewConsumer(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	handle := librmq.WrapConsumer(consumer)
	defer handle.Close()

	if err := consumer.EnsureQueue("demo-queue"); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	handler := func(ctx context.Context, msg amqp.Delivery) error {
		fmt.Printf("body=%s\n", msg.Body)
		return nil
	}

	if err := consumer.ConsumeWithConcurrency(ctx, "demo-queue", 4, handler); err != nil && err != context.Canceled {
		log.Printf("consume exit: %v", err)
	}
}
