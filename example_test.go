package rabbitmq_test

import (
	"context"
	"fmt"

	"github.com/andywang2011/go-rabbitmq"
)

func ExampleNewProducer() {
	cfg := rabbitmq.ConfigDefault()

	p, err := rabbitmq.NewProducer(context.Background(), cfg)
	if err != nil {
		fmt.Println("unavailable")
		return
	}
	defer p.Close()
	fmt.Println("ok")
	// Output: unavailable
}
