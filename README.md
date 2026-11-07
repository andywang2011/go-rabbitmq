# go-rabbitmq

Thin, testable RabbitMQ producer and consumer wrapper.

## Install

```bash
go get github.com/andywang2011/go-rabbitmq@v0.1.0
```

## Quick start

```go
import (
	"fmt"

	"github.com/andywang2011/go-rabbitmq"
)

cfg := rabbitmq.ConfigDefault()

p, err := rabbitmq.NewProducer(cfg)
if err != nil {
	fmt.Println("unavailable")
	return
}
defer p.Close()
fmt.Println("ok")
```

## Examples

```bash
export AMQP_URL='amqp://guest:guest@127.0.0.1:5672/'
cd go-rabbitmq/examples/producer && go run .
cd go-rabbitmq/examples/consumer && go run .
```

## License

MIT — see [LICENSE](LICENSE).
