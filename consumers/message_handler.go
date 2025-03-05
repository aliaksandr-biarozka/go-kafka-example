package consumers

import (
	"context"
	"fmt"
	"time"

	"github.com/aliaksandr-biarozka/go-kafka-example/sample"
	"github.com/cenkalti/backoff/v5"
)

type SimpleMessageHandler struct{}

func (h *SimpleMessageHandler) Handle(ctx context.Context, e sample.Message) error {
	p := backoff.NewExponentialBackOff()
	p.MaxInterval = 2 * time.Minute

	_, err := backoff.Retry(ctx, func() (string, error) {
		// your handle logic here. time sleep is just for example
		if e.Sequence < 0 {
			return "", backoff.Permanent(fmt.Errorf("poison message %v", e))
		}

		return "", nil
	}, backoff.WithBackOff(p))

	return err
}
