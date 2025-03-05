package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aliaksandr-biarozka/go-kafka-example/sample"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Config struct {
	BootstrapServers string
	Topic            string
}

type outbox struct {
	cfg      Config
	producer *kafka.Producer
}

func New(cfg Config) (*outbox, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     cfg.BootstrapServers,
		"security.protocol":                     "plaintext", // use secure protocol. plain is only as an example
		"sasl.mechanism":                        "PLAIN",     // use secure protocol. plain is only as an example
		"partitioner":                           "consistent_random",
		"enable.idempotence":                    true,
		"max.in.flight.requests.per.connection": 5,
		"acks":                                  "-1", // all
	})
	if err != nil {
		return nil, fmt.Errorf("can not create new producer %w", err)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	return &outbox{cfg: cfg, producer: p}, nil
}

func (p *outbox) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// usually events/messages are fetched from the database. it's just a sample
			messages := sample.GetNextBatch(100)
			dict := make(map[string][]sample.Message)
			for _, msg := range messages {
				dict[msg.CustomerId] = append(dict[msg.CustomerId], msg)
			}

			var wg sync.WaitGroup
			errChan := make(chan error, len(dict))
			for id, group := range dict {
				wg.Add(1)
				go func(id string, messages []sample.Message) {
					defer wg.Done()
					// messages within a group should be sent in order
					for _, msg := range messages {
						jsonMsg, err := json.Marshal(msg)
						if err != nil {
							errChan <- fmt.Errorf("unable to marshal message %v: %w", msg, err)
							break
						}

						ch := make(chan kafka.Event)
						err = p.producer.Produce(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &p.cfg.Topic, Partition: kafka.PartitionAny},
							Key:            []byte(msg.CustomerId),
							Value:          jsonMsg,
							Headers: []kafka.Header{
								{Key: "id", Value: []byte(msg.Id)},
							},
						}, ch)
						if err != nil {
							errChan <- fmt.Errorf("error publishing message %s of customer %s with seq #%v", msg.Id, msg.CustomerId, msg.Sequence)
							break
						}

						select {
						case r := <-ch:
							if dr := r.(*kafka.Message); dr.TopicPartition.Error != nil {
								errChan <- fmt.Errorf("delivery failed for message %s (customer: %s): %w", msg.Id, msg.CustomerId, dr.TopicPartition.Error)
								return
							}
							fmt.Printf("successfully delivered message %s with seq %v. do your cool stuff below\n", msg.Id, msg.Sequence)
						case <-ctx.Done():
							return
						}

						// delete message from db
						if err := sample.DeleteMessage(ctx, msg); err != nil {
							errChan <- err
						}
					}
				}(id, group)
			}

			go func() {
				wg.Wait()
				close(errChan)
			}()

			var e error
			for err := range errChan {
				e = errors.Join(e, err)
			}
			if e != nil {
				fmt.Printf("error: %v\n", e)
			}

			if e != nil || len(messages) < 10 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(5_000 * time.Second):
					continue
				}
			}
		}
	}
}
