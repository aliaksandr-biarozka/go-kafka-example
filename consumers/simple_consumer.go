package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type SimpleConsumerConfig struct {
	BootstrapServers      string
	GroupId               string
	AutoOffsetReset       string
	EnableAutoCommit      bool
	EnableAutoOffsetStore bool
	MaxPoolIntervalMs     int
	Topic                 string
	ErrorTopic            string
}

type MessageHandler[T any] interface {
	Handle(ctx context.Context, m T) error
}

type simpleConsumer[T any] struct {
	cfg     SimpleConsumerConfig
	handler MessageHandler[T]
}

func NewSimpleConsumer[T any](cfg SimpleConsumerConfig, handler MessageHandler[T]) *simpleConsumer[T] {
	return &simpleConsumer[T]{cfg: cfg, handler: handler}
}

func (sc *simpleConsumer[T]) StartConsumption(ctx context.Context) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        sc.cfg.BootstrapServers,
		"group.id":                 sc.cfg.GroupId,
		"security.protocol":        "plaintext", // use secure protocol. plain is only as an example
		"sasl.mechanism":           "PLAIN",     // use secure protocol. plain is only as an example
		"auto.offset.reset":        sc.cfg.AutoOffsetReset,
		"enable.auto.commit":       sc.cfg.EnableAutoCommit,
		"enable.auto.offset.store": sc.cfg.EnableAutoOffsetStore,
		"max.poll.interval.ms":     sc.cfg.MaxPoolIntervalMs,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer c.Close()

	if err := c.Subscribe(sc.cfg.Topic, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	var p *kafka.Producer
	if len(sc.cfg.ErrorTopic) > 0 {
		p, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":                     sc.cfg.BootstrapServers,
			"security.protocol":                     "plaintext", // use secure protocol. plain is only as an example
			"sasl.mechanism":                        "PLAIN",     // use secure protocol. plain is only as an example
			"partitioner":                           "consistent_random",
			"enable.idempotence":                    true,
			"max.in.flight.requests.per.connection": 5,
			"acks":                                  "-1", // all
		})
		if err != nil {
			return fmt.Errorf("can not create new producer %w", err)
		}
		defer p.Close()

		// Listen to all the client instance-level errors.
		// It's important to read these errors too otherwise the events channel will eventually fill up
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
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			m, err := c.ReadMessage(10_000 * time.Millisecond)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsTimeout() {
					// The client will automatically try to recover from all errors.
					// Timeout is not considered an error because it is raised by
					// ReadMessage in absence of messages.
					continue
				}
				fmt.Printf("error consuming message: %s\n", err)
				continue
			}
			fmt.Printf("message consumed %s: %s\n", m.TopicPartition, string(m.Value))

			var e T
			if err := json.Unmarshal(m.Value, &e); err != nil {
				if p != nil {
					if err := sc.sendToErrorTopic(ctx, m, p); err != nil && !errors.Is(err, context.Canceled) {
						return fmt.Errorf("failed to send message to error topic: %w", err)
					}
					continue
				}
				return fmt.Errorf("failed to unmarshal message: %s", err)
			}

			err = sc.handler.Handle(ctx, e)
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				if p != nil {
					if err := sc.sendToErrorTopic(ctx, m, p); err != nil && !errors.Is(err, context.Canceled) {
						return fmt.Errorf("failed to send message to error topic: %w", err)
					}
					continue
				}
				return fmt.Errorf("failed to handle message: %w", err)
			}

			if _, err = c.CommitMessage(m); err != nil {
				fmt.Printf("failed to commit message: %s\n", err)
				continue
			}

			fmt.Printf("message %s successfully processed\n", extractMessageId(m.Headers))
		}
	}
}

func (sc simpleConsumer[T]) sendToErrorTopic(ctx context.Context, m *kafka.Message, p *kafka.Producer) error {
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sc.cfg.ErrorTopic, Partition: kafka.PartitionAny},
		Key:            m.Key,
		Value:          m.Value,
		Headers:        m.Headers,
	}

	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 2 * time.Minute
	_, err := backoff.Retry(ctx, func() (string, error) {
		dc := make(chan kafka.Event)
		if err := p.Produce(&msg, dc); err != nil {
			return "", fmt.Errorf("failed to produce message: %w", err)
		}

		res := <-dc
		if dr := res.(*kafka.Message); dr.TopicPartition.Error != nil {
			return "", fmt.Errorf("delivery failed: %w", dr.TopicPartition.Error)
		}

		return "", nil
	}, backoff.WithBackOff(b))

	return err
}
