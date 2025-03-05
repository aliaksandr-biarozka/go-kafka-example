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

type AdvancedRetryConsumerConfig struct {
	BootstrapServers      string
	GroupId               string
	AutoOffsetReset       string
	EnableAutoCommit      bool
	EnableAutoOffsetStore bool
	MaxPoolIntervalMs     int
	Topic                 string
	RedirectTopic         string
}

type advancedRetryConsumer[T any] struct {
	cfg     AdvancedRetryConsumerConfig
	handler MessageHandler[T]
}

func NewAdvancedRetryConsumer[T any](cfg AdvancedRetryConsumerConfig, handler MessageHandler[T]) (*advancedRetryConsumer[T], error) {
	if cfg.RedirectTopic == "" {
		return nil, errors.New("redirect topic is required")
	}

	return &advancedRetryConsumer[T]{
		cfg:     cfg,
		handler: handler,
	}, nil
}

func (ac *advancedRetryConsumer[T]) StartConsumption(ctx context.Context) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     ac.cfg.BootstrapServers,
		"security.protocol":                     "plaintext", // use secure protocol. plain is only as an example
		"sasl.mechanism":                        "PLAIN",     // use secure protocol. plain is only as an example
		"partitioner":                           "consistent_random",
		"enable.idempotence":                    true,
		"max.in.flight.requests.per.connection": 5,
		"acks":                                  "-1", // all
		"socket.nagle.disable":                  true,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
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

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        ac.cfg.BootstrapServers,
		"group.id":                 ac.cfg.GroupId,
		"security.protocol":        "plaintext", // use secure protocol. plain is only as an example
		"sasl.mechanism":           "PLAIN",     // use secure protocol. plain is only as an example
		"auto.offset.reset":        ac.cfg.AutoOffsetReset,
		"enable.auto.commit":       ac.cfg.EnableAutoCommit,
		"enable.auto.offset.store": ac.cfg.EnableAutoOffsetStore,
		"max.poll.interval.ms":     ac.cfg.MaxPoolIntervalMs,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer c.Close()

	if err := c.Subscribe(ac.cfg.Topic, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
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

			id := extractMessageId(m.Headers)
			var e T
			if err := json.Unmarshal(m.Value, &e); err != nil {
				return fmt.Errorf("failed to unmarshal message: %s with id: %s", err, id)
			}

			b := backoff.NewExponentialBackOff()
			b.MaxInterval = 2 * time.Minute
			_, err = backoff.Retry(ctx, func() (string, error) {
				err = ac.handler.Handle(ctx, e)
				if err != nil {
					if ctx.Err() != nil {
						return "", nil
					}
					return "", fmt.Errorf("failed to handle message: %s with id: %s", err, id)
				}

				return "", nil
			}, backoff.WithBackOff(b))
			if err != nil {
				fmt.Printf("failed to process message: %s\n", err)
				continue
			}

			value, err := json.Marshal(redirectMessage{
				Key:       string(m.Key),
				Id:        id,
				Tombstone: true,
			})
			if err != nil {
				return fmt.Errorf("failed to marshal message: %s", err)
			}

			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &ac.cfg.RedirectTopic, Partition: kafka.PartitionAny},
				Key:            m.Key,
				Headers:        m.Headers,
				Value:          value,
			}
			_, err = backoff.Retry(ctx, func() (string, error) {
				return "", ac.produceMessage(ctx, p, msg)
			}, backoff.WithBackOff(b))
			if err != nil {
				fmt.Printf("failed to produce message to redirect topic: %s\n", err)
				continue
			}

			if _, err = c.CommitMessage(m); err != nil {
				fmt.Printf("failed to commit message: %s\n", err)
				continue
			}
			fmt.Printf("message %s processed\n", id)
		}
	}
}

func (ac *advancedRetryConsumer[T]) produceMessage(ctx context.Context, p *kafka.Producer, msg *kafka.Message) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	if err := p.Produce(msg, deliveryChan); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil
	case e := <-deliveryChan:
		if dr := e.(*kafka.Message); dr.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", dr.TopicPartition.Error)
		}
		return nil
	}
}
