package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type AdvancedConsumerConfig struct {
	BootstrapServers      string
	GroupId               string
	AutoOffsetReset       string
	EnableAutoCommit      bool
	EnableAutoOffsetStore bool
	MaxPoolIntervalMs     int
	Topic                 string
	RedirectTopic         string
	RetryTopic            string
}

type redirectMessage struct {
	Key       string `json:"key"`
	Id        string `json:"id"`
	Tombstone bool   `json:"tombstone"`
}

type advancedConsumer[T any] struct {
	cfg              AdvancedConsumerConfig
	handler          MessageHandler[T]
	redirectMessages map[string]map[string]redirectMessage
	mutex            sync.RWMutex
}

func NewAdvancedConsumer[T any](cfg AdvancedConsumerConfig, handler MessageHandler[T]) (*advancedConsumer[T], error) {
	if len(cfg.RedirectTopic) == 0 && len(cfg.RetryTopic) == 0 {
		return nil, errors.New("redirect topic and retry topic are required")
	}

	return &advancedConsumer[T]{cfg: cfg, handler: handler, redirectMessages: make(map[string]map[string]redirectMessage)}, nil
}

func (ac *advancedConsumer[T]) StartConsumption(ctx context.Context) error {
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

	initCh := make(chan error)
	go ac.fillRedirectMessageStore(ctx, initCh)
	select {
	case <-ctx.Done():
		return nil
	case err := <-initCh:
		if err != nil {
			return err
		}
		fmt.Println("internal cache is initialized")
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

			ac.mutex.RLock()
			_, onRetryFlow := ac.redirectMessages[string(m.Key)]
			ac.mutex.RUnlock()

			if onRetryFlow {
				if err := ac.redirectToRetryTopic(ctx, m, p); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return fmt.Errorf("failed to redirect message to retry flow: %w", err)
				}
				if _, err = c.CommitMessage(m); err != nil {
					fmt.Printf("failed to commit message: %s\n", err)
					continue
				}
				continue
			}

			id := extractMessageId(m.Headers)
			var e T
			if err := json.Unmarshal(m.Value, &e); err != nil {
				fmt.Printf("failed to unmarshal message: %s with id: %s\n", err, id)
				if err := ac.redirectToRetryTopic(ctx, m, p); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return fmt.Errorf("failed to redirect message to retry flow: %w", err)
				}
				if _, err = c.CommitMessage(m); err != nil {
					fmt.Printf("failed to commit message: %s\n", err)
					continue
				}
				continue
			}

			err = ac.handler.Handle(ctx, e)
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				if err := ac.redirectToRetryTopic(ctx, m, p); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return fmt.Errorf("failed to redirect message to retry flow: %w", err)
				}
			}

			if _, err = c.CommitMessage(m); err != nil {
				fmt.Printf("failed to commit message: %s\n", err)
				continue
			}

			fmt.Printf("message %s processed\n", id)
		}
	}
}

func (ac *advancedConsumer[T]) fillRedirectMessageStore(ctx context.Context, initCh chan<- error) {
	id, _ := uuid.NewV7()
	rc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        ac.cfg.BootstrapServers,
		"group.id":                 ac.cfg.GroupId + "-redirect" + id.String(),
		"security.protocol":        "plaintext", // use secure protocol. plain is only as an example
		"sasl.mechanism":           "PLAIN",     // use secure protocol. plain is only as an example
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       "true",
		"enable.auto.offset.store": "false",
		"enable.partition.eof":     "true",
	})
	if err != nil {
		initCh <- fmt.Errorf("failed to create redirect consumer: %w", err)
		return
	}
	defer rc.Close()

	if err := rc.SubscribeTopics([]string{ac.cfg.RedirectTopic}, nil); err != nil {
		initCh <- fmt.Errorf("failed to subscribe to redirect topic: %w", err)
		return
	}

	eofReached := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev := rc.Poll(1_000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var rm redirectMessage
				if err := json.Unmarshal(e.Value, &rm); err != nil {
					initCh <- fmt.Errorf("failed to unmarshal redirect message: %w", err)
					return
				}

				func() {
					ac.mutex.Lock()
					defer ac.mutex.Unlock()
					if !rm.Tombstone {
						ac.redirectMessages[rm.Key][rm.Id] = rm
					} else {
						delete(ac.redirectMessages[rm.Key], rm.Id)
						if len(ac.redirectMessages[rm.Key]) == 0 {
							delete(ac.redirectMessages, rm.Key)
						}
					}
				}()
			case kafka.PartitionEOF:
				if !eofReached {
					eofReached = true
					initCh <- nil
				}
			}
		}
	}
}

func (ac *advancedConsumer[T]) produceMessage(ctx context.Context, p *kafka.Producer, msg *kafka.Message) error {
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

func (ac *advancedConsumer[T]) redirectToRetryTopic(ctx context.Context, m *kafka.Message, p *kafka.Producer) error {
	rm := redirectMessage{
		Key:       string(m.Key),
		Id:        extractMessageId(m.Headers),
		Tombstone: true,
	}
	func() {
		ac.mutex.Lock()
		defer ac.mutex.Unlock()
		ac.redirectMessages[string(m.Key)][rm.Id] = rm
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)

	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 2 * time.Minute

	go func() {
		defer wg.Done()

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &ac.cfg.RetryTopic, Partition: kafka.PartitionAny},
			Key:            m.Key,
			Value:          m.Value,
			Headers:        m.Headers,
		}

		_, err := backoff.Retry(ctx, func() (string, error) {
			return "", ac.produceMessage(ctx, p, msg)
		}, backoff.WithBackOff(b))
		errCh <- err
	}()

	go func() {
		defer wg.Done()

		value, err := json.Marshal(rm)
		if err != nil {
			errCh <- fmt.Errorf("failed to marshal redirect message: %w", err)
			return
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
		errCh <- err
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errs error
	for err := range errCh {
		if errors.Is(err, context.Canceled) {
			return err
		}
		errs = errors.Join(errs, err)
	}

	return errs
}

func extractMessageId(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == "id" {
			return string(header.Value)
		}
	}
	return ""
}
