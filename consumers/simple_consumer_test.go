package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aliaksandr-biarozka/go-kafka-example/sample"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var scriptsDir = "./../scripts"

func startKafkaAndCreateTopics(topics ...string) error {
	cmd := exec.Command("./01_kafka_start.sh")
	cmd.Dir = scriptsDir
	if _, err := cmd.Output(); err != nil {
		return err
	}

	cmd = exec.Command("./02_kafka_create_topic.sh", topics...)
	cmd.Dir = scriptsDir
	o, err := cmd.Output()
	fmt.Println(string(o))
	return err
}

func deleteTopicsAndStopKafka(topics ...string) error {
	cmd := exec.Command("./03_kafka_delete_topic.sh", topics...)
	cmd.Dir = scriptsDir
	if o, err := cmd.Output(); err != nil {
		return err
	} else {
		fmt.Println(string(o))
	}

	cmd = exec.Command("./04_kafka_stop.sh")
	cmd.Dir = scriptsDir
	_, err := cmd.Output()
	return err
}

func createConsumer() *simpleConsumer[sample.Message] {
	return NewSimpleConsumer(SimpleConsumerConfig{
		BootstrapServers:      "localhost",
		GroupId:               "simple-consumer-group",
		EnableAutoCommit:      true,
		EnableAutoOffsetStore: false,
		AutoOffsetReset:       "earliest",
		MaxPoolIntervalMs:     30_0000,
		Topic:                 "customer-messages",
	}, &SimpleMessageHandler{})
}

func TestConsumer_WhenMessagesAreValid_ConsumerProcessedThemSuccessfully(t *testing.T) {
	topic := "customer-messages"
	startKafkaAndCreateTopics(topic)
	defer deleteTopicsAndStopKafka(topic)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":                     "localhost",
			"security.protocol":                     "plaintext", // use secure protocol. plain is only as an example
			"sasl.mechanism":                        "PLAIN",     // use secure protocol. plain is only as an example
			"partitioner":                           "consistent_random",
			"enable.idempotence":                    true,
			"max.in.flight.requests.per.connection": 5,
			"acks":                                  "-1", // all
		})
		if err != nil {
			t.Error(fmt.Errorf("can not create new producer %w", err))
		}
		defer p.Close()

		messages := sample.GetNextBatch(100)
		for _, m := range messages {
			jsonMsg, err := json.Marshal(m)
			if err != nil {
				t.Error(fmt.Errorf("failed to marshal message %w", err))
				break
			}

			if err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(m.CustomerId),
				Value:          jsonMsg,
				Headers: []kafka.Header{
					{Key: "id", Value: []byte(m.Id)},
				},
			}, nil); err != nil {
				t.Error(fmt.Errorf("failed to produce message %w", err))
			}
		}

		time.Sleep(3 * time.Second)
		cancel()
	}()

	c := createConsumer()
	if err := c.StartConsumption(ctx); err != nil {
		t.Error(fmt.Errorf("consumer failed %s", err))
	}
	fmt.Println("graceful shutdown")
}

func TestConsumer_WhenPoisonMessageAppeared_ConsumerStoppedWorking(t *testing.T) {
	topic := "customer-messages"
	startKafkaAndCreateTopics(topic)
	defer deleteTopicsAndStopKafka(topic)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":                     "localhost",
			"security.protocol":                     "plaintext", // use secure protocol. plain is only as an example
			"sasl.mechanism":                        "PLAIN",     // use secure protocol. plain is only as an example
			"partitioner":                           "consistent_random",
			"enable.idempotence":                    true,
			"max.in.flight.requests.per.connection": 5,
			"acks":                                  "-1", // all
		})
		if err != nil {
			t.Error(fmt.Errorf("can not create new producer %w", err))
		}
		defer p.Close()

		m := sample.Message{
			Id:         "80e2b670-88d1-4119-a00c-b4a8b0907901",
			CustomerId: "customer-1",
			Text:       "example of poison message",
			Created:    time.Now().UTC(),
			Sequence:   -1,
		}

		jsonMsg, err := json.Marshal(m)
		if err != nil {
			t.Error(fmt.Errorf("failed to marshal message %w", err))
			return
		}

		if err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(m.CustomerId),
			Value:          jsonMsg,
			Headers: []kafka.Header{
				{Key: "id", Value: []byte(m.Id)},
			},
		}, nil); err != nil {
			t.Error(fmt.Errorf("failed to produce message %w", err))
		}

		time.Sleep(3 * time.Second)
		cancel()
	}()

	c := createConsumer()
	if err := c.StartConsumption(ctx); err != nil && !strings.Contains(err.Error(), "poison message") {
		t.Error(fmt.Errorf("consumer failed %s", err))
	}
}
