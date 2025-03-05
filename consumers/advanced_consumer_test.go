package consumers

import (
	"context"
	"fmt"
	"testing"

	"github.com/aliaksandr-biarozka/go-kafka-example/sample"
)

func createAdvancedConsumer() *advancedConsumer[sample.Message] {
	c, _ := NewAdvancedConsumer(AdvancedConsumerConfig{
		BootstrapServers:      "localhost",
		GroupId:               "simple-consumer-group",
		EnableAutoCommit:      true,
		EnableAutoOffsetStore: false,
		AutoOffsetReset:       "earliest",
		MaxPoolIntervalMs:     30_0000,
		Topic:                 "customer-messages",
		RedirectTopic:         "customer-messages-redirect",
		RetryTopic:            "customer-messages-retry",
	}, &SimpleMessageHandler{})
	return c
}

func TestAdvancedConsumer_WhenMessagesAreValid_ConsumerProcessedThemSuccessfully(t *testing.T) {
	topic := "customer-messages"
	redirectTopic := "customer-messages-redirect"
	retryTopic := "customer-messages-retry"
	startKafkaAndCreateTopics(topic, redirectTopic, retryTopic)
	defer deleteTopicsAndStopKafka(topic, redirectTopic, retryTopic)

	ctx, _ := context.WithCancel(context.Background())

	c := createAdvancedConsumer()
	if err := c.StartConsumption(ctx); err != nil {
		t.Error(fmt.Errorf("consumer failed %w", err))
	}
	fmt.Println("graceful shutdown")
}
