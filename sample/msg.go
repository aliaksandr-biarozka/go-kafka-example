package sample

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

var customerIds = []string{gofakeit.UUID(), gofakeit.UUID(), gofakeit.UUID(), gofakeit.UUID(), gofakeit.UUID(), gofakeit.UUID(), gofakeit.UUID()}

type Message struct {
	Id         string    `json:"id"`
	CustomerId string    `json:"customer_id"`
	Text       string    `json:"text"`
	Created    time.Time `json:"created"`
	Sequence   int64     `json:"sequence"`
}

func GetNextBatch(size int) []Message {
	// emulate random batch size retrieved from the database
	s := rand.IntN(size)
	batch := make([]Message, s)

	for i := range s {
		batch[i] = Message{
			Id:         gofakeit.UUID(),
			CustomerId: gofakeit.RandomString(customerIds),
			Text:       gofakeit.MinecraftFood(),
			Created:    time.Now().UTC(),
			Sequence:   int64(i), // just for example
		}
	}
	return batch
}

func DeleteMessage(ctx context.Context, m Message) error {
	// your handle logic here. time sleep is just for example
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(min(rand.Int64(), 5_000)) * time.Millisecond):
		fmt.Printf("message deleted %v\n", m)
	}
	return nil
}
