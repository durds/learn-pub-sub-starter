package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	valData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        valData,
	})
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {

	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)

	if err != nil {
		return err
	}

	deliveryCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryCh {
			var data T
			err := json.Unmarshal(msg.Body, &data)
			if err != nil {
				fmt.Printf("Failed to Unmarshal msg: %v", err)
				msg.Nack(false, true)
				continue
			}
			ack := handler(data)
			switch ack {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack called")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequeue called")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard called")
			}

		}
	}()

	return nil
}
