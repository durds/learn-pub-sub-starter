package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	durable SimpleQueueType = iota
	transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create channel: %v\n", err)
		return nil, amqp.Queue{}, err
	}

	isQueueDurable := false
	autoDelete := false
	exclusive := false

	switch queueType {
	case durable:
		isQueueDurable = true
	case transient:
		autoDelete = true
		exclusive = true
	}

	table := make(amqp.Table, 0)

	table["x-dead-letter-exchange"] = "peril_dlx"

	queue, err := channel.QueueDeclare(queueName, isQueueDurable, autoDelete, exclusive, false, table)
	if err != nil {
		fmt.Printf("Failed to create queue: %v\n", err)
		return nil, amqp.Queue{}, err
	}

	channel.QueueBind(queue.Name, key, exchange, false, nil)

	return channel, queue, nil
}
