package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {

	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
	)

	if err != nil {
		return err
	}

	if err = ch.Qos(10, 0, false); err != nil {
		return err
	}

	deliveryCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryCh {
			var data T
			data, err := unmarshaller(msg.Body)

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
