package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var encoded bytes.Buffer
	encoder := gob.NewEncoder(&encoded)

	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        encoded.Bytes(),
	})

	return nil
}

func PublishGameLog(msg, username string, ch *amqp.Channel) error {
	var gl = routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}

	key := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)

	err := PublishGob(ch, routing.ExchangePerilTopic, key, gl)
	if err != nil {
		return err
	}

	return nil
}

func SubsCribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {

	err := subscribe(conn, exchange, queueName, key, simpleQueueType, handler,
		func(encodedData []byte) (T, error) {

			var data T
			decoder := gob.NewDecoder(bytes.NewReader(encodedData))

			err := decoder.Decode(&data)

			if err != nil {
				fmt.Printf("Failed to Unmarshal msg: %v", err)
				var zeroT T
				return zeroT, err
			}

			return data, nil

		})

	if err != nil {
		return err
	}

	return nil
}
