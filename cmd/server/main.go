package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	gamelogic.PrintServerHelp()

	const connectionString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Failed to dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Sucessfully connected")

	err = pubsub.SubsCribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.SimpleQueueType(0),
		handlerWriteLogs,
	)

	if err != nil {
		fmt.Printf("Failed to subscribe to game_logs queue: %v", err)
		os.Exit(1)
	}

	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open channel: %v", err)
		os.Exit(1)
	}

	for {

		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		if len(words) > 0 && words[0] == "pause" {
			fmt.Println("Sending a pause message")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		} else if len(words) > 0 && words[0] == "resume" {
			fmt.Println("Sending a resume message")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		} else if len(words) > 0 && words[0] == "quit" {
			fmt.Println("Quitting")
			return
		} else {
			fmt.Printf("Invalid command: %s\n", words[0])
		}

	}

	fmt.Println("Stopping Peril server...")

}
