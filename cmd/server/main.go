package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handleSignals(cancel context.CancelFunc) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	fmt.Println("Setting up signals handler")
	<-signalChan
	fmt.Println("Ctrl + C received")
	cancel()
}

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

	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create channel: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go handleSignals(cancel)

	for {
		select {

		case <-ctx.Done():
			fmt.Println("Stopping Peril server...")
			return
		default:
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

	}

}
