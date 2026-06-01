package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type GameConfig struct {
	gs *gamelogic.GameState
}

func main() {
	fmt.Println("Starting Peril client...")

	const connectionString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Failed to dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Sucessfully connected")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Failed to get username: %v\n", err)
		os.Exit(1)
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueType(1),
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Printf("failed to subscibe to queue: %v\n", err)
		os.Exit(1)
	}

	mvCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("failed to get channel from connection: %v\n", err)
		os.Exit(1)
	}
	pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueType(1),
		handlerMove(gameState, mvCh),
	)
	pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.SimpleQueueType(0),
		handleWarMsg(gameState, mvCh),
	)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "spawn":
			gameState.CommandSpawn(words)
		case "move":
			am, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Failed to make move: %v\n", err)
			}
			ch, err := conn.Channel()
			pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				am)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) != 2 {
				fmt.Printf("Wrong number of args: spam <nr>")
			}

			nMessages, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("Failed to parse nr of spam messages: %v", err)
			}
			fmt.Printf("spamming %d msgs\n", nMessages)

			for range nMessages {
				lm := gamelogic.GetMaliciousLog()
				ch, err := conn.Channel()
				if err != nil {
					fmt.Printf("Failed to get channel: %v\n", err)
				}
				pubsub.PublishGameLog(lm, gameState.GetUsername(), ch)
			}
		case "quit":
			fmt.Println("Quitting game")
			return
		default:
			fmt.Println("Unknown command")

		}

	}
}
