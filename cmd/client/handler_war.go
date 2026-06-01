package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handleWarMsg(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, looser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message := fmt.Sprintf("{%s} won war against {%s}", winner, looser)
			err := pubsub.PublishGameLog(message, gs.GetUsername(), ch)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		case gamelogic.WarOutcomeYouWon:
			message := fmt.Sprintf("{%s} won war against {%s}", winner, looser)
			err := pubsub.PublishGameLog(message, gs.GetUsername(), ch)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			message := fmt.Sprintf("A war between {%s} and {%s} resulted in a draw", winner, looser)
			err := pubsub.PublishGameLog(message, gs.GetUsername(), ch)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		default:
			return pubsub.NackDiscard
		}

	}
}
