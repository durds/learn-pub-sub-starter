package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerWriteLogs(gamelog routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")

	if err := gamelogic.WriteLog(gamelog); err != nil {
		return pubsub.NackRequeue
	}

	return pubsub.Ack

}
