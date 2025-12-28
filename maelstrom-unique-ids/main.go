package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const GENERATE = "generate"

type GenerateRequest struct {
	Type string `json:"type"`
}

const GENERATE_OK = "generate_ok"

type GenerateResponse struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

func main() {
	n := maelstrom.NewNode()
	n.Handle(GENERATE, func(msg maelstrom.Message) error {
		var request GenerateRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		response := GenerateResponse{
			Type: GENERATE_OK,
			ID:   uuid.NewString(),
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
