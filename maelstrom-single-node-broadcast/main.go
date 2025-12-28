package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastRequest struct {
	Message int `json:"message"`
}

type BroadcastResponse struct {
	Type string `json:"type"`
}

func NewBroadcastResponse() BroadcastResponse {
	return BroadcastResponse{
		Type: "broadcast_ok",
	}
}

type ReadResponse struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func NewReadResponse(messages []int) ReadResponse {
	return ReadResponse{
		Type:     "read_ok",
		Messages: messages,
	}
}

type TopologyResponse struct {
	Type string `json:"type"`
}

func NewTopologyResponse() TopologyResponse {
	return TopologyResponse{
		Type: "topology_ok",
	}
}

func main() {
	var messages []int

	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var request BroadcastRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		messages = append(messages, request.Message)
		return n.Reply(msg, NewBroadcastResponse())
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, NewReadResponse(messages))
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, NewTopologyResponse())
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
