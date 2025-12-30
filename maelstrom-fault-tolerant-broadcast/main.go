package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	n := maelstrom.NewNode()
	server := NewServer(n)

	n.Handle("broadcast", BroadcastHandler(server, n))
	n.Handle("broadcast_ok", func(msg maelstrom.Message) error { return nil })
	n.Handle("read", ReadHandler(server, n))
	n.Handle("topology", TopologyHandler(server, n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Server struct {
	Store      *Store
	Topology   *Topology
	broadcasts chan BroadcastMessage
}

type BroadcastMessage struct {
	Message     int
	Destination string
}

func NewServer(n *maelstrom.Node) *Server {
	server := &Server{
		Store:      NewStore(),
		Topology:   NewTopology(),
		broadcasts: make(chan BroadcastMessage, 100),
	}

	for i := 0; i < 50; i++ {
		go func() {
			for {
				select {
				case bm := <-server.broadcasts:
					backoff := time.Millisecond * 100
					maxBackoff := time.Second * 10

					for {
						ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

						log.Printf("======== Node %s broadcasting message %d to %s", n.ID(), bm.Message, bm.Destination)
						_, err := n.SyncRPC(ctx, bm.Destination, NewBroadcastRequest(bm.Message))
						cancel()

						if err == nil {
							break
						}
						time.Sleep(backoff)
						backoff = min(backoff*2, maxBackoff)
					}
				}
			}
		}()
	}

	return server
}

type BroadcastRequest struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func NewBroadcastRequest(message int) BroadcastRequest {
	return BroadcastRequest{
		Type:    "broadcast",
		Message: message,
	}
}

type BroadcastResponse struct {
	Type string `json:"type"`
}

func NewBroadcastResponse() BroadcastResponse {
	return BroadcastResponse{
		Type: "broadcast_ok",
	}
}

func BroadcastHandler(server *Server, n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var request BroadcastRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		if server.Store.AddMessage(request.Message) {
			for _, neighbor := range server.Topology.GetNeighbours(n.ID()) {
				server.broadcasts <- BroadcastMessage{
					Message:     request.Message,
					Destination: neighbor,
				}
			}
		}

		return n.Reply(msg, NewBroadcastResponse())
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

func ReadHandler(server *Server, n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		return n.Reply(msg, NewReadResponse(server.Store.GetMessages()))
	}
}

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

type TopologyResponse struct {
	Type string `json:"type"`
}

func NewTopologyResponse() TopologyResponse {
	return TopologyResponse{
		Type: "topology_ok",
	}
}

func TopologyHandler(server *Server, n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var request TopologyRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}
		server.Topology.SetTopology(request.Topology)
		return n.Reply(msg, NewTopologyResponse())
	}
}
