package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	server := NewServer()

	n := maelstrom.NewNode()
	n.Handle("broadcast", BroadcastHandler(server, n))
	n.Handle("read", ReadHandler(server, n))
	n.Handle("topology", TopologyHandler(n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Server struct {
	messages map[int]struct{}
	mutex    sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		messages: make(map[int]struct{}),
	}
}

func (s *Server) AddMessage(message int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.messages[message] = struct{}{}
}

func (s *Server) GetMessages() []int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	result := make([]int, 0, len(s.messages))
	for message := range s.messages {
		result = append(result, message)
	}
	return result
}

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

func BroadcastHandler(server *Server, n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var request BroadcastRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		server.AddMessage(request.Message)
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
		return n.Reply(msg, NewReadResponse(server.GetMessages()))
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

func TopologyHandler(n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		return n.Reply(msg, NewTopologyResponse())
	}
}
