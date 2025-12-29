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
	n.Handle("topology", TopologyHandler(server, n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Server struct {
	mutexMessages sync.RWMutex
	messages      map[int]struct{}

	mutexTopology sync.RWMutex
	Topology      map[string][]string `json:"topology"`
}

func NewServer() *Server {
	return &Server{
		mutexMessages: sync.RWMutex{},
		messages:      make(map[int]struct{}),
		mutexTopology: sync.RWMutex{},
		Topology:      make(map[string][]string),
	}
}

func (s *Server) AddMessage(message int) bool {
	s.mutexMessages.Lock()
	defer s.mutexMessages.Unlock()
	if _, exists := s.messages[message]; exists {
		return false
	}
	s.messages[message] = struct{}{}
	return true
}

func (s *Server) GetMessages() []int {
	s.mutexMessages.RLock()
	defer s.mutexMessages.RUnlock()
	result := make([]int, 0, len(s.messages))
	for message := range s.messages {
		result = append(result, message)
	}
	return result
}

func (s *Server) SetTopology(topology map[string][]string) {
	s.mutexTopology.Lock()
	defer s.mutexTopology.Unlock()
	s.Topology = topology
}

func (s *Server) GetNeighbours(nodeID string) []string {
	s.mutexTopology.RLock()
	defer s.mutexTopology.RUnlock()
	neighbours, ok := s.Topology[nodeID]
	if !ok {
		return []string{}
	}
	return neighbours
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

func BroadcastToNeighbours(server *Server, n *maelstrom.Node, message int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(server.GetNeighbours(n.ID())))

	for _, neighbor := range server.GetNeighbours(n.ID()) {
		wg.Add(1)
		go func(neighbor string) {
			defer wg.Done()
			err := n.Send(neighbor, NewBroadcastRequest(message))
			if err != nil {
				errCh <- err
			}
		}(neighbor)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func BroadcastHandler(server *Server, n *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var request BroadcastRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return err
		}

		if server.AddMessage(request.Message) {
			err := BroadcastToNeighbours(server, n, request.Message)
			if err != nil {
				return err
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
		return n.Reply(msg, NewReadResponse(server.GetMessages()))
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
		server.SetTopology(request.Topology)
		return n.Reply(msg, NewTopologyResponse())
	}
}
