package main

import "sync"

type Store struct {
	mutexMessages sync.RWMutex
	messages      map[int]struct{}
}

func NewStore() *Store {
	return &Store{
		mutexMessages: sync.RWMutex{},
		messages:      make(map[int]struct{}),
	}
}

func (s *Store) AddMessage(message int) bool {
	s.mutexMessages.Lock()
	defer s.mutexMessages.Unlock()
	if _, exists := s.messages[message]; exists {
		return false
	}
	s.messages[message] = struct{}{}
	return true
}

func (s *Store) GetMessages() []int {
	s.mutexMessages.RLock()
	defer s.mutexMessages.RUnlock()
	result := make([]int, 0, len(s.messages))
	for message := range s.messages {
		result = append(result, message)
	}
	return result
}
