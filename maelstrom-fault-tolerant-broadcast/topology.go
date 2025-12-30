package main

import "sync"

type Topology struct {
	mutexTopology sync.RWMutex
	topology      map[string][]string
}

func NewTopology() *Topology {
	return &Topology{
		mutexTopology: sync.RWMutex{},
		topology:      make(map[string][]string),
	}
}

func (t *Topology) SetTopology(topology map[string][]string) {
	t.mutexTopology.Lock()
	defer t.mutexTopology.Unlock()
	for k := range t.topology {
		delete(t.topology, k)
	}
	for k, v := range topology {
		t.topology[k] = v
	}
}

func (t *Topology) GetNeighbours(nodeID string) []string {
	t.mutexTopology.RLock()
	defer t.mutexTopology.RUnlock()
	neighbours, ok := t.topology[nodeID]
	if !ok {
		return []string{}
	}

	result := make([]string, len(neighbours))
	copy(result, neighbours)
	return result
}
