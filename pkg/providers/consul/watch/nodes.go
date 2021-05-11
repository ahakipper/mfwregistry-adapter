package watch

import (
	"encoding/json"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

var (
	keyForStoreNodes = "node_watcher/nodes"
)

const (
	statusOnline  = "passing"
	statusOffline = "critical"
)

type nodeEvent struct {
	Action string
	Node   *api.Node
}

type nodesSnapshot struct {
	client      *api.Client
	keyForStore string
}

func NewNodeSnopshot(consulAddress string) (*nodesSnapshot, error) {
	c := api.DefaultConfig()
	c.Address = consulAddress
	client, err := api.NewClient(c)
	if err != nil {
		return nil, err
	}

	ns := &nodesSnapshot{
		client:      client,
		keyForStore: keyForStoreNodes,
	}

	err = ns.initStorageOnce()
	if err != nil {
		return nil, errors.WithMessage(err, "get previous nodes failed")
	}

	return ns, nil
}

func (ns *nodesSnapshot) OnNodesChanged(nodes []*api.Node) ([]*nodeEvent, error) {
	prevNodes, err := ns.getNodes()
	if err != nil {
		return nil, err
	}

	change := compareNodes(prevNodes, nodes)
	if len(change) > 0 {
		if err = ns.saveNodes(nodes); err != nil {
			return change, errors.WithMessage(err, "refresh nodes failed")
		}
	}
	return change, err
}

func (ns *nodesSnapshot) initStorageOnce() error {
	nodes, err := ns.getNodes()
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		nodes, err := ns.getMembers()
		if err != nil {
			return errors.WithMessage(err, "can not initialize node from members")
		}

		if err = ns.saveNodes(nodes); err != nil {
			return err
		}
	}
	return nil
}

func (ns *nodesSnapshot) getNodes() ([]*api.Node, error) {
	var nodes []*api.Node
	pair, _, err := ns.client.KV().Get(ns.keyForStore, &api.QueryOptions{})
	if err != nil {
		return nil, err
	}

	// unset before
	if pair == nil {
		return nodes, nil
	}

	err = json.Unmarshal(pair.Value, &nodes)
	if err != nil {
		return nil, errors.WithMessage(err, "the value of stored nodes is illegal")
	}
	return nodes, nil
}

func (ns *nodesSnapshot) getMembers() ([]*api.Node, error) {
	members, err := ns.client.Agent().Members(true)
	if err != nil {
		return nil, err
	}

	var nodes []*api.Node
	for _, m := range members {
		n := &api.Node{
			ID:              m.Tags["id"],
			Node:            m.Name,
			Address:         m.Addr,
			Datacenter:      m.Tags["dc"],
			TaggedAddresses: map[string]string{},
			Meta:            map[string]string{},
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func (ns *nodesSnapshot) saveNodes(nodes []*api.Node) error {
	value, err := json.Marshal(nodes)
	if err != nil {
		return err
	}

	pair := &api.KVPair{
		Key:   ns.keyForStore,
		Value: value,
	}
	_, err = ns.client.KV().Put(pair, &api.WriteOptions{})
	if err != nil {
		return errors.WithMessage(err, "put data into consul failed")
	}

	return nil
}

func compareNodes(previous []*api.Node, current []*api.Node) []*nodeEvent {
	var indexing = func(nodes []*api.Node) map[string]*api.Node {
		m := map[string]*api.Node{}
		for _, n := range nodes {
			m[n.ID] = n
		}
		return m
	}

	var changes []*nodeEvent

	prevMap, currMap := indexing(previous), indexing(current)
	for id, pn := range prevMap {
		if _, exist := currMap[id]; !exist {
			changes = append(changes, &nodeEvent{
				Action: statusOffline,
				Node:   pn,
			})
		}
	}
	for id, cn := range currMap {
		if _, exist := prevMap[id]; !exist {
			changes = append(changes, &nodeEvent{
				Action: statusOnline,
				Node:   cn,
			})
		}
	}
	return changes
}
