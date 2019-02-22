package groupcache

import (
	"fmt"
)

/*
	pmap:{
		pmapname1:{
			peerName: string
			items:
		}
		pmapname2:{
			peerName: string
			items:
		}
	}
*/

type Gmap struct {
	MaxEntries int
	pmaps      map[string]*Pmap
	OnEvicted  func(key Key, value interface{})
}

type Pmap struct {
	pName string
	items map[string]bool
}

type Key interface{}

//--------------------------------------增删改查------------------------------------------

func (g *Gmap) Add(peer, key string) {
	g.checkPeer(peer)

	g.pmaps[peer].items[key] = true
}

func (g *Gmap) Count() map[string]int {
	c := make(map[string]int)

	for k, v := range g.pmaps {
		c[k] = len(v.items)
	}

	return c
}

func (g *Gmap) Delete(peer, key string) {
	if !g.checkPeer(peer) {
		return
	}

	_, ok := g.pmaps[peer].items[key]
	if !ok {
		fmt.Println("no such key: ", key)
		return
	}

	var strbe, afstr int

	for K, _ := range g.pmaps {
		strbe += len(g.pmaps[K].items)
	}

	delete(g.pmaps[peer].items, key)
	for K, _ := range g.pmaps {
		afstr += len(g.pmaps[K].items)
	}

	//fmt.Println("gmap after: ", key, " ", afstr)
}

func (g *Gmap) ClearNegative() {
	for k, _ := range g.pmaps {
		g.pmaps[k] = &Pmap{
			pName: k,
			items: make(map[string]bool),
		}
	}
}

func (g *Gmap) ClearPositive(peer string, f func(key string)) {
	if !g.checkPeer(peer) {
		return
	}

	for k, _ := range g.pmaps[peer].items {
		f(k)
		delete(g.pmaps[peer].items, k)
	}
}

func (g *Gmap) checkPeer(peer string) bool {
	t, ok := g.pmaps[peer]
	if !ok {
		t = &Pmap{
			pName: peer,
			items: make(map[string]bool),
		}
		g.pmaps[peer] = t
		return false
	}

	return true
}
