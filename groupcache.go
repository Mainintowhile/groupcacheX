package groupcache

import (
	"encoding/json"
	"errors"
	"groupcacheX/expire"
	pb "groupcacheX/groupcachepb"
	"groupcacheX/lru"
	"groupcacheX/singleflight"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	groupPeersGetStatsOperation = "getStats"
	groupPeersGetOperation      = "get"
	groupPeersHeartOperation    = "heart"
	groupPeersSetOperation      = "set"
	groupPeersRemoveOperation   = "remove"
	groupPeersClearOperation    = "clear"
)

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

//----------------------------------------group-----------------------------------------

type Opts struct {
	Name            string
	CacheBytes      int64
	ExpireDuration  time.Duration
	Logger          func(v ...interface{})
	Getter          Getter
	DisableHttpPeer bool
	FullLoaded      bool
}

func NewGroup(opts *Opts) *Group {
	if opts.Getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, dup := groups[opts.Name]; dup {
		panic("duplicate registration of group " + opts.Name)
	}
	g := &Group{
		name:            opts.Name,
		getter:          opts.Getter,
		cacheBytes:      opts.CacheBytes,
		loadGroup:       &singleflight.Group{},
		disableHttpPeer: opts.DisableHttpPeer,
	}
	if opts.Logger != nil {
		g.log = opts.Logger
	} else {
		g.log = func(v ...interface{}) {}
	}

	if opts.ExpireDuration > 0 {
		g.expire = expire.NewExpire(opts.Name, opts.ExpireDuration, opts.Logger, func(k interface{}) {
			key := k.(string)
			g.mainCache.removeKey(key)
			if g.httpPeer != nil {
				peer := g.httpPeer.peer.GetPeerNameByKey(key)
				if peer != g.httpPeer.self {
					g.httpPeer.pmap.pMapDelete(peer, key)
					if peer, _, ok := g.httpPeer.peer.PickPeer(key); ok {
						g.httpPeer.peerRemove(peer, g.name, key)
					}
				}
			}
		})
	}

	groups[opts.Name] = g
	return g
}

type Group struct {
	name            string
	getter          Getter
	cacheBytes      int64 // limit for sum of mainCache size
	mainCache       cache
	fullLoaded      bool
	httpPeer        *peerHttp
	disableHttpPeer bool
	expire          *expire.Expire
	log             func(v ...interface{})
	loadGroup       flightGroup
	_               int32 // force Stats to be 8-byte aligned on 32-bit platforms
	Stats           Stats
}

func (g *Group) Get(key string, dest Sink, peer ...string) error {
	g.Stats.Gets.Add(1)
	if dest == nil {
		return errors.New("groupcache: nil dest Sink")
	}

	value, cacheHit := g.lookupCache(key)

	if cacheHit {
		g.Stats.CacheHits.Add(1)
		return setSinkView(dest, value)
	}

	value, destPopulated, err := g.load(key, dest, peer...)
	if err != nil {
		g.log(err)
		return err
	}

	if destPopulated {
		return nil
	}

	return setSinkView(dest, value)
}

func (g *Group) Set(key string, i *string) error {
	if len(*i) == 0 {
		return nil
	}
	sink := StringSink(i)
	err := sink.SetString(*i)
	if err != nil {
		g.log(err)
		return err
	}

	value, err := sink.view()
	if err != nil {
		g.log(err)
		return err
	}

	if g.httpPeer != nil {
		if peer, who, ok := g.httpPeer.peer.PickPeer(key); ok {
			if who != g.httpPeer.self {
				err = g.httpPeer.peerSet(peer, g.name, key, i)
				if err == nil {
					return nil
				}
			}
		}
	}

	g.populateCache(key, value, &g.mainCache)
	return nil
}

func (g *Group) Remove(key string) {
	g.mainCache.removeKey(key)
	if g.expire != nil {
		go g.expire.Delete(key)
	}
	if g.httpPeer != nil {
		peer := g.httpPeer.peer.GetPeerNameByKey(key)
		if peer != g.httpPeer.self {
			g.httpPeer.pmap.pMapDelete(peer, key)
			if peer, _, ok := g.httpPeer.peer.PickPeer(key); ok {
				g.httpPeer.peerRemove(peer, g.name, key)
			}
		}
	}
}

func (g *Group) Clear() {
	g.mainCache.clear()
	if g.expire != nil {
		go g.expire.Clear()
	}
	if g.httpPeer != nil {
		g.httpPeer.pmap.pMapclearNegative()
	}
}

func (g *Group) List() { //for debug
	if g.expire != nil {
		g.expire.Foreach(0, func(k interface{}) {
			//g.log("expire key: ", k)
		})
	}
	g.mainCache.forEach(func(k interface{}) {
		//g.log("cache key: ", k)
	})
}

type MachinesStats struct {
	Group     string
	AllStats  []AllStats
	FailPeers []string
}

func (g *Group) MachinesStats() []*MachinesStats {
	o := make([]*MachinesStats, 0)
	for _, v := range groups {
		o = append(o, g.httpPeer.peersStats(v))
	}
	return o
}

func (g *Group) SetDefaultExpireDuration(duration time.Duration) {
	g.log("SetDefaultExpireDuration:", duration)
	if duration > 0 && g.expire != nil {
		g.expire.SetDefaultExpireDuration(duration)
	}
}

func (g *Group) load(key string, dest Sink, peer ...string) (value ByteView, destPopulated bool, err error) {
	g.Stats.Loads.Add(1)
	viewi, err := g.loadGroup.Do(key, func() (interface{}, error) {
		if value, cacheHit := g.lookupCache(key); cacheHit {
			g.Stats.CacheHits.Add(1)
			return value, nil
		}

		g.Stats.LoadsDeduped.Add(1)
		var value ByteView
		var err error
		//means from net, not pickPeer twice
		if len(peer) == 0 && g.httpPeer != nil {
			if peer, _, ok := g.httpPeer.peer.PickPeer(key); ok {
				//ok means network is ok but data notfound
				var ok bool
				ok, value, err = g.httpPeer.peerGet(peer, g.name, key)
				if err == nil {
					g.Stats.PeerLoads.Add(1)
					return value, nil
				}
				//strategy: don't find local again if the other peer not found, trust it
				if ok {
					return value, err
				}
				/*(strategies:
				⤫ 1 find local don't cache,
				⤫ 2 find local cache it and set shortor expire time)
				√ 3 find local cache it with peer mark; (how mark: use )
					add peer sync heart timer like 5 seconds;
					when the peer back, broadcast clear the peer relate data op
				*/
				//strategy: find local and mark it into expire table if net work go wrong.
				//let's set expire about 30 seconds for now
				g.Stats.PeerErrors.Add(1)
			}
		}

		value, err = g.getLocally(key, dest)
		if err != nil {
			g.log(err)
			g.Stats.LocalLoadErrs.Add(1)
			return nil, err
		}

		g.Stats.LocalLoads.Add(1)
		destPopulated = true // only one caller of load gets this return value
		g.populateCache(key, value, &g.mainCache)
		return value, nil
	})
	if err == nil {
		value = viewi.(ByteView)
	}

	return
}

func (g *Group) getLocally(key string, dest Sink) (ByteView, error) {
	//全量加载数据，不再查数据库
	if g.fullLoaded {
		return ByteView{}, errors.New("not find")
	}

	err := g.getter.Get(key, dest)
	if err != nil {
		g.log(err)
		return ByteView{}, err
	}
	return dest.view()
}

func (g *Group) lookupCache(key string) (value ByteView, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.get(key)
	if ok {
		if g.expire != nil {
			go g.expire.KeepAlive(key)
		}
		return
	}
	return
}

func (g *Group) populateCache(key string, value ByteView, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}

	cache.add(key, value)
	if g.expire != nil {
		go g.expire.KeepAlive(key)
	}

	if g.httpPeer != nil {
		peer := g.httpPeer.peer.GetPeerNameByKey(key)
		if peer != g.httpPeer.self {
			g.httpPeer.pmap.pMapAdd(peer, key)
		}
	}

	for {
		mainBytes := g.mainCache.bytes()
		if mainBytes <= g.cacheBytes {
			//fmt.Println("------------------------------------------------main cache finall:", g.mainCache.items())
			return
		}

		victim := &g.mainCache
		k := victim.removeOldest()
		if k != nil && g.expire != nil {
			go g.expire.Delete(k)
		}

		if k != nil && g.httpPeer != nil {
			peer := g.httpPeer.peer.GetPeerNameByKey(k.(string))
			if peer != g.httpPeer.self {
				g.httpPeer.pmap.pMapDelete(peer, k.(string))
			} else {
				//fmt.Println("-----removeOldest local peer: ", " key", k)
			}
		}
	}
}

func (g *Group) CacheStats() CacheStats {
	return g.mainCache.stats()
}

func (g *Group) stats() *AllStats {
	o := &AllStats{
		Stats:      g.Stats,
		CacheStats: g.mainCache.stats(),
	}
	if g.httpPeer != nil {
		o.Peer = g.httpPeer.self
		o.PmapCount = g.httpPeer.pmap.pMapCount()
	}
	return o
}

func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

//----------------------------------------Peer -----------------------------------------

type peerHttp struct {
	self     string
	peer     PeerPicker
	errEvent chan string
	pmap     pMap
}

func InitPeers() {
	for _, v := range groups {
		if v.httpPeer == nil && !v.disableHttpPeer {
			peer := getPeer(v.name)
			v.httpPeer = &peerHttp{
				self:     peer.PeerName(),
				peer:     peer,
				errEvent: make(chan string, 20000),
			}
			if v.httpPeer.pmap.gmap == nil {
				v.httpPeer.pmap.gmap = &Gmap{
					pmaps: make(map[string]*Pmap),
				}
			}

			go v.httpPeer.peerHeartCheckProcess(v)
		}
	}
}

func (p *peerHttp) peerHeartCheckProcess(g *Group) {
	var lock sync.RWMutex
	peerMap := make(map[string]bool)

	for {
		var ok bool
		peerName := <-p.errEvent
		peer := p.peer.GetPeerByName(peerName)

		lock.Lock()
		_, ok = peerMap[peerName]
		if !ok {
			peerMap[peerName] = true
		}
		lock.Unlock()

		if !ok {
			go func() {
				for {
					ok := p.peerHeart(peer, g.name)
					if ok {
						p.pmap.pMapClearPositive(peerName, func(key string) {
							g.mainCache.removeKey(key)
							if g.expire != nil {
								go g.expire.Delete(key)
							}
						})
						//缓存可能有出入，清除该缓存
						p.peerClear(peer, g.name)
						lock.Lock()
						delete(peerMap, peerName)
						lock.Unlock()
						break
					}
					time.Sleep(5 * time.Second)
				}
			}()
		}
	}
}

func (p *peerHttp) peerGet(peer ProtoGetter, name, key string) (bool, ByteView, error) {
	isRemoteErr, v, netErr := peer.HttpReqHelper(&pb.GroupPeerRequest{
		Op:    groupPeersGetOperation,
		Group: name,
		Key:   key,
	})

	if netErr != nil {
		p.errEvent <- peer.PeerName()
		return false, ByteView{}, netErr
	}

	if isRemoteErr {
		return true, ByteView{}, errors.New(string(v))
	}

	return false, ByteView{b: v}, nil
}

func (p *peerHttp) peerSet(peer ProtoGetter, name, key string, value *string) error {
	isRemoteErr, v, netErr := peer.HttpReqHelper(&pb.GroupPeerRequest{
		Op:    groupPeersSetOperation,
		Group: name,
		Key:   key,
		Value: *value,
	})

	if netErr != nil {
		p.errEvent <- peer.PeerName()
		return netErr
	}

	if isRemoteErr {
		return errors.New(string(v))
	}

	return nil
}

func (p *peerHttp) peerRemove(peer ProtoGetter, name, key string) error {
	isRemoteErr, v, netErr := peer.HttpReqHelper(&pb.GroupPeerRequest{
		Op:    groupPeersRemoveOperation,
		Group: name,
		Key:   key,
	})

	if netErr != nil {
		p.errEvent <- peer.PeerName()
		return netErr
	}

	if isRemoteErr {
		return errors.New(string(v))
	}

	return nil
}

func (p *peerHttp) peerClear(peer ProtoGetter, name string) error {
	isRemoteErr, v, netErr := peer.HttpReqHelper(&pb.GroupPeerRequest{
		Op:    groupPeersClearOperation,
		Group: name,
	})

	if netErr != nil {
		if p != nil {
			p.errEvent <- peer.PeerName()
		}
		return netErr
	}

	if isRemoteErr {
		return errors.New(string(v))
	}

	return nil
}

func (p *peerHttp) peerHeart(peer ProtoGetter, name string) bool {
	isRemoteErr, _, netErr := peer.HttpReqHelper(&pb.GroupPeerRequest{
		Op:    groupPeersHeartOperation,
		Group: name,
	})

	if netErr != nil || isRemoteErr {
		return false
	}

	return true
}

func (p *peerHttp) peerStats(peer ProtoGetter, name string) (*AllStats, error) {
	isRemoteErr, v, netErr := peer.HttpReqHelper(&pb.GroupPeerRequest{
		Op:    groupPeersGetStatsOperation,
		Group: name,
	})

	if netErr != nil {
		if p != nil {
			p.errEvent <- peer.PeerName()
		}
		return &AllStats{}, netErr
	}

	if isRemoteErr {
		return &AllStats{}, errors.New(string(v))
	}

	as := new(AllStats)
	err := json.Unmarshal(v, as)
	if err != nil {
		return &AllStats{}, err
	}
	return as, nil

}

func (p *peerHttp) peersStats(g *Group) *MachinesStats {
	var failNumber int
	allstats := make([]AllStats, 0)
	failPeer := make([]string, 0)

	allstats = append(allstats, *(g.stats()))

	if g.httpPeer != nil {
		peers := g.httpPeer.peer.GetOtherPeers()

		for _, v := range peers {
			//TODO 并行
			t, err := p.peerStats(v, g.name)
			if err == nil {
				allstats = append(allstats, *t)
			} else {
				g.log(err)
				failNumber++
				failPeer = append(failPeer, v.PeerName())
			}
		}
	}

	return &MachinesStats{
		Group:     g.name,
		AllStats:  allstats,
		FailPeers: failPeer,
	}
}

//----------------------------------------gmap-----------------------------------------

type pMap struct {
	mu   sync.RWMutex
	gmap *Gmap
}

func (p *pMap) pMapAdd(peer, key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.gmap == nil {
		p.gmap = &Gmap{
			pmaps: make(map[string]*Pmap),
		}
	}

	p.gmap.Add(peer, key)
}

func (p *pMap) pMapCount() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.gmap.Count()
}

func (p *pMap) pMapDelete(peer, key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.gmap.Delete(peer, key)
}

func (p *pMap) pMapclearNegative() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.gmap.ClearNegative()
}

func (p *pMap) pMapClearPositive(peer string, f func(key string)) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.gmap.ClearPositive(peer, f)
}

//---------------------------------Getter & flightGroup----------------------------------

type Getter interface {
	Get(key string, dest Sink) error
}

type GetterFunc func(key string, dest Sink) error

func (f GetterFunc) Get(key string, dest Sink) error {
	return f(key, dest)
}

type flightGroup interface {
	Do(key string, fn func() (interface{}, error)) (interface{}, error)
}

//----------------------------------------lru--------------------------------------------

type AllStats struct {
	Stats      Stats
	CacheStats CacheStats
	Peer       string
	PmapCount  map[string]int
}

type cache struct {
	mu         sync.RWMutex
	nbytes     int64 // of all keys and values
	lru        *lru.Cache
	nhit, nget int64
	nevict     int64 // number of evictions
}

type Stats struct {
	Gets                   AtomicInt // any Get request, including from peer
	CacheHits              AtomicInt // either cache was good
	PeerLoads              AtomicInt // either remote load or remote cache hit (not an error)
	PeerErrors             AtomicInt
	Loads                  AtomicInt // (gets - cacheHits)
	LoadsDeduped           AtomicInt // after singleflight
	LocalLoads             AtomicInt // total good local loads
	LocalLoadErrs          AtomicInt // total bad local loads
	ServerGetStatsRequests AtomicInt // xxx that came over the network from peer
	ServerSetRequests      AtomicInt // xxx that came over the network from peer
	ServerGetRequests      AtomicInt // gets that came over the network from peer
	ServerRemoveRequests   AtomicInt // xxx that came over the network from peer
	ServerClearRequests    AtomicInt // xxx that came over the network from peer
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = &lru.Cache{
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(ByteView)
				c.nbytes -= int64(len(key.(string))) + int64(val.Len())
				c.nevict++
			},
		}
	}
	c.lru.Add(key, value)
	c.nbytes += int64(len(key)) + int64(value.Len())
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	c.nhit++
	return vi.(ByteView), true
}

func (c *cache) removeKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.Remove(key)
	}
}

func (c *cache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.Clear()
	}
}

func (c *cache) forEach(f func(key interface{})) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.Foreach(f)
	}
}

func (c *cache) removeOldest() interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		return c.lru.RemoveOldest()
	}
	return nil
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}
