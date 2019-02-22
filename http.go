package groupcache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"groupcacheX/consistenthash"
	pb "groupcacheX/groupcachepb"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peer.
type HTTPPool struct {
	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func() http.RoundTripper

	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	// opts specifies the options.
	opts HTTPPoolOptions

	mu          sync.Mutex // guards peer and httpGetters
	peer        *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"

	A string
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash
}

//----------------------------------------init--------------------------------------------

func NewHTTPPool(tables []string) (string, *HTTPPool) {
	var ip, addr string

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic("Oops:" + err.Error())
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = ipnet.IP.String()
				break
			}
		}
	}

	if len(ip) == 0 {
		panic("Oops: not found ip")
	}

	for _, v := range tables {
		if strings.Contains(v, ip) {
			addr = v
			break
		}
	}

	if len(addr) == 0 {
		panic("Oops: not found addr in addr tables")
	}

	p := NewHTTPPoolOpts(fmt.Sprintf("http://%s", addr), nil)
	http.Handle(p.opts.BasePath, p)

	p.Set(AddrToURL(tables)...)

	InitPeers()

	return addr, p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peer with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}

	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peer = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

// Set updates the pool's list of peer.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPool) Set(peer ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peer = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peer.Add(peer...)
	p.httpGetters = make(map[string]*httpGetter, len(peer))
	for _, peer := range peer {
		p.httpGetters[peer] = &httpGetter{
			transport: p.Transport,
			baseURL:   peer + p.opts.BasePath,
			self:      peer,
		}
	}
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peer.IsEmpty() {
		return nil, "", false
	}

	if peer := p.peer.Get(key); peer != p.self {
		return p.httpGetters[peer], peer, true
	}
	return nil, "", false
}

func (p *HTTPPool) GetPeerByName(peer string) ProtoGetter {
	return p.httpGetters[peer]
}

func (p *HTTPPool) GetPeerNameByKey(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peer.IsEmpty() {
		return ""
	}

	return p.peer.Get(key)
}

func (p *HTTPPool) GetOtherPeers() (peers []ProtoGetter) {
	peers = make([]ProtoGetter, 0)
	for k, v := range p.httpGetters {
		if k != p.self {
			peers = append(peers, v)
		}
	}
	return
}

func (p *HTTPPool) PeerName() string {
	return p.self
}

//---------------------------------receive & send-------------------------------------------

//TODO test nil value
func httpRetHelper(w http.ResponseWriter, value []byte, isErr bool) {
	o := &pb.GroupPeerResponse{}
	o.Ok = true
	o.Err = isErr
	o.Value = value

	ret, err := proto.Marshal(o)
	if err != nil {
		o.Err = true
		o.Value = []byte(err.Error())
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(ret)
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		httpRetHelper(w, []byte("HTTPPool serving unexpected path: "+r.URL.Path), true)
		return
	}

	parts := strings.Split(r.URL.Path[len(p.opts.BasePath):], "/")
	if len(parts) < 2 {
		httpRetHelper(w, []byte("bad request"), true)
		return
	}

	op := parts[0]
	groupName := parts[1]
	var key string
	if len(parts) == 3 {
		key = parts[2]
	}

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		httpRetHelper(w, []byte("no such group: "+groupName), true)
		return
	}

	if op == groupPeersGetOperation {
		group.Stats.ServerGetRequests.Add(1)
		var value []byte
		err := group.Get(key, AllocatingByteSliceSink(&value), p.self)
		if err != nil {
			httpRetHelper(w, []byte(err.Error()), true)
			return
		}
		httpRetHelper(w, value, false)
		return
	} else if op == groupPeersSetOperation {
		group.Stats.ServerSetRequests.Add(1)
		_v := make([]byte, 0)
		n, err := r.Body.Read(_v)
		if n == 0 {
			httpRetHelper(w, []byte("没有value数据"), true)
			return
		}
		if err != nil {
			httpRetHelper(w, []byte(err.Error()), true)
			return
		}
		v := string(_v)
		err = group.Set(key, &v)
		if err != nil {
			httpRetHelper(w, []byte(err.Error()), true)
			return
		}
		httpRetHelper(w, nil, false)
		return
	} else if op == groupPeersClearOperation {
		group.Stats.ServerClearRequests.Add(1)
		group.Clear()
		httpRetHelper(w, nil, false)
		return
	} else if op == groupPeersRemoveOperation {
		group.Stats.ServerRemoveRequests.Add(1)
		group.Remove(key)
		httpRetHelper(w, nil, false)
		return
	} else if op == groupPeersGetStatsOperation {
		group.Stats.ServerGetStatsRequests.Add(1)
		b, err := json.Marshal(group.stats())
		if err != nil {
			httpRetHelper(w, []byte(err.Error()), true)
			return
		}
		httpRetHelper(w, b, false)
		return
	} else if op == groupPeersHeartOperation {
		httpRetHelper(w, nil, false)
		return
	} else {
		httpRetHelper(w, []byte("no such oparation surport: "+op), true)
		return
	}
}

type httpGetter struct {
	transport func() http.RoundTripper
	baseURL   string
	self      string
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (p *httpGetter) HttpReqHelper(req *pb.GroupPeerRequest) (isRemoteErr bool, value []byte, netErr error) {
	res := &pb.GroupPeerResponse{}
	netErr = p.Op(req, res)
	if netErr != nil || !res.Ok {
		return false, nil, netErr
	}

	if res.Err {
		return true, res.Value, nil
	}

	return false, res.Value, nil
}

func (h *httpGetter) Op(in *pb.GroupPeerRequest, out *pb.GroupPeerResponse) error {
	op := in.GetOp()
	group := in.GetGroup()
	key := in.GetKey()

	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(op),
		url.QueryEscape(group),
	)
	if op != groupPeersClearOperation && op != groupPeersGetStatsOperation && op != groupPeersHeartOperation {
		if len(key) == 0 {
			return errors.New("operation no key")
		}
		u = fmt.Sprintf("%v/%v", u, url.QueryEscape(key))
	}

	var req *http.Request
	var err error
	if op == groupPeersSetOperation {
		body := &bytes.Buffer{}
		if len(in.Value) == 0 {
			return errors.New("set operation no value")
		}
		body.WriteString(in.Value)
		req, err = http.NewRequest("GET", u, body)
		if err != nil {
			return err
		}
	} else {
		req, err = http.NewRequest("GET", u, nil)
		if err != nil {
			return err
		}
	}

	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport()
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

func (h *httpGetter) PeerName() string {
	return h.self
}

func AddrToURL(addr []string) []string {
	uri := make([]string, len(addr))
	for i := range addr {
		uri[i] = "http://" + addr[i]
	}
	return uri
}
