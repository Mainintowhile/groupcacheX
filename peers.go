/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// peer.go defines how processes find and communicate with their peer.

package groupcache

import (
	pb "groupcacheX/groupcachepb"
)

// ProtoGetter is the interface that must be implemented by a peer.
type ProtoGetter interface {
	Op(in *pb.GroupPeerRequest, out *pb.GroupPeerResponse) error
	PeerName() string
	HttpReqHelper(req *pb.GroupPeerRequest) (isRemoteErr bool, value []byte, netErr error)
}

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
type PeerPicker interface {
	// PickPeer returns the peer that owns the specific key
	// and true to indicate that a remote peer was nominated.
	// It returns nil, false if the key owner is the current peer.
	PickPeer(key string) (peer ProtoGetter, who string, ok bool)

	//get the rest peer
	GetOtherPeers() (peer []ProtoGetter)

	PeerName() string

	GetPeerByName(peer string) ProtoGetter

	GetPeerNameByKey(key string) string
}

// NoPeer is an implementation of PeerPicker that never finds a peer.
type NoPeer struct {
	A string
}

func (NoPeer) PickPeer(key string) (peer ProtoGetter, who string, ok bool) { return }

func (NoPeer) GetOtherPeers() (peer []ProtoGetter) { return }

func (NoPeer) PeerName() (name string) { return }

func (NoPeer) GetPeerByName(peerName string) (peer ProtoGetter) { return }

func (NoPeer) GetPeerNameByKey(key string) (peer string) { return }

var (
	portPicker func(groupName string) PeerPicker
)

// RegisterPeerPicker registers the peer initialization function.
// It is called once, when the first group is created.
// Either RegisterPeerPicker or RegisterPerGroupPeerPicker should be
// called exactly once, but not both.
func RegisterPeerPicker(fn func() PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = func(_ string) PeerPicker { return fn() }
}

// RegisterPerGroupPeerPicker registers the peer initialization function,
// which takes the groupName, to be used in choosing a PeerPicker.
// It is called once, when the first group is created.
// Either RegisterPeerPicker or RegisterPerGroupPeerPicker should be
// called exactly once, but not both.
func RegisterPerGroupPeerPicker(fn func(groupName string) PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = fn
}

func getPeer(groupName string) PeerPicker {
	if portPicker == nil {
		return NoPeer{}
	}
	pk := portPicker(groupName)
	if pk == nil {
		pk = NoPeer{}
	}
	return pk
}
