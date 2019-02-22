package expire

import (
	"sync"
	"time"
)

type ExpireItem struct {
	sync.RWMutex
	key       interface{}
	lifeSpan  time.Duration
	createdAt time.Time
	updatedAt time.Time
	gets      int64
}

func NewCacheItem(key interface{}, lifeSpan time.Duration) *ExpireItem {
	t := time.Now()
	return &ExpireItem{
		key:       key,
		lifeSpan:  lifeSpan,
		createdAt: t,
		updatedAt: t,
		gets:      0,
	}
}

func (item *ExpireItem) KeepAlive() {
	item.Lock()
	defer item.Unlock()
	item.updatedAt = time.Now()
	item.gets++
}

func (item *ExpireItem) LifeSpan() time.Duration {
	return item.lifeSpan
}

func (item *ExpireItem) UpdateTime() time.Time {
	item.RLock()
	defer item.RUnlock()
	return item.updatedAt
}

func (item *ExpireItem) CreateTime() time.Time {
	return item.createdAt
}

func (item *ExpireItem) Gets() int64 {
	item.RLock()
	defer item.RUnlock()
	return item.gets
}

func (item *ExpireItem) Key() interface{} {
	return item.key
}
