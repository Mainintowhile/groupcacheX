package expire

import (
	"sort"
	"sync"
	"time"
)

var (
	expires = make(map[string]*Expire)
	mutex   sync.RWMutex
)

func NewExpire(name string, defaultLifeSpan time.Duration, logger func(v ...interface{}), onExpire func(key interface{})) *Expire {
	mutex.Lock()
	defer mutex.Unlock()
	t, ok := expires[name]

	if !ok {
		t = &Expire{
			name:            name,
			defaultLifeSpan: defaultLifeSpan,
			items:           make(map[interface{}]*ExpireItem),
			onExpire:        onExpire,
		}
		if logger != nil {
			t.log = logger
		} else {
			t.log = func(v ...interface{}) {}
		}

		expires[name] = t
	}

	return t
}

type Expire struct {
	sync.RWMutex
	name                  string
	defaultLifeSpan       time.Duration
	items                 map[interface{}]*ExpireItem
	cleanTaskTimer        *time.Timer   //清除过期数据定时器
	nextCleanTaskInterval time.Duration //下一次清除过期数据时间间隔
	log                   func(v ...interface{})
	onExpire              func(key interface{})
}

func (e *Expire) expirationCheck() {
	e.Lock()
	if e.cleanTaskTimer != nil {
		e.cleanTaskTimer.Stop()
	}
	if e.nextCleanTaskInterval > 0 {
		//e.log("Expiration check triggered after", e.nextCleanTaskInterval, "for table", e.name)
	} else {
		//e.log("Expiration check installed for table", e.name)
	}

	now := time.Now()
	smallestDuration := 0 * time.Second
	for key, item := range e.items {
		item.RLock()
		lifeSpan := item.lifeSpan
		updatedAt := item.updatedAt
		item.RUnlock()

		if lifeSpan == 0 {
			continue
		}
		if now.Sub(updatedAt) >= lifeSpan {
			if e.onExpire != nil {
				e.onExpire(key)
			}
			//e.log("ExpirationCheck Deleting item with key", key, "created on", item.createdAt, "and hit", item.gets, "times from table", e.name)
			delete(e.items, key)
		} else {
			if smallestDuration == 0 || lifeSpan-now.Sub(updatedAt) < smallestDuration {
				smallestDuration = lifeSpan - now.Sub(updatedAt)
			}
		}
	}

	e.nextCleanTaskInterval = smallestDuration
	if smallestDuration > 0 {
		e.cleanTaskTimer = time.AfterFunc(smallestDuration, func() {
			go e.expirationCheck()
		})
	}
	e.Unlock()
}

//--------------------------------------增删改查------------------------------------------

func (e *Expire) KeepAlive(key interface{}, args ...interface{}) {
	e.RLock()
	r, ok := e.items[key]
	e.RUnlock()

	if ok {
		r.KeepAlive()
		return
	}

	e.add(key)
}

func (e *Expire) add(key interface{}, lifeSpan ...time.Duration) {
	var l time.Duration
	if len(lifeSpan) == 0 {
		l = e.defaultLifeSpan
	} else {
		l = lifeSpan[0]
	}
	item := NewCacheItem(key, l)

	e.Lock()

	//e.log("Adding item with key", item.key, "and lifespan of", item.lifeSpan, "to table", e.name)
	e.items[item.key] = item

	expDur := e.nextCleanTaskInterval
	e.Unlock()

	if item.lifeSpan > 0 && (expDur == 0 || item.lifeSpan < expDur) {
		e.expirationCheck()
	}
}

func (e *Expire) Delete(key interface{}) {
	e.Lock()
	defer e.Unlock()

	r, ok := e.items[key]
	if !ok {
		return
	}

	e.log("Deleting item with key", key, "created on", r.createdAt, "and hit", r.gets, "times from table", e.name)
	delete(e.items, key)
}

func (e *Expire) Clear() {
	e.Lock()
	defer e.Unlock()

	e.log("Clean table", e.name)

	e.items = make(map[interface{}]*ExpireItem)
	e.nextCleanTaskInterval = 0
	if e.cleanTaskTimer != nil {
		e.cleanTaskTimer.Stop()
	}
}

func (e *Expire) Count() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.items)
}

func (e *Expire) Foreach(max int, f func(key interface{})) {
	e.RLock()
	defer e.RUnlock()

	var count int
	for k, _ := range e.items {
		if count >= max {
			return
		}
		f(k)
		count++
	}
}

func (e *Expire) Exists(key interface{}) bool {
	e.RLock()
	defer e.RUnlock()
	_, ok := e.items[key]

	return ok
}

//--------------------------------------log------------------------------------------

func (e *Expire) SetLogger(logger func(v ...interface{})) {
	e.Lock()
	defer e.Unlock()
	e.log = logger
}

func (e *Expire) SetDefaultExpireDuration(duration time.Duration) {
	e.Lock()
	defer e.Unlock()
	e.defaultLifeSpan = duration
}

//---------------------------------Statistics----------------------------------------

type CacheItemPair struct {
	Key         interface{}
	AccessCount int64
}

type CacheItemPairList []CacheItemPair

func (p CacheItemPairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (p CacheItemPairList) Len() int { return len(p) }

func (p CacheItemPairList) Less(i, j int) bool { return p[i].AccessCount > p[j].AccessCount }

func (e *Expire) MostAccessed(count int64) []*ExpireItem {
	e.RLock()
	defer e.RUnlock()

	p := make(CacheItemPairList, len(e.items))
	i := 0
	for k, v := range e.items {
		p[i] = CacheItemPair{k, v.gets}
		i++
	}
	sort.Sort(p)

	var r []*ExpireItem
	c := int64(0)
	for _, v := range p {
		if c >= count {
			break
		}

		item, ok := e.items[v.Key]
		if ok {
			r = append(r, item)
		}
		c++
	}

	return r
}
