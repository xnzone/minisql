package lru

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key     interface{}
	value   interface{}
	addedAt time.Time
	ttl     time.Duration
}

type TimeoutCallbackFunc func(key interface{}, value interface{})

type Options struct {
	TimeoutCallback TimeoutCallbackFunc
}

type LRU struct {
	mu         sync.Mutex
	list       *list.List
	table      map[interface{}]*list.Element
	expiration time.Duration
	maxLen     int
	options    *Options
}

type OptionFunc func(opt *Options)

func NewLRU(maxLen int, expiration time.Duration, opt ...OptionFunc) *LRU {
	return &LRU{
		list:       list.New(),
		table:      make(map[interface{}]*list.Element, maxLen),
		expiration: expiration,
		maxLen:     maxLen,
		options:    newOption(opt...),
	}
}

func TimeoutFunc(fn TimeoutCallbackFunc) OptionFunc {
	return func(opt *Options) {
		opt.TimeoutCallback = fn
	}
}

func newOption(opt ...OptionFunc) *Options {
	opts := &Options{
		TimeoutCallback: nil,
	}
	for _, o := range opt {
		o(opts)
	}
	return opts
}

func (c *LRU) Get(key interface{}) (interface{}, time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el := c.table[key]
	if el == nil {
		return nil, time.Duration(0), false
	}

	en := el.Value.(*entry)
	if time.Since(en.addedAt) > en.ttl {
		c.delete(el)
		return nil, time.Duration(0), false
	}
	c.list.MoveToFront(el)
	val := en.value
	ttl := en.ttl
	return val, ttl, true
}

func (c *LRU) All() map[interface{}]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make(map[interface{}]interface{}, len(c.table))

	for k, v := range c.table {
		ret[k] = v.Value.(*entry).value
	}
	return ret
}

func (c *LRU) Set(key interface{}, val interface{}, ttl time.Duration) {
	if ttl == 0 {
		ttl = c.expiration
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el := c.table[key]; el != nil {
		en := el.Value.(*entry)
		en.value = val
		en.ttl = ttl
		c.promote(el, en)
	} else {
		c.add(key, val, ttl)
	}
}

func (c *LRU) Delete(key interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	el := c.table[key]
	if el == nil {
		return nil
	}
	c.delete(el)
	return nil
}

func (c *LRU) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = list.New()
	c.table = make(map[interface{}]*list.Element, c.maxLen)
	return nil
}

func (c *LRU) GetTTL() time.Duration {
	return c.expiration
}

func (c *LRU) add(key interface{}, val interface{}, ttl time.Duration) {
	en := &entry{
		key:     key,
		value:   val,
		addedAt: time.Now(),
		ttl:     ttl,
	}
	el := c.list.PushFront(en)
	c.table[key] = el
	c.check()
}

func (c *LRU) promote(el *list.Element, en *entry) {
	en.addedAt = time.Now()
	c.list.MoveToFront(el)
}

func (c *LRU) delete(el *list.Element) {
	if c.options.TimeoutCallback != nil {
		c.options.TimeoutCallback(el.Value.(*entry).key, el.Value.(*entry).value)
	}
	c.list.Remove(el)
	delete(c.table, el.Value.(*entry).key)
}

func (c *LRU) check() {
	for c.list.Len() > c.maxLen {
		el := c.list.Back()
		c.delete(el)
	}
}

func (c *LRU) RemoveExpired() {
	now := time.Now()
	var total int
	var count int

	c.mu.Lock()
	defer c.mu.Unlock()

	total = c.maxLen
	count = c.list.Len()
	for i := 0; i < total && i < count; i++ {
		el := c.list.Back()
		en := el.Value.(*entry)
		if now.Sub(en.addedAt) <= en.ttl {
			return
		}
		c.delete(el)
	}
}
