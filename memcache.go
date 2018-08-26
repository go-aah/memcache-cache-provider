// Copyright (c) Jeevanandam M. (https://github.com/jeevatkm)
// Source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.

package memcache // import "aahframe.work/cache/provider/memcache"

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
	"time"

	"aahframe.work/aah/cache"
	"aahframe.work/aah/config"
	"aahframe.work/aah/log"
	"github.com/bradfitz/gomemcache/memcache"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Provider and its exported methods
//______________________________________________________________________________

// Provider struct represents the Redis cache provider.
type Provider struct {
	name   string
	logger log.Loggerer
	cfg    *cache.Config
	appCfg *config.Config
	client *memcache.Client
}

var _ cache.Provider = (*Provider)(nil)

// Init method initializes the Redis cache provider.
func (p *Provider) Init(providerName string, appCfg *config.Config, logger log.Loggerer) error {
	p.name = providerName
	p.appCfg = appCfg
	p.logger = logger.WithField("cache_provider", providerName)

	cfgPrefix := "cache." + p.name + "."
	if strings.ToLower(p.appCfg.StringDefault(cfgPrefix+"provider", "")) != "memcache" {
		return fmt.Errorf("aah/cache: not a vaild provider name, expected 'memcache'")
	}

	addresses, found := p.appCfg.StringList(cfgPrefix + "addresses")
	if !found {
		addresses = []string{"0.0.0.0:11211"}
	}

	p.client = memcache.New(addresses...)
	p.client.MaxIdleConns = p.appCfg.IntDefault(cfgPrefix+"max_idle_conns", memcache.DefaultMaxIdleConns)
	p.client.Timeout = parseDuration(p.appCfg.StringDefault(cfgPrefix+"timeout", "5s"), "5s")

	gob.Register(entry{})

	// Check server connection
	if _, err := p.client.Get(p.name + "-testkey"); err != nil && err != memcache.ErrCacheMiss {
		return fmt.Errorf("aah/cache/%s: %s", p.name, err)
	}

	p.logger.Infof("aah/cache/provider: %s connected successfully with %s", p.name, strings.Join(addresses, ", "))

	return nil
}

// Create method creates new Redis cache with given options.
func (p *Provider) Create(cfg *cache.Config) (cache.Cache, error) {
	p.cfg = cfg
	m := &memcacheCache{
		keyPrefix: p.cfg.Name + "-",
		p:         p,
	}
	return m, nil
}

// Client method returns underlying memcache client. So that aah user could perform
// cache provider specific features.
func (p *Provider) Client() *memcache.Client {
	return p.client
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// memcacheCache struct implements `cache.Cache` interface.
//______________________________________________________________________________

type memcacheCache struct {
	keyPrefix string
	p         *Provider
}

var _ cache.Cache = (*memcacheCache)(nil)

// Name method returns the cache store name.
func (m *memcacheCache) Name() string {
	return m.p.cfg.Name
}

// Get method returns the cached entry for given key if it exists otherwise nil.
// Method uses `gob.Decoder` to unmarshal cache value from bytes.
func (m *memcacheCache) Get(k string) interface{} {
	k = m.keyPrefix + k
	v, err := m.p.client.Get(k)
	if err != nil {
		// if notacacheMiss(err) != nil {
		m.p.logger.Errorf("aah/cache/%s: key(%s) %v", m.Name(), k[len(m.keyPrefix):], err)
		// }
		return nil
	}

	var e entry
	err = gob.NewDecoder(bytes.NewBuffer(v.Value)).Decode(&e)
	if err != nil {
		m.p.logger.Errorf("aah/cache/%s: %v", m.Name(), err)
		return nil
	}
	if m.p.cfg.EvictionMode == cache.EvictionModeSlide {
		if err = m.p.client.Touch(k, e.D); err != nil {
			m.p.logger.Errorf("aah/cache/%s: key(%s) %v", m.Name(), k[len(m.keyPrefix):], err)
		}
	}

	return e.V
}

// GetOrPut method returns the cached entry for the given key if it exists otherwise
// it puts the new entry into cache store and returns the value.
func (m *memcacheCache) GetOrPut(k string, v interface{}, d time.Duration) (interface{}, error) {
	ev := m.Get(k)
	if ev == nil {
		if err := m.Put(k, v, d); err != nil {
			return nil, err
		}
		return v, nil
	}
	return ev, nil
}

// Put method adds the cache entry with specified expiration. Returns error
// if cache entry exists. Method uses `gob.Encoder` to marshal cache value into bytes.
func (m *memcacheCache) Put(k string, v interface{}, d time.Duration) error {
	e := entry{D: int32(d.Seconds()), V: v}
	buf := acquireBuffer()
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(e); err != nil {
		return fmt.Errorf("aah/cache/%s: %v", m.Name(), err)
	}

	err := m.p.client.Set(&memcache.Item{
		Key:        m.keyPrefix + k,
		Value:      buf.Bytes(),
		Expiration: e.D,
	})
	releaseBuffer(buf)
	return err
}

// Delete method deletes the cache entry from cache store.
func (m *memcacheCache) Delete(k string) error {
	if err := m.p.client.Delete(m.keyPrefix + k); notacacheMiss(err) != nil {
		return fmt.Errorf("aah/cache/%s: key(%s) %v", m.Name(), k, err)
	}
	return nil
}

// Exists method checks given key exists in cache store and its not expried.
func (m *memcacheCache) Exists(k string) bool {
	if v := m.Get(k); v != nil {
		return true
	}
	return false
}

// Flush methods flushes(deletes) all the cache entries from cache.
func (m *memcacheCache) Flush() error {
	if err := m.p.client.FlushAll(); err != nil {
		return fmt.Errorf("aah/cache/%s: %v", m.Name(), err)
	}
	return nil
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Helper methods
//______________________________________________________________________________

type entry struct {
	D int32
	V interface{}
}

func parseDuration(v, f string) time.Duration {
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	d, _ := time.ParseDuration(f)
	return d
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func acquireBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func releaseBuffer(b *bytes.Buffer) {
	if b != nil {
		b.Reset()
		bufPool.Put(b)
	}
}

func notacacheMiss(err error) error {
	if err == memcache.ErrCacheMiss {
		return nil
	}
	return nil
}
